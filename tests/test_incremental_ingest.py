import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from scripts import incremental_update
from src.db.schema import (
    LlmOutput,
    Paper,
    PaperScore,
    RawRecord,
    SourceRun,
    get_session,
    init_db,
)
from src.pipeline.raw_ingest import insert_raw_record


class IncrementalIngestTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "papers.db")
        init_db(self.db_path)
        session = get_session(self.db_path)
        old_run = SourceRun(source="arxiv", params={"category": "cs.AI"})
        session.add(old_run)
        session.flush()
        insert_raw_record(
            session,
            run_id=old_run.id,
            source="arxiv",
            source_record_id="duplicate",
            payload={"arxiv_id": "duplicate"},
        )
        session.commit()
        session.close()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_arxiv_increment_keeps_new_records_around_a_duplicate(self):
        records = [
            {"arxiv_id": "new-before-duplicate"},
            {"arxiv_id": "duplicate"},
            {"arxiv_id": "new-after-duplicate"},
        ]

        with (
            patch.object(incremental_update, "DB_PATH", self.db_path),
            patch.object(incremental_update, "ARXIV_CATEGORIES", ["cs.AI"]),
            patch.object(incremental_update, "LOOKBACK_DAYS", 30),
            patch.object(incremental_update, "fetch_category", return_value=records),
        ):
            incremental_update.step_fetch_arxiv_incremental()

        session = get_session(self.db_path)
        try:
            latest_run = session.execute(
                select(SourceRun).order_by(SourceRun.id.desc())
            ).scalars().first()
            persisted_ids = session.execute(
                select(RawRecord.source_record_id)
                .where(RawRecord.run_id == latest_run.id)
                .order_by(RawRecord.source_record_id)
            ).scalars().all()

            self.assertEqual(latest_run.status, "success")
            self.assertEqual(latest_run.records_fetched, 2)
            self.assertEqual(
                persisted_ids,
                ["new-after-duplicate", "new-before-duplicate"],
            )
        finally:
            session.close()

    def test_arxiv_increment_uses_openalex_fallback_after_api_failure(self):
        fallback_records = [
            {
                "arxiv_id": "2608.04205",
                "title": "MatrAIx",
                "abstract": "simulated user evaluation",
            }
        ]

        with (
            patch.object(incremental_update, "DB_PATH", self.db_path),
            patch.object(incremental_update, "ARXIV_CATEGORIES", ["cs.AI"]),
            patch.object(incremental_update, "LOOKBACK_DAYS", 30),
            patch.object(
                incremental_update,
                "fetch_category",
                side_effect=RuntimeError("arXiv API unavailable"),
            ),
            patch.object(
                incremental_update,
                "fetch_arxiv_via_openalex",
                return_value=fallback_records,
            ) as fallback,
        ):
            incremental_update.step_fetch_arxiv_incremental()

        session = get_session(self.db_path)
        try:
            fallback_run = session.execute(
                select(SourceRun)
                .where(SourceRun.params.contains("openalex_fallback"))
                .order_by(SourceRun.id.desc())
            ).scalars().first()
            persisted_ids = session.execute(
                select(RawRecord.source_record_id).where(
                    RawRecord.run_id == fallback_run.id
                )
            ).scalars().all()

            fallback.assert_called_once()
            self.assertEqual(fallback_run.status, "success")
            self.assertEqual(fallback_run.records_fetched, 1)
            self.assertEqual(persisted_ids, ["2608.04205"])
        finally:
            session.close()

    def seed_paper(self, title: str, *, pub_date: str | None = None) -> int:
        session = get_session(self.db_path)
        try:
            p = Paper(title=title, pub_date=pub_date)
            session.add(p)
            session.commit()
            return p.id
        finally:
            session.close()

    def seed_score(self, paper_id: int, ai: float, dom: float):
        session = get_session(self.db_path)
        try:
            session.add(PaperScore(paper_id=paper_id, ai_relevance=ai,
                                   domain_relevance=dom))
            session.commit()
        finally:
            session.close()

    def seed_tldr(self, paper_id: int, tldr: str | None):
        session = get_session(self.db_path)
        try:
            session.add(LlmOutput(paper_id=paper_id, tldr_zh=tldr))
            session.commit()
        finally:
            session.close()

    def test_enrichment_queue_picks_old_scored_papers_missing_tldr(self):
        """已打分但缺 TL;DR 的老论文必须进补全队列，不受新论文抢占。"""
        old_missing = self.seed_paper("old missing tldr", pub_date="2022-11-01")
        self.seed_score(old_missing, ai=4.0, dom=4.0)
        fresh_unscored = self.seed_paper("fresh unscored", pub_date="2026-08-15")

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            total, ids = incremental_update.select_enrichment_candidate_ids(limit=50)

        self.assertEqual(total, 1)
        self.assertEqual(ids, [old_missing])
        self.assertNotIn(fresh_unscored, ids)

    def test_enrichment_queue_prioritizes_missing_tldr_over_missing_title(self):
        """缺 TL;DR 的排前面；已有 TL;DR 只缺中文标题的排后面。"""
        missing_tldr = self.seed_paper("needs tldr", pub_date="2026-01-01")
        self.seed_score(missing_tldr, ai=4.0, dom=4.0)
        missing_title = self.seed_paper("needs title zh", pub_date="2026-02-01")
        self.seed_score(missing_title, ai=4.0, dom=4.0)
        self.seed_tldr(missing_title, "已有摘要")

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            total, ids = incremental_update.select_enrichment_candidate_ids(limit=50)

        self.assertEqual(total, 2)
        self.assertEqual(ids[0], missing_tldr)
        self.assertEqual(ids[1], missing_title)

    def test_enrichment_queue_retries_empty_tldr_row(self):
        """llm_outputs 里 tldr 为空的论文也要重跑，不能永久跳过。"""
        pid = self.seed_paper("empty tldr row", pub_date="2026-03-01")
        self.seed_score(pid, ai=4.0, dom=4.0)
        self.seed_tldr(pid, "")

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            total, ids = incremental_update.select_enrichment_candidate_ids(limit=50)

        self.assertEqual(total, 1)
        self.assertEqual(ids, [pid])

    def test_enrichment_queue_skips_complete_papers(self):
        """TL;DR 和中文标题都齐全的论文不进补全队列。"""
        complete = self.seed_paper("complete", pub_date="2026-04-01")
        self.seed_score(complete, ai=4.0, dom=4.0)
        self.seed_tldr(complete, "有摘要")
        session = get_session(self.db_path)
        try:
            session.get(Paper, complete).title_zh = "完整标题"
            session.commit()
        finally:
            session.close()

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            total, ids = incremental_update.select_enrichment_candidate_ids(limit=50)

        self.assertEqual(total, 0)
        self.assertEqual(ids, [])

    def test_scoring_queue_only_unscored_papers(self):
        """打分队列只含未打分论文，不含已打分缺摘要的。"""
        unscored = self.seed_paper("unscored", pub_date="2026-08-10")
        scored = self.seed_paper("scored", pub_date="2026-08-11")
        self.seed_score(scored, ai=4.0, dom=4.0)

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            total, ids = incremental_update.select_scoring_candidate_ids(limit=50)

        self.assertEqual(total, 1)
        self.assertEqual(ids, [unscored])

    def test_queues_have_independent_limits(self):
        """两个队列的 limit 互不影响：打分队列上限不削减补全队列。"""
        pid = self.seed_paper("needs tldr", pub_date="2022-01-01")
        self.seed_score(pid, ai=4.0, dom=4.0)

        with patch.object(incremental_update, "DB_PATH", self.db_path):
            _, enrich_ids = incremental_update.select_enrichment_candidate_ids(limit=1)
            score_total, score_ids = incremental_update.select_scoring_candidate_ids(limit=1)

        self.assertEqual(enrich_ids, [pid])
        self.assertEqual(score_total, 0)
        self.assertEqual(score_ids, [])


if __name__ == "__main__":
    unittest.main()

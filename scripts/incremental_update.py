"""
每日增量更新脚本（GitHub Actions 调用）。

流程:
  1. 英文期刊增量: Crossref from-index-date + OpenAlex from-publication-date
  2. arXiv 增量: 近 14 天 + 现有关键词
  3. normalize（幂等）
  4. LLM 双打分（仅未打分的）
  5. LLM TL;DR（仅双≥3 且未生成的）
  6. coverage_audit
  7. export_web_data

时间窗口:
  默认回看 14 天（弥补出版延迟），与上次跑去重通过 raw_records 唯一约束
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

from src.db.schema import get_session, SourceRun, init_db
from src.utils.journals import english_journals
from src.connectors import openalex as oa
from src.connectors import crossref as cr
from src.connectors.arxiv import fetch_arxiv_via_openalex, fetch_category
from src.pipeline.normalize import normalize as normalize_english
from src.pipeline.ingest_arxiv import normalize_arxiv
from src.pipeline.coverage_audit import audit
from src.pipeline.export_web_data import main as export_data
from src.pipeline.llm_score_parallel import run as llm_score_run
from src.pipeline.llm_tldr import run as llm_tldr_run
from src.pipeline.llm_title_zh import run as llm_title_zh_run
from src.pipeline.raw_ingest import (
    assert_run_record_count,
    count_run_records,
    insert_raw_record,
)

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./data/papers.db")

# 增量窗口
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
# HCI 主导：cs.HC/CY/CL/AI/SI/SE/IR + stat.ME/ML + econ.GN
ARXIV_CATEGORIES = ["cs.HC", "cs.CY", "cs.CL", "cs.AI", "cs.SI", "cs.SE", "cs.IR", "stat.ME", "stat.ML", "econ.GN"]


def ensure_db():
    """确保 db 文件存在；不存在则建表。"""
    db_path = Path(DB_PATH).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        print(f"[init] DB 不存在，建表: {db_path}")
        init_db(str(db_path))


def step_fetch_english_incremental():
    """对每本英文期刊/会议，按 Crossref from-index-date + OpenAlex from-publication-date 抓新论文。

    注意：会议（type=conference）通常无 ISSN，跳过 Crossref；只走 OpenAlex source_id。
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    since = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    print(f"\n=== 英文期刊增量 ({since} → {today}) ===")

    session = get_session(DB_PATH)
    try:
        for j in english_journals():
            sources = []
            if j.get("issn"):
                sources.append(("crossref",
                                lambda: cr.fetch_works_by_issn(j["issn"], from_index_date=since)))
            if j.get("openalex_source_id"):
                sources.append(("openalex",
                                lambda: oa.fetch_works_by_source(j["openalex_source_id"], from_date=since)))
            if not sources:
                print(f"  [skip] {j['abbr']} 无 ISSN 也无 openalex_source_id，跳过")
                continue
            for src_name, fetch in sources:
                run = SourceRun(source=src_name, journal_abbr=j["abbr"],
                                params={"mode": "incremental", "since": since})
                session.add(run); session.flush()
                seen = 0
                inserted = 0
                duplicates = 0
                try:
                    for w in fetch():
                        slim = (cr.slim_record(w) if src_name == "crossref" else oa.slim_record(w))
                        sid = slim.get("doi") if src_name == "crossref" else slim.get("id")
                        if not sid:
                            continue
                        seen += 1
                        if insert_raw_record(
                            session,
                            run_id=run.id,
                            source=src_name,
                            source_record_id=sid,
                            payload=slim,
                        ):
                            inserted += 1
                        else:
                            duplicates += 1
                    session.commit()
                    assert_run_record_count(session, run.id, expected=inserted)
                    run.status = "success"
                except Exception as e:
                    session.rollback()
                    inserted = count_run_records(session, run.id)
                    run.status = "failed"
                    run.error_message = str(e)[:500]
                    print(f"  [{src_name}] {j['abbr']} 失败: {e}")
                finally:
                    run.records_fetched = inserted
                    run.finished_at = datetime.utcnow()
                    session.merge(run); session.commit()
                print(
                    f"  [{src_name}] {j['abbr']}: "
                    f"API {seen} / 新增 {inserted} / 重复 {duplicates}"
                )
    finally:
        session.close()


def step_fetch_arxiv_incremental():
    """arXiv 近 14 天的论文（用现有关键词）。"""
    since = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"\n=== arXiv 增量 (since {since}) ===")
    session = get_session(DB_PATH)
    try:
        failed_categories = []
        for cat in ARXIV_CATEGORIES:
            run = SourceRun(source="arxiv", journal_abbr=None,
                            params={"mode": "incremental", "category": cat, "since": since})
            session.add(run); session.flush()
            seen = 0
            inserted = 0
            duplicates = 0
            try:
                for rec in fetch_category(cat, from_date=since):
                    seen += 1
                    if insert_raw_record(
                        session,
                        run_id=run.id,
                        source="arxiv",
                        source_record_id=rec["arxiv_id"],
                        payload=rec,
                    ):
                        inserted += 1
                    else:
                        duplicates += 1
                session.commit()
                assert_run_record_count(session, run.id, expected=inserted)
                run.status = "success"
            except Exception as e:
                session.rollback()
                inserted = count_run_records(session, run.id)
                run.status = "failed"
                run.error_message = str(e)[:500]
                failed_categories.append(cat)
                print(f"  arXiv {cat} 失败: {e}")
            finally:
                run.records_fetched = inserted
                run.finished_at = datetime.utcnow()
                session.merge(run); session.commit()
            print(
                f"  arXiv {cat}: "
                f"API {seen} / 新增 {inserted} / 重复 {duplicates}"
            )
        if failed_categories:
            _run_arxiv_openalex_fallback(
                session,
                since=since,
                until=today,
                failed_categories=failed_categories,
            )
    finally:
        session.close()


def _run_arxiv_openalex_fallback(
    session,
    *,
    since: str,
    until: str,
    failed_categories: list[str],
) -> None:
    print(
        "\n=== arXiv OpenAlex 回退 "
        f"({since} → {until}; 分类失败 {len(failed_categories)}) ==="
    )
    run = SourceRun(
        source="arxiv",
        journal_abbr=None,
        params={
            "mode": "openalex_fallback",
            "since": since,
            "until": until,
            "failed_categories": failed_categories,
        },
    )
    session.add(run)
    session.flush()
    seen = 0
    inserted = 0
    duplicates = 0
    try:
        for record in fetch_arxiv_via_openalex(since, until_date=until):
            seen += 1
            if insert_raw_record(
                session,
                run_id=run.id,
                source="arxiv",
                source_record_id=record["arxiv_id"],
                payload=record,
            ):
                inserted += 1
            else:
                duplicates += 1
        session.commit()
        assert_run_record_count(session, run.id, expected=inserted)
        run.status = "success"
    except Exception as error:
        session.rollback()
        inserted = count_run_records(session, run.id)
        run.status = "failed"
        run.error_message = str(error)[:500]
        print(f"  arXiv OpenAlex 回退失败: {error}")
    finally:
        run.records_fetched = inserted
        run.finished_at = datetime.utcnow()
        session.merge(run)
        session.commit()
    print(
        f"  arXiv OpenAlex 回退: "
        f"候选 {seen} / 新增 {inserted} / 重复 {duplicates}"
    )


def step_normalize():
    print("\n=== Normalize (英文+arXiv) ===")
    normalize_english(journal_abbr=None, only_new=True)
    normalize_arxiv()


def _window():
    """返回候选时间窗口 (since_dt, since_date, today)。"""
    days = int(os.getenv("LLM_CANDIDATE_LOOKBACK_DAYS", str(LOOKBACK_DAYS)))
    since_dt_obj = datetime.utcnow() - timedelta(days=days)
    since_dt = since_dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    since_date = since_dt_obj.strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return since_dt, since_date, today


def select_enrichment_candidate_ids(limit: int) -> tuple[int, list[int]]:
    """已打分(双≥3)但缺 TL;DR 或中文标题的论文，独立配额，优先补全。

    与打分队列分开，避免新论文每天占满 LLM 限额导致积压永不消减。
    缺 TL;DR 的排在前面（这是页面上直接可见的内容）。
    """
    from sqlalchemy import text

    session = get_session(DB_PATH)
    try:
        total = session.execute(text("""
            SELECT COUNT(*)
            FROM papers p
            JOIN paper_scores s ON s.paper_id = p.id
            LEFT JOIN llm_outputs o ON o.paper_id = p.id
            WHERE s.ai_relevance >= 3 AND s.domain_relevance >= 3
              AND (
                o.paper_id IS NULL
                OR o.tldr_zh IS NULL
                OR o.tldr_zh = ''
                OR p.title_zh IS NULL
                OR p.title_zh = ''
              )
        """)).scalar() or 0
        ids = session.execute(text("""
            SELECT p.id
            FROM papers p
            JOIN paper_scores s ON s.paper_id = p.id
            LEFT JOIN llm_outputs o ON o.paper_id = p.id
            WHERE s.ai_relevance >= 3 AND s.domain_relevance >= 3
              AND (
                o.paper_id IS NULL
                OR o.tldr_zh IS NULL
                OR o.tldr_zh = ''
                OR p.title_zh IS NULL
                OR p.title_zh = ''
              )
            ORDER BY
              CASE
                WHEN o.paper_id IS NULL OR o.tldr_zh IS NULL OR o.tldr_zh = '' THEN 0
                ELSE 1
              END,
              COALESCE(p.pub_date, '') DESC,
              COALESCE(p.created_at, '') DESC,
              p.id DESC
            LIMIT :limit
        """), {"limit": limit}).scalars().all()
        return total, [int(pid) for pid in ids]
    finally:
        session.close()


def select_scoring_candidate_ids(limit: int) -> tuple[int, list[int]]:
    """尚未打分的新论文（近窗口优先），用独立的打分配额。"""
    from sqlalchemy import text

    since_dt, since_date, today = _window()
    session = get_session(DB_PATH)
    try:
        total = session.execute(text("""
            SELECT COUNT(*)
            FROM papers p
            LEFT JOIN paper_scores s ON s.paper_id = p.id
            WHERE s.paper_id IS NULL OR s.ai_relevance IS NULL
        """)).scalar() or 0
        ids = session.execute(text("""
            SELECT p.id
            FROM papers p
            LEFT JOIN paper_scores s ON s.paper_id = p.id
            WHERE s.paper_id IS NULL OR s.ai_relevance IS NULL
            ORDER BY
              CASE
                WHEN p.pub_date >= :since_date AND p.pub_date <= :today THEN 0
                WHEN p.created_at >= :since_dt THEN 1
                ELSE 2
              END,
              COALESCE(p.pub_date, '') DESC,
              COALESCE(p.created_at, '') DESC,
              p.id DESC
            LIMIT :limit
        """), {"since_date": since_date, "since_dt": since_dt,
               "today": today, "limit": limit}).scalars().all()
        return total, [int(pid) for pid in ids]
    finally:
        session.close()


def step_llm():
    """两阶段 LLM：
    1) 补全队列：已打分但缺 TL;DR/中文标题的论文（独立配额，优先跑）
    2) 打分队列：新论文打分 + 为刚打分的生成 TL;DR/标题
    """
    if not os.getenv("MINIMAX_API_KEY"):
        print("\n[skip] 无 MINIMAX_API_KEY，跳过 LLM 步骤")
        return

    SAFETY_LIMIT = int(os.getenv("LLM_SAFETY_LIMIT", "500"))
    DAILY_LIMIT = int(os.getenv("LLM_DAILY_LIMIT", "200"))
    BACKFILL_LIMIT = int(os.getenv("LLM_BACKFILL_DAILY_LIMIT", "100"))
    backfill_enabled = os.getenv("LLM_BACKFILL_ENRICHMENT", "true").lower() in {
        "1", "true", "yes", "on"
    }
    if DAILY_LIMIT <= 0:
        print("\n[skip] LLM_DAILY_LIMIT <= 0，跳过 LLM 步骤")
        return

    llm_workers = int(os.getenv("LLM_WORKERS", "4"))
    title_batch_size = int(os.getenv("LLM_TITLE_BATCH_SIZE", "3"))
    print(f"[LLM] 并发数: {llm_workers}")

    # ---------- 阶段 1：补全已打分论文的 TL;DR / 中文标题 ----------
    if backfill_enabled:
        backfill_limit = min(BACKFILL_LIMIT, SAFETY_LIMIT)
        enrich_total, enrich_ids = select_enrichment_candidate_ids(backfill_limit)
        print(
            f"\n[补全队列] 缺 TL;DR/中文标题 {enrich_total} 篇；"
            f"本次上限 {backfill_limit} 篇"
        )
        if enrich_ids:
            print("\n=== LLM TL;DR（补全）===")
            llm_tldr_run(batch_size=3, n_workers=llm_workers, candidate_ids=enrich_ids)
            print("\n=== LLM 中文标题翻译（补全）===")
            llm_title_zh_run(batch_size=title_batch_size, n_workers=llm_workers,
                             candidate_ids=enrich_ids)
        else:
            print("[skip] 补全队列为空")
    else:
        print("\n[skip] LLM_BACKFILL_ENRICHMENT=false，跳过补全队列")

    # ---------- 阶段 2：新论文打分 ----------
    run_limit = min(DAILY_LIMIT, SAFETY_LIMIT)
    score_total, score_ids = select_scoring_candidate_ids(run_limit)
    print(
        f"\n[打分队列] 待打分 {score_total} 篇；本次上限 {run_limit} 篇"
    )
    if not score_ids:
        print("[skip] 打分队列为空")
        return
    if score_total > len(score_ids):
        print(f"⚠️  待打分超过本次上限，仅处理前 {len(score_ids)} 篇")

    print("\n=== LLM 双打分 ===")
    llm_score_run(batch_size=12, n_workers=llm_workers, candidate_ids=score_ids)

    print("\n=== LLM TL;DR（新打分）===")
    llm_tldr_run(batch_size=3, n_workers=llm_workers, candidate_ids=score_ids)

    print("\n=== LLM 中文标题翻译（新打分）===")
    llm_title_zh_run(batch_size=title_batch_size, n_workers=llm_workers,
                     candidate_ids=score_ids)


def step_audit_export():
    print("\n=== Coverage 审计 ===")
    audit()
    print("\n=== 导出前端数据 ===")
    export_data()


def main():
    t0 = time.time()
    ensure_db()
    step_fetch_english_incremental()
    step_fetch_arxiv_incremental()
    step_normalize()
    step_llm()
    step_audit_export()
    elapsed = (time.time() - t0) / 60
    print(f"\n=== 增量更新完成，用时 {elapsed:.1f} 分钟 ===")


if __name__ == "__main__":
    main()

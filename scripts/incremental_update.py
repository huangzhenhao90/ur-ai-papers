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
from sqlalchemy.exc import IntegrityError

from src.db.schema import get_session, SourceRun, RawRecord, init_db
from src.utils.journals import english_journals
from src.connectors import openalex as oa
from src.connectors import crossref as cr
from src.connectors.arxiv import fetch_category, UR_KEYWORDS
from src.pipeline.normalize import normalize as normalize_english
from src.pipeline.ingest_arxiv import normalize_arxiv
from src.pipeline.coverage_audit import audit
from src.pipeline.export_web_data import main as export_data
from src.pipeline.llm_score_parallel import run as llm_score_run
from src.pipeline.llm_tldr import run as llm_tldr_run
from src.pipeline.llm_title_zh import run as llm_title_zh_run

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
                count = 0
                try:
                    for w in fetch():
                        slim = (cr.slim_record(w) if src_name == "crossref" else oa.slim_record(w))
                        sid = slim.get("doi") if src_name == "crossref" else slim.get("id")
                        if not sid:
                            continue
                        rec = RawRecord(run_id=run.id, source=src_name, source_record_id=sid, payload=slim)
                        session.add(rec)
                        try:
                            session.flush()
                            count += 1
                        except IntegrityError:
                            session.rollback()
                            continue
                    session.commit()
                    run.status = "success"
                except Exception as e:
                    session.rollback()
                    run.status = "failed"
                    run.error_message = str(e)[:500]
                    print(f"  [{src_name}] {j['abbr']} 失败: {e}")
                finally:
                    run.records_fetched = count
                    run.finished_at = datetime.utcnow()
                    session.merge(run); session.commit()
                if count:
                    print(f"  [{src_name}] {j['abbr']}: +{count}")
    finally:
        session.close()


def step_fetch_arxiv_incremental():
    """arXiv 近 14 天的论文（用现有关键词）。"""
    since = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    print(f"\n=== arXiv 增量 (since {since}) ===")
    session = get_session(DB_PATH)
    try:
        for cat in ARXIV_CATEGORIES:
            run = SourceRun(source="arxiv", journal_abbr=None,
                            params={"mode": "incremental", "category": cat, "since": since})
            session.add(run); session.flush()
            count = 0
            try:
                for rec in fetch_category(cat, from_date=since):
                    raw = RawRecord(run_id=run.id, source="arxiv",
                                    source_record_id=rec["arxiv_id"], payload=rec)
                    session.add(raw)
                    try:
                        session.flush(); count += 1
                    except IntegrityError:
                        session.rollback(); continue
                session.commit()
                run.status = "success"
            except Exception as e:
                session.rollback()
                run.status = "failed"
                run.error_message = str(e)[:500]
                print(f"  arXiv {cat} 失败: {e}")
            finally:
                run.records_fetched = count
                run.finished_at = datetime.utcnow()
                session.merge(run); session.commit()
            if count:
                print(f"  arXiv {cat}: +{count}")
    finally:
        session.close()


def step_normalize():
    print("\n=== Normalize (英文+arXiv) ===")
    normalize_english(journal_abbr=None, only_new=True)
    normalize_arxiv()


def step_llm():
    """打分新增论文 + 生成 TL;DR。如果没有 API key 则跳过 LLM 步骤。"""
    if not os.getenv("MINIMAX_API_KEY"):
        print("\n[skip] 无 MINIMAX_API_KEY，跳过 LLM 步骤")
        return

    # 安全阀：检查未打分论文数量，过多则报警退出（避免初次跑或 cache miss 烧钱）
    from sqlalchemy import select, func
    from src.db.schema import Paper, PaperScore
    session = get_session(DB_PATH)
    try:
        scored_ids = set(session.execute(select(PaperScore.paper_id)).scalars().all())
        total_papers = session.execute(select(func.count(Paper.id))).scalar() or 0
        unscored = total_papers - len(scored_ids)
    finally:
        session.close()

    SAFETY_LIMIT = int(os.getenv("LLM_SAFETY_LIMIT", "500"))
    print(f"\n[安全阀] 待打分 {unscored} 篇 / 阈值 {SAFETY_LIMIT}")
    if unscored > SAFETY_LIMIT:
        print(f"⚠️  待打分超阈值，跳过 LLM 步骤（避免烧钱）")
        print(f"   如需强制跑，本地手动: python -m src.pipeline.llm_score_parallel")
        return

    if unscored == 0:
        print("[skip] 无新增论文，跳过 LLM")
        return

    print("\n=== LLM 双打分 ===")
    llm_score_run(batch_size=12, n_workers=50)

    print("\n=== LLM TL;DR ===")
    llm_tldr_run(batch_size=3, n_workers=20)

    print("\n=== LLM 中文标题翻译 ===")
    llm_title_zh_run(batch_size=10, n_workers=50)


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

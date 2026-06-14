"""
arXiv connector：用 query API 按 [分类 + 用户研究/HCI/UX/CX 关键词] 历史回溯。

设计要点：
- arXiv 没有 user research 子分类，必须用关键词预过滤（与期刊抓取不同）
- query 语法: cat:cs.HC AND (abs:usability OR abs:"user experience" OR ...)
- arXiv API 返回 Atom XML，每次最多 2000 条
- 限速：3 秒/请求（官方建议）
- 时间过滤：用 submittedDate:[20230101 TO 99991231]

输出 slim_record 格式与 OpenAlex/Crossref 对齐，便于 normalizer 复用。
"""

import os
import time
import re
from typing import Iterator
import feedparser
from src.utils.http import make_client, get_with_retry

ARXIV_API = "https://export.arxiv.org/api/query"
PER_PAGE = 200       # 实际每次拉 200，太大易超时
RATE_SLEEP = 3.0     # arXiv 官方限速建议

# 用户研究 / HCI / UX / CX 强信号关键词
# 砍掉在 AI 论文里被滥用的词：user（太宽）、model、performance 等。
# 这套词强调「人在 AI 系统中的体验、研究方法、行为」。
UR_KEYWORDS = [
    # 用户研究方法（核心强信号）
    "user research", "UX research", "usability study", "usability testing",
    "usability evaluation", "user study", "user studies",
    "field study", "field experiment", "in-the-wild study",
    "diary study", "contextual inquiry",
    "in-depth interview", "semi-structured interview",
    "focus group", "card sorting", "think-aloud",
    "eye-tracking", "eye tracking",
    "survey design", "questionnaire",
    "persona", "personas", "customer journey", "user journey", "journey map",
    # UX / UCD
    "user experience", "UX design",
    "user-centered design", "user-centred design",
    "human-centered design", "human-centred design",
    "interaction design", "service design", "experience design",
    # HCI（强信号）
    "human-computer interaction",
    "user interface", "interaction technique",
    "user perception", "user engagement",
    "user satisfaction", "user trust", "user acceptance",
    "technology acceptance",
    "user behavior", "user behaviour",
    # CX / 消费者
    "customer experience", "consumer experience",
    "consumer behavior", "consumer behaviour",
    "consumer research", "consumer psychology",
    "willingness to pay", "personalization",
    "personalized recommendation", "recommender system",
    # AI × 用户视角
    "human-AI interaction", "human-AI collaboration", "human-AI trust",
    "human-AI teaming", "AI-augmented work", "AI-mediated",
    "algorithm aversion", "algorithm appreciation",
    "trust in AI", "conversational agent",
    # 众包 / 在线社区
    "crowdsourcing", "crowdworker", "online community",
]


def _build_query(category: str, kw_chunk: list[str]) -> str:
    """Build arXiv API search query. abstract OR title 任一命中关键词。"""
    kw_clauses = []
    for kw in kw_chunk:
        kw_q = f'"{kw}"' if " " in kw else kw
        kw_clauses.append(f"abs:{kw_q}")
        kw_clauses.append(f"ti:{kw_q}")
    kw_part = "(" + " OR ".join(kw_clauses) + ")"
    return f"cat:{category} AND {kw_part}"


def fetch_category(
    category: str,
    from_date: str = "2023-01-01",
    until_date: str | None = None,
    max_results: int = 5000,
) -> Iterator[dict]:
    """生成器：按分类抓取与用户研究/HCI 相关的 arXiv 论文。

    arXiv 的 query 长度有限制（~6000 字符），所以关键词分块发请求。
    每块用 cursor pagination。
    """
    # 把关键词切成 ~10 个一块，避免 URL 过长
    chunks = [UR_KEYWORDS[i : i + 10] for i in range(0, len(UR_KEYWORDS), 10)]

    seen_ids = set()
    with make_client(timeout=60) as client:
        for chunk_idx, chunk in enumerate(chunks):
            query = _build_query(category, chunk)
            start = 0
            print(f"  [chunk {chunk_idx+1}/{len(chunks)}] cat={category} kw={chunk[:3]}...")
            while start < max_results:
                params = {
                    "search_query": query,
                    "start": start,
                    "max_results": PER_PAGE,
                    "sortBy": "submittedDate",
                    "sortOrder": "descending",
                }
                try:
                    r = get_with_retry(client, ARXIV_API, params=params)
                except Exception as e:
                    print(f"    ! API error: {e}")
                    break
                feed = feedparser.parse(r.text)
                entries = feed.entries
                if not entries:
                    break

                yielded_in_page = 0
                for e in entries:
                    rec = parse_entry(e)
                    if not rec:
                        continue
                    pub = rec.get("publication_date") or ""
                    # 历史窗过滤
                    if pub and pub < from_date:
                        continue
                    if until_date and pub and pub > until_date:
                        continue
                    if rec["arxiv_id"] in seen_ids:
                        continue
                    seen_ids.add(rec["arxiv_id"])
                    yield rec
                    yielded_in_page += 1

                # 当结果按时间倒排时，遇到日期早于 from_date 的就该停
                if entries and (entries[-1].get("published") or "")[:10] < from_date:
                    break

                start += PER_PAGE
                if len(entries) < PER_PAGE:
                    break
                time.sleep(RATE_SLEEP)
            time.sleep(RATE_SLEEP)


def parse_entry(e) -> dict | None:
    """parse 一个 feedparser entry。"""
    arxiv_url = e.get("id", "")  # 形如 http://arxiv.org/abs/2401.12345v2
    m = re.search(r"arxiv\.org/abs/([\w.\-]+?)(?:v\d+)?$", arxiv_url)
    if not m:
        return None
    arxiv_id = m.group(1)

    title = (e.get("title") or "").replace("\n", " ").strip()
    summary = (e.get("summary") or "").replace("\n", " ").strip()
    published = (e.get("published") or "")[:10]
    updated = (e.get("updated") or "")[:10]

    authors = [
        {"name": a.get("name")} for a in (e.get("authors") or [])
    ]
    cats = [t.get("term") for t in (e.get("tags") or [])]

    # arXiv 有时给 doi, journal_ref
    doi = None
    for link in (e.get("links") or []):
        if link.get("title") == "doi":
            doi = link.get("href", "").replace("http://dx.doi.org/", "")
    arxiv_doi = e.get("arxiv_doi") if hasattr(e, "arxiv_doi") else None
    if not doi and arxiv_doi:
        doi = arxiv_doi

    pdf_url = None
    abs_url = None
    for link in (e.get("links") or []):
        if link.get("type") == "application/pdf":
            pdf_url = link.get("href")
        elif link.get("rel") == "alternate":
            abs_url = link.get("href")

    return {
        "arxiv_id": arxiv_id,
        "doi": doi,
        "title": title,
        "abstract": summary,
        "authors": authors,
        "publication_date": published,
        "updated_date": updated,
        "publication_year": int(published[:4]) if published[:4].isdigit() else None,
        "categories": cats,
        "primary_category": cats[0] if cats else None,
        "pdf_url": pdf_url,
        "landing_page_url": abs_url or arxiv_url,
    }

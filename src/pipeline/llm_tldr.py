"""
为双≥3 的论文生成中文 TL;DR + 主题标签 + AI 类型标签。

输出写入 llm_outputs 表：
- tldr_zh: 200 字以内中文摘要
- topic_tags: ["可用性", "用户研究方法", "人机协作", "AI 助手", "消费者行为", ...]
- ai_type_tags: ["GenAI", "LLM", "ChatGPT", "对话式 AI", "AI 助手", ...]

batch=5（每篇输出量大），并发 50。
"""

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
from sqlalchemy import select, text

from src.db.schema import get_session, Paper, PaperScore, LlmOutput
from src.llm.client import MiniMaxClient, extract_json

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./data/papers.db")

BATCH_SIZE = 5
N_WORKERS = 50
ABS_TRUNC = 1500  # TL;DR 任务给更长摘要

# 候选标签（鼓励复用，但 LLM 可自由生成新的）
TOPIC_TAG_HINTS = [
    "可用性", "用户研究方法", "用户体验(UX)", "人机交互(HCI)",
    "用户画像", "用户旅程", "客户体验(CX)", "消费者行为",
    "访谈", "问卷", "可用性测试", "眼动追踪", "田野研究",
    "推荐系统", "个性化", "AI 助手", "对话式 AI", "智能体",
    "人机协作", "信任", "可解释性", "可访问性", "伦理",
    "AI 治理", "AI 采用", "AI 拒绝", "AI 依赖",
    "营销", "广告", "服务设计", "服务体验",
    "可视化", "情感计算", "多模态交互",
    "儿童/老人/无障碍", "在线社区", "众包",
]

AI_TYPE_HINTS = [
    "GenAI", "LLM", "ChatGPT", "图像生成", "多模态",
    "智能体/Agent", "对话式 AI", "AI 助手",
    "推荐算法", "AI 推荐", "AI 客服", "AI 招聘",
    "AI 访谈分析", "AI 问卷生成", "AI 用户画像",
    "人机协作", "人机交互", "AI 治理", "可解释 AI",
    "情感识别", "眼动 AI", "可用性 AI",
]

SYSTEM_PROMPT = f"""你是一名精通用户研究、人机交互(HCI)、用户体验(UX)、客户体验(CX)的中文学术编辑。
任务：对一批与 AI 相关的顶刊/顶会论文，输出三件事：
1) 200 字以内中文 TL;DR：交代研究问题、方法、核心发现，不写"本文提出/本文研究"。
2) topic_tags：从论文实质议题中提取 1-4 个学科标签。可参考但不局限于：{", ".join(TOPIC_TAG_HINTS)}
3) ai_type_tags：标识涉及的 AI 技术或议题。可参考但不局限于：{", ".join(AI_TYPE_HINTS)}

【关键】必须为输入的每一篇论文返回一个对象，id 严格对应 [p1] [p2] ... 编号。

输出格式（严格 JSON 数组，无任何 markdown 包裹）：
[{{"id":"p1","tldr":"...","topic_tags":["..."],"ai_type_tags":["..."]}}, ...]
"""

USER_TEMPLATE = """请处理以下 {n} 篇论文，每篇返回 tldr + 标签：

{papers}

只输出长度为 {n} 的 JSON 数组。tldr 控制在 200 字内。"""


def fmt_paper(idx: int, p: dict) -> str:
    abs_text = (p.get("abstract") or "")[:ABS_TRUNC]
    abs_part = f"\n摘要: {abs_text}" if abs_text else "\n（无摘要，请仅根据标题推断）"
    return f"[p{idx}] 期刊={p.get('journal_abbr')} 标题: {p.get('title')}{abs_part}"


def process_one_batch(client, batch_idx, batch):
    body = "\n\n".join(fmt_paper(i + 1, p) for i, p in enumerate(batch))
    user = USER_TEMPLATE.format(n=len(batch), papers=body)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
    # TL;DR 任务输出量大：reasoning 1500-2000 + 每篇输出 ~600 token
    max_tok = 3000 + 800 * len(batch)
    try:
        data = client.chat(messages, max_tokens=max_tok, temperature=0.2)
    except Exception as e:
        return [(p["id"], {"error": str(e)[:200]}) for p in batch]

    usage = client.usage(data)
    raw_text = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    parsed = extract_json(raw_text) or []

    score_by_idx = {}
    for s in parsed:
        sid = str(s.get("id", "")).lower().lstrip("p")
        try:
            idx = int(sid) - 1
            score_by_idx[idx] = s
        except (ValueError, TypeError):
            continue

    if len(score_by_idx) < len(batch) // 2:
        print(f"  ! batch_idx={batch_idx} 仅解析 {len(score_by_idx)}/{len(batch)} raw[:200]: {raw_text[:200]!r}")

    out = []
    for i, p in enumerate(batch):
        s = score_by_idx.get(i)
        if s is None:
            out.append((p["id"], {"error": "LLM 漏返回"}))
        else:
            out.append((p["id"], {
                "tldr_zh": (s.get("tldr") or "")[:500],
                "topic_tags": s.get("topic_tags") or [],
                "ai_type_tags": s.get("ai_type_tags") or [],
                "_usage": usage,
            }))
    return out


# 全局统计
stats_lock = threading.Lock()
g_in = g_out = g_reason = 0
g_done = g_ok = g_fail = 0


def run(min_ai: float = 3.0, min_dom: float = 3.0, batch_size: int = BATCH_SIZE,
        n_workers: int = N_WORKERS, limit: int | None = None):
    global g_in, g_out, g_reason, g_done, g_ok, g_fail

    session = get_session(DB_PATH)
    client = MiniMaxClient()
    try:
        existing = set(session.execute(select(LlmOutput.paper_id)).scalars().all())

        # 选 ai≥3 且 domain≥3 且尚无 tldr 的论文
        rows = session.execute(text("""
            SELECT p.id, p.title, p.abstract, p.journal_abbr
            FROM papers p
            JOIN paper_scores s ON s.paper_id = p.id
            WHERE s.ai_relevance >= :ai AND s.domain_relevance >= :dom
            ORDER BY p.pub_year DESC, p.id
        """), {"ai": min_ai, "dom": min_dom}).all()

        todo = [
            {"id": r[0], "title": r[1], "abstract": r[2], "journal_abbr": r[3]}
            for r in rows if r[0] not in existing
        ]
        if limit:
            todo = todo[:limit]

        print(f"待生成 TL;DR: {len(todo)} 篇 (ai≥{min_ai} AND dom≥{min_dom}) batch={batch_size} workers={n_workers}")
        batches = [todo[i : i + batch_size] for i in range(0, len(todo), batch_size)]
        print(f"共 {len(batches)} 个 batch")

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futs = {pool.submit(process_one_batch, client, i, b): i for i, b in enumerate(batches)}
            for fut in as_completed(futs):
                results = fut.result()
                for paper_id, data in results:
                    if "error" in data:
                        with stats_lock:
                            g_fail += 1; g_done += 1
                        # 不写库，下次可重跑
                        continue
                    usage = data.pop("_usage", {})
                    with stats_lock:
                        g_in += usage.get("prompt_tokens", 0)
                        g_out += usage.get("completion_tokens", 0)
                        g_reason += (usage.get("completion_tokens_details") or {}).get("reasoning_tokens", 0)

                    obj = session.get(LlmOutput, paper_id)
                    if obj:
                        obj.tldr_zh = data["tldr_zh"]
                        obj.topic_tags = data["topic_tags"]
                        obj.ai_type_tags = data["ai_type_tags"]
                        obj.model_used = client.model
                        obj.generated_at = datetime.utcnow()
                    else:
                        session.add(LlmOutput(
                            paper_id=paper_id,
                            tldr_zh=data["tldr_zh"],
                            topic_tags=data["topic_tags"],
                            ai_type_tags=data["ai_type_tags"],
                            model_used=client.model,
                            generated_at=datetime.utcnow(),
                        ))
                    with stats_lock:
                        g_ok += 1; g_done += 1
                session.commit()

                elapsed = time.time() - t0
                print(f"  [{g_done}/{len(todo)}] ok={g_ok} fail={g_fail} "
                      f"in={g_in} out={g_out} reason={g_reason} elapsed={elapsed:.0f}s")

        print(f"\n完成: 成功 {g_ok} / 失败 {g_fail} / 用时 {(time.time()-t0)/60:.1f} 分钟")
        cost = g_in / 1e6 * 1.2 + g_out / 1e6 * 8
        print(f"Token: in={g_in} out={g_out} (reasoning={g_reason})")
        print(f"估算成本: ¥{cost:.2f}")
    finally:
        session.close()
        client.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--batch", type=int, default=BATCH_SIZE)
    p.add_argument("--workers", type=int, default=N_WORKERS)
    p.add_argument("--min-ai", type=float, default=3.0)
    p.add_argument("--min-dom", type=float, default=3.0)
    args = p.parse_args()
    run(min_ai=args.min_ai, min_dom=args.min_dom,
        batch_size=args.batch, n_workers=args.workers, limit=args.limit)

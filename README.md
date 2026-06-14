# UR-AI-Papers

聚合 2023-01 至今 HCI / UX / 消费者 / CX / 用研方法论顶刊与顶会中**与 AI（尤其 GenAI/LLM/智能体）相关**的论文。

## 范围

- **23 本期刊 + 12 个顶会 + 3 本中文刊**（英文 HCI/UX 8 + 消费者/营销 6 + CX/服务 4 + 用研方法论 3 + 顶会 12 + 中文 3）
- **arXiv** 9 个分类（cs.HC, cs.CY, cs.CL, cs.AI, cs.SI, cs.IR, stat.ME, stat.ML, econ.GN）
- 时间窗：2023-01-01 至今
- 召回原则：**期刊全量入库 → 后置 AI 相关性判定**（关键词不作召回闸门）；arXiv 用用研/HCI 强信号词预过滤

## 架构

```
GitHub Actions cron
  ↓ Source Registry (config/journals.yaml)
  ↓ Connectors (crossref / openalex / arxiv / semantic_scholar / unpaywall)
  ↓ raw_records (永不删)
  ↓ normalizer → deduper → coverage_auditor
  ↓ enrichment_queue (摘要/引用/OA/PDF 补全)
  ↓ llm_queue (MiniMax-M2.5-lightning: 双打分 + TL;DR + 标签)
  ↓ publish_index → Next.js 前端
```

## 目录结构

```
ur-ai-papers/
├── config/              # journals.yaml, keywords.yaml
├── src/
│   ├── connectors/      # crossref, openalex, arxiv, semantic_scholar, unpaywall
│   ├── pipeline/        # normalize, dedupe, coverage_audit, enrich, llm_score
│   ├── db/              # schema, migrations
│   ├── llm/             # MiniMax client, prompts
│   └── utils/           # 通用工具
├── data/
│   ├── raw/             # 原始 API 响应（JSON）
│   ├── cnki_imports/    # 用户手动导出的 RIS/Endnote 文件
│   └── exports/         # 数据库导出（不入 git）
├── scripts/             # 一次性脚本
├── web/                 # Next.js 前端
├── docs/
└── logs/
```

## 与 ob-ai-papers 的差异

- **主题**：组织行为/营销 → 用户研究/HCI/UX/CX
- **arXiv 分类**：8 个 → 9 个（加 cs.IR、stat.ME；去 cs.LG）
- **arXiv 关键词**：OB/HR/管理词 → 用研方法/UX/HCI 词
- **信息源**：新增 Semantic Scholar（连接器已就位）；保留 OpenAlex/Crossref/Unpaywall
- **会议覆盖**：新增 12 个 HCI/IS 顶会（CHI、CSCW、UIST、DIS、IUI、HRI 等）
- **代码骨架**：完全复用，仅替换 config 和 prompt 文案

"use client";

import { useEffect, useState } from "react";
import PaperList, { type Paper } from "@/components/PaperList";
import { comparePapersByRecent } from "@/lib/paperSort";

const DAYS = 7;

function ingestedDate(paper: Paper): string | null {
  return paper.ingested_at ? paper.ingested_at.slice(0, 10) : paper.date;
}

function compareByIngestedAt(a: Paper, b: Paper): number {
  const ingestDiff = (b.ingested_at || "").localeCompare(a.ingested_at || "");
  if (ingestDiff !== 0) return ingestDiff;
  return comparePapersByRecent(a, b);
}

export default function RecentPage() {
  const [papers, setPapers] = useState<Paper[]>([]);
  const [meta, setMeta] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch("/data/papers.json").then((r) => r.json()),
      fetch("/data/meta.json").then((r) => r.json()),
    ]).then(([all, m]: [Paper[], any]) => {
      // 取近 N 天新增入库的论文；论文卡片仍展示实际发表日期。
      const since = new Date(Date.now() - DAYS * 24 * 3600 * 1000);
      const sinceStr = since.toISOString().slice(0, 10);
      const untilStr = new Date().toISOString().slice(0, 10);
      const recent = all
        .filter((p) => {
          const d = ingestedDate(p);
          return d && d >= sinceStr && d <= untilStr;
        })
        .sort(compareByIngestedAt);
      setPapers(recent);
      setMeta(m);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="text-stone-500 text-sm py-20 text-center">加载中…</div>;

  const since = new Date(Date.now() - DAYS * 24 * 3600 * 1000).toISOString().slice(0, 10);
  const until = new Date().toISOString().slice(0, 10);

  const banner = (
    <div className="mb-3 p-3 bg-accent/5 border-l-4 border-accent rounded-r">
      <div className="text-accent font-semibold text-sm">📅 本周新增</div>
      <div className="text-xs text-stone-600 mt-0.5">
        过去 {DAYS} 天内新增入库的论文 · {since} 至 {until} · 共 {papers.length} 篇
      </div>
    </div>
  );

  return (
    <PaperList
      papers={papers}
      meta={meta}
      banner={banner}
    />
  );
}

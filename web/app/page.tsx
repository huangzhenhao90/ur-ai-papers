"use client";

import { useEffect, useState } from "react";
import PaperList, { type Paper } from "@/components/PaperList";

export default function HomePage() {
  const [papers, setPapers] = useState<Paper[]>([]);
  const [meta, setMeta] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch("/data/papers.json").then((r) => r.json()),
      fetch("/data/meta.json").then((r) => r.json()),
    ]).then(([p, m]) => {
      setPapers(p);
      setMeta(m);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="text-stone-500 text-sm py-20 text-center">加载中…</div>;

  return <PaperList papers={papers} meta={meta} />;
}

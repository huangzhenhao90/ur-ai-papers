import "./globals.css";
import type { Metadata } from "next";
import Link from "next/link";
import HeaderNav from "@/components/HeaderNav";
import { Analytics } from "@vercel/analytics/next";

export const metadata: Metadata = {
  title: "UR × AI Papers — 用户研究 / HCI / CX AI 论文索引",
  description: "聚合 2023 至今 HCI / UX / 消费者 / CX 顶刊与顶会中与 AI 相关的论文，含中文 TL;DR、主题标签与覆盖率审计。",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>
        <header className="border-b border-stone-200 bg-white sticky top-0 z-10">
          <div className="max-w-6xl mx-auto px-4 py-3 flex items-center justify-between gap-4">
            <Link href="/" className="flex items-baseline gap-2 min-w-0">
              <span className="font-mono font-semibold tracking-tight">
                UR × AI <span className="text-accent">Papers</span>
              </span>
              <span className="hidden sm:inline text-xs text-stone-500 truncate">
                用户研究 / HCI / CX AI 相关研究索引
              </span>
            </Link>
            <HeaderNav />
          </div>
        </header>
        <main className="max-w-6xl mx-auto px-4 py-6">{children}</main>
        <footer className="max-w-6xl mx-auto px-4 py-8 text-xs text-stone-500 border-t border-stone-200 mt-10">
          数据来源：OpenAlex + Crossref + Semantic Scholar · LLM：MiniMax-M2.5-lightning ·
          构建：<span className="font-mono">ur-ai-papers</span>
        </footer>
        <Analytics />
      </body>
    </html>
  );
}

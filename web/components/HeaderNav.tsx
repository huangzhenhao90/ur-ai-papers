"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import FavLink from "@/components/FavLink";

const navLinkBase = "px-1.5 py-1 border-b-2 transition-colors";
const navLinkActive = "border-accent text-accent font-medium";
const navLinkInactive = "border-transparent text-stone-600 hover:text-accent";

function isActive(pathname: string, href: string): boolean {
  if (href === "/") return pathname === "/";
  return pathname === href || pathname.startsWith(`${href}/`);
}

function NavLink({ href, children }: { href: string; children: React.ReactNode }) {
  const pathname = usePathname();
  const active = isActive(pathname, href);
  return (
    <Link
      href={href}
      className={`${navLinkBase} ${active ? navLinkActive : navLinkInactive}`}
      aria-current={active ? "page" : undefined}
    >
      {children}
    </Link>
  );
}

export default function HeaderNav() {
  return (
    <nav className="flex gap-3 text-sm items-center">
      <NavLink href="/recent">本周新增</NavLink>
      <NavLink href="/">全部论文</NavLink>
      <FavLink />
      <NavLink href="/about">关于</NavLink>
    </nav>
  );
}

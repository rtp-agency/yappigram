"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { api, clearTokens, disconnectWS, getTokens, isTelegramWebApp, getTgWebApp } from "./lib";

// ============================================================
// SVG Icons
// ============================================================

function IconChat({ className = "w-5 h-5" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  );
}

function IconTeam({ className = "w-5 h-5" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
      <circle cx="9" cy="7" r="4" />
      <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
      <path d="M16 3.13a4 4 0 0 1 0 7.75" />
    </svg>
  );
}

function IconSettings({ className = "w-5 h-5" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  );
}

function IconSend({ className = "w-5 h-5" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  );
}

function IconLogout({ className = "w-5 h-5" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
      <polyline points="16 17 21 12 16 7" />
      <line x1="21" y1="12" x2="9" y2="12" />
    </svg>
  );
}

// ============================================================
// Auth Guard
// ============================================================

export function AuthGuard({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const tokens = getTokens();
    if (!tokens) {
      router.replace("/login");
    } else {
      setReady(true);
    }
  }, [router]);

  if (!ready) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="w-8 h-8 border-2 border-brand/30 border-t-brand rounded-full animate-spin" />
      </div>
    );
  }
  return <>{children}</>;
}

// ============================================================
// Sidebar / Navigation
// ============================================================

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [isTg, setIsTg] = useState(false);
  const [isOrgTeam, setIsOrgTeam] = useState(false);

  // Fetch current user to check if they're in an organization (not personal workspace)
  useEffect(() => {
    const tokens = getTokens();
    if (!tokens) return;
    api("/api/staff/me")
      .then((data: any) => {
        if (data?.postforge_org_id && !data.postforge_org_id.startsWith("personal_")) {
          setIsOrgTeam(true);
        }
      })
      .catch(() => {});
  }, []);

  const navItems = [
    { href: "/chats", label: "Чаты", icon: IconChat },
    { href: "/broadcasts", label: "Рассылки", icon: IconSend },
    ...(isOrgTeam ? [{ href: "/team", label: "Команда", icon: IconTeam }] : []),
    { href: "/settings", label: "Настройки", icon: IconSettings },
  ];


  useEffect(() => {
    const tg = isTelegramWebApp();
    setIsTg(tg);
    if (tg) {
      const webapp = getTgWebApp();
      webapp?.ready();
      webapp?.expand();
    }
  }, []);

  // Telegram BackButton: show on non-home pages, navigate back
  useEffect(() => {
    if (!isTg) return;
    const webapp = getTgWebApp();
    if (!webapp) return;

    const isHome = pathname === "/chats";
    if (isHome) {
      webapp.BackButton.hide();
    } else {
      webapp.BackButton.show();
      const goBack = () => router.back();
      webapp.BackButton.onClick(goBack);
      return () => webapp.BackButton.offClick(goBack);
    }
  }, [isTg, pathname, router]);

  const logout = () => {
    disconnectWS();
    clearTokens();
    if (isTg) {
      getTgWebApp()?.close();
    } else {
      router.replace("/login");
    }
  };

  return (
    <div className="flex h-dvh" style={{ paddingTop: "env(safe-area-inset-top)" }}>
      {/* Desktop sidebar — hidden in TG Mini App */}
      {!isTg && (
        <nav className="hidden md:flex flex-col w-56 bg-gradient-to-b from-surface-card to-surface border-r border-surface-border p-4">
          <div className="flex items-center gap-2.5 mb-8">
            <div className="w-9 h-9 bg-brand/90 rounded-xl flex items-center justify-center flex-shrink-0 shadow-lg shadow-brand/20">
              <img src="/metra-icon.png" alt="" className="w-5 h-5 object-contain" />
            </div>
            <div>
              <img src="/metra-wordmark.png" alt="METRA" className="h-3.5 object-contain brightness-0 invert" />
              <div className="text-[10px] text-brand font-semibold tracking-widest mt-0.5">CRM</div>
            </div>
          </div>
          <div className="flex flex-col gap-1 flex-1">
            {navItems.map((item) => {
              const Icon = item.icon;
              const active = pathname.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 ${
                    active
                      ? "bg-brand/10 text-brand border border-brand/20 shadow-[0_0_12px_rgba(14,165,233,0.1)]"
                      : "text-slate-400 hover:text-white hover:bg-surface-hover border border-transparent"
                  }`}
                >
                  <Icon className={`w-[18px] h-[18px] ${active ? "text-brand" : ""}`} />
                  {item.label}
                </Link>
              );
            })}
          </div>
          <button
            onClick={logout}
            className="flex items-center gap-2 text-sm text-slate-500 hover:text-red-400 transition-colors mt-auto px-3 py-2"
          >
            <IconLogout className="w-4 h-4" />
            Выход
          </button>
        </nav>
      )}

      {/* Main content — pb for mobile bottom nav */}
      <main className="flex-1 min-h-0 overflow-auto pb-16 md:pb-0">{children}</main>

      {/* Mobile bottom nav */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 bg-surface-card/95 backdrop-blur-lg border-t border-surface-border flex justify-around pt-2 z-50" style={{ paddingBottom: "max(0.5rem, env(safe-area-inset-bottom))" }}>
        {navItems.map((item) => {
          const Icon = item.icon;
          const active = pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex flex-col items-center gap-0.5 text-xs transition-colors min-w-[44px] min-h-[44px] justify-center ${
                active ? "text-brand" : "text-slate-400"
              }`}
            >
              <Icon className="w-5 h-5" />
              {item.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}

// ============================================================
// Common UI
// ============================================================

export function Badge({ text, color = "#0ea5e9" }: { text: string; color?: string }) {
  return (
    <span
      className="px-2 py-0.5 rounded-full text-xs font-medium border"
      style={{
        backgroundColor: color + "15",
        color,
        borderColor: color + "30",
      }}
    >
      {text}
    </span>
  );
}

export function Button({
  children,
  onClick,
  variant = "primary",
  disabled = false,
  className = "",
}: {
  children: React.ReactNode;
  onClick?: () => void;
  variant?: "primary" | "secondary" | "danger" | "ghost";
  disabled?: boolean;
  className?: string;
}) {
  const styles = {
    primary:
      "bg-gradient-to-r from-brand to-brand-dark hover:from-brand-light hover:to-brand text-white border border-brand/30 shadow-[0_0_20px_rgba(14,165,233,0.15)] hover:shadow-[0_0_25px_rgba(14,165,233,0.25)]",
    secondary:
      "bg-surface-card hover:bg-surface-hover text-slate-200 border border-surface-border hover:border-slate-600",
    danger:
      "bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/20 hover:border-red-500/40",
    ghost:
      "bg-transparent hover:bg-surface-hover text-slate-400 hover:text-white border border-transparent",
  };

  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed ${styles[variant]} ${className}`}
    >
      {children}
    </button>
  );
}

export function Input({
  label,
  type = "text",
  value,
  onChange,
  placeholder,
}: {
  label?: string;
  type?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      {label && <label className="text-sm text-slate-400 font-medium">{label}</label>}
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="bg-surface-card border border-surface-border rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-brand/50 focus:shadow-[0_0_12px_rgba(14,165,233,0.1)] transition-all duration-200 placeholder:text-slate-600"
      />
    </div>
  );
}

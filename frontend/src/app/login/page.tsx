"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  isTelegramWebApp, tgAuth, tgSelectWorkspace, ssoAuth,
  getTgWebApp, getTokens, saveTokens,
  type TgWorkspace,
} from "@/lib";

export default function LoginPage() {
  const router = useRouter();
  const [status, setStatus] = useState<"loading" | "error" | "workspaces">("loading");
  const [workspaces, setWorkspaces] = useState<TgWorkspace[]>([]);
  const [selecting, setSelecting] = useState(false);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);

    // Pre-authenticated tokens (passed directly from PostForge parent) — always take priority
    const accessToken = params.get("access_token");
    const refreshToken = params.get("refresh_token");
    if (accessToken && refreshToken) {
      saveTokens({ access_token: accessToken, refresh_token: refreshToken, role: "operator" });
      router.replace("/chats");
      return;
    }

    // Already authenticated — go to chats
    if (getTokens()?.access_token) {
      router.replace("/chats");
      return;
    }

    // SSO token (standalone window open from PostForge)
    const ssoToken = params.get("sso_token");
    if (ssoToken) {
      ssoAuth(ssoToken).then((ok) => {
        if (ok) router.replace("/chats");
        else setStatus("error");
      });
      return;
    }

    // SSO: listen for PostForge token via postMessage (when embedded in iframe)
    function handleSsoMessage(event: MessageEvent) {
      const allowedOrigins = [window.location.origin, "https://metra-ai.org", "https://app.metra-ai.org"];
      if (!allowedOrigins.includes(event.origin)) return;
      if (event.data?.type === "postforge_sso" && event.data?.token) {
        ssoAuth(event.data.token).then((ok) => {
          if (ok) router.replace("/chats");
          else setStatus("error");
        });
      }
    }
    window.addEventListener("message", handleSsoMessage);

    // Telegram Mini App auth
    if (isTelegramWebApp()) {
      getTgWebApp()?.ready();
      getTgWebApp()?.expand();
      tgAuth().then((result) => {
        if (result.ok) {
          router.replace("/chats");
        } else if (result.workspaces) {
          setWorkspaces(result.workspaces);
          setStatus("workspaces");
        } else {
          setStatus("error");
        }
      });
    } else {
      // Request SSO token from parent (PostForge iframe)
      window.parent?.postMessage({ type: "crm_ready" }, window.location.origin);
    }

    return () => window.removeEventListener("message", handleSsoMessage);
  }, [router]);

  const handleSelectWorkspace = async (orgId: string) => {
    setSelecting(true);
    const ok = await tgSelectWorkspace(orgId);
    if (ok) {
      router.replace("/chats");
    } else {
      setStatus("error");
    }
    setSelecting(false);
  };

  const roleLabel: Record<string, string> = {
    super_admin: "Админ",
    admin: "Админ",
    operator: "Оператор",
  };

  return (
    <div className="flex items-center justify-center h-screen">
      <div className="w-full max-w-sm p-8 bg-gradient-to-b from-surface-card to-surface rounded-2xl border border-surface-border shadow-[0_0_40px_rgba(14,165,233,0.06)] animate-fade-in text-center">
        <div className="flex flex-col items-center gap-2 mb-4">
          <div className="w-12 h-12 bg-brand/90 rounded-2xl flex items-center justify-center shadow-lg shadow-brand/20">
            <img src="/metra-icon.png" alt="" className="w-7 h-7 object-contain" />
          </div>
          <div className="flex flex-col items-center">
            <img src="/metra-wordmark.png" alt="METRA" className="h-5 object-contain brightness-0 invert" />
            <span className="text-xs text-brand font-semibold tracking-widest mt-1">CRM</span>
          </div>
        </div>

        {status === "loading" && (
          <div className="flex flex-col items-center gap-3">
            <div className="w-8 h-8 border-2 border-brand/30 border-t-brand rounded-full animate-spin" />
            <p className="text-sm text-slate-400">Подключение...</p>
          </div>
        )}

        {status === "workspaces" && (
          <div className="space-y-3">
            <p className="text-sm text-slate-400 mb-4">Выберите пространство</p>
            {workspaces.map((ws) => (
              <button
                key={ws.org_id}
                onClick={() => handleSelectWorkspace(ws.org_id)}
                disabled={selecting}
                className="w-full flex items-center justify-between px-4 py-3 rounded-xl border border-surface-border bg-surface hover:bg-surface-hover hover:border-brand/30 transition-all disabled:opacity-50"
              >
                <div className="flex items-center gap-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-sm ${
                    ws.org_id.startsWith("personal_")
                      ? "bg-slate-500/10 text-slate-400"
                      : "bg-brand/10 text-brand"
                  }`}>
                    {ws.org_id.startsWith("personal_") ? (
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
                    ) : (
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
                    )}
                  </div>
                  <div className="text-left">
                    <div className="text-sm font-medium text-white">{ws.name}</div>
                    <div className="text-[10px] text-slate-500">{roleLabel[ws.role] || ws.role}</div>
                  </div>
                </div>
                <svg className="w-4 h-4 text-slate-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="9 18 15 12 9 6"/></svg>
              </button>
            ))}
          </div>
        )}

        {status === "error" && (
          <div className="space-y-3">
            <p className="text-red-400 text-sm">
              Не удалось авторизоваться. Войдите через METRA AI.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  isTelegramWebApp, tgAuth, tgSelectWorkspace, ssoAuth,
  getTgWebApp, getTokens, saveTokens, clearAllCrmStorage, disconnectWS,
  api,
  type TgWorkspace,
} from "@/lib";

/**
 * Decode a JWT payload without verification (signature is checked server-side).
 * Returns the parsed object or null if the token is malformed. The server still
 * authoritatively validates everything — this is purely so the login page can
 * tell whether a locally-stored token belongs to the currently-logged-in
 * PostForge user before silently accepting it.
 */
function decodeJwtPayload(token: string): Record<string, any> | null {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const payload = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = payload + "===".slice((payload.length + 3) % 4);
    return JSON.parse(decodeURIComponent(escape(atob(padded))));
  } catch {
    return null;
  }
}

/**
 * Owner-check: confirms that the locally-stored CRM token belongs to the
 * currently-logged-in PostForge user. If the user shares a browser/profile
 * with a teammate, the previous flow happily reused whichever CRM token was
 * sitting in localStorage — the user ended up impersonating their teammate
 * and seeing the wrong contacts. This is the guard against that.
 *
 * Returns true if the token is OK to use as-is, false if it must be cleared
 * and re-issued via SSO.
 */
async function isLocalCrmTokenOwnedByCurrentPfUser(): Promise<boolean | null> {
  const pfToken = localStorage.getItem("access_token") || sessionStorage.getItem("access_token");
  if (!pfToken) {
    // No PostForge token in shared storage — we have no source of truth to
    // compare against, so we can't say either way. Caller will decide.
    return null;
  }
  const pfPayload = decodeJwtPayload(pfToken);
  const pfUserId = pfPayload?.sub ? String(pfPayload.sub) : null;
  if (!pfUserId) return null;

  let staff: any;
  try {
    staff = await api("/api/staff/me");
  } catch (err: any) {
    // 401 = CRM token is bad → definitely force re-SSO.
    // Network error / timeout = CRM backend might be down but the local
    // token could be perfectly valid. Returning null here lets the caller
    // proceed with the existing CRM session rather than locking the user
    // out when the backend has a brief hiccup.
    if (err?.status === 401 || err?.message?.includes("401")) return false;
    return null;
  }
  const staffPfUserId = staff?.postforge_user_id ? String(staff.postforge_user_id) : null;
  if (!staffPfUserId) return null;
  return staffPfUserId === pfUserId;
}

export default function LoginPage() {
  const router = useRouter();
  const [status, setStatus] = useState<"loading" | "error" | "workspaces">("loading");
  const [workspaces, setWorkspaces] = useState<TgWorkspace[]>([]);
  const [selecting, setSelecting] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let messageHandler: ((e: MessageEvent) => void) | null = null;

    const run = async () => {
      const params = new URLSearchParams(window.location.search);

      // Pre-authenticated tokens from hash fragment or query params
      const hashParams = window.location.hash ? new URLSearchParams(window.location.hash.substring(1)) : null;
      const accessToken = hashParams?.get("access_token") || params.get("access_token");
      const refreshToken = hashParams?.get("refresh_token") || params.get("refresh_token");
      if (accessToken && refreshToken) {
        // Clear old session completely before applying new SSO tokens
        disconnectWS();
        clearAllCrmStorage();
        const role = hashParams?.get("role") || params.get("role") || "operator";
        saveTokens({ access_token: accessToken, refresh_token: refreshToken, role });
        // Mark as coming from PostForge so AppShell shows "back to dashboard" instead of "logout"
        try { sessionStorage.setItem("crm_is_embedded", "1"); } catch {}
        const base = window.location.pathname.split("/login")[0] || "";
        window.location.href = base + "/chats/";
        return;
      }

      // Already authenticated — but if direct navigation (not iframe), re-SSO to sync workspace
      const isInIframe = window.self !== window.top;
      if (getTokens()?.access_token) {
        // OWNER CHECK: if a PostForge token is in shared localStorage, the locally-stored
        // CRM token MUST belong to the same user. Otherwise we have a stale token from a
        // different teammate (shared browser/profile) and we'd silently impersonate them.
        // This was the root cause of the "Maxim sees other operators' chats" incident.
        const ownership = await isLocalCrmTokenOwnedByCurrentPfUser();
        if (cancelled) return;
        if (ownership === false) {
          // CONFIRMED mismatch — CRM token belongs to a different PostForge user.
          // This is the critical security path: nuclear cleanup + force re-SSO.
          const pfToken = localStorage.getItem("access_token") || sessionStorage.getItem("access_token");
          clearAllCrmStorage();
          disconnectWS();
          if (pfToken) {
            const ok = await ssoAuth(pfToken);
            if (cancelled) return;
            if (ok) {
              const base = window.location.pathname.split("/login")[0] || "";
              window.location.href = base + "/chats/";
            } else {
              setStatus("error");
            }
          } else {
            setStatus("error");
          }
          return;
        }
        // ownership === true: verified, safe to proceed.
        // ownership === null: cannot verify (no PF token / network error / backend
        //   missing postforge_user_id). We let the user through because:
        //   1) Backend enforces org isolation on EVERY API call via _org_accounts_subq()
        //   2) Blocking here would lock users out during CRM backend hiccups
        //   3) sessionStorage is now cleaned on every logout, so stale account
        //      selection can't leak — the remaining risk is acceptable.

        if (!isInIframe) {
          // Direct access (crm.metra-ai.org): user already has a valid CRM token
          // (ownership check passed above). Just redirect to /chats.
          // Clear crm_selected_account to avoid loading a stale TG account from
          // a previous session/user. The chats page will re-validate from fetchTgStatus.
          try { sessionStorage.removeItem("crm_selected_account"); } catch {}
          const base = window.location.pathname.split("/login")[0] || "";
          window.location.href = base + "/chats/";
          return;
        }
      }

      // SSO token (standalone window open from PostForge)
      const ssoToken = params.get("sso_token");
      if (ssoToken) {
        const ok = await ssoAuth(ssoToken);
        if (cancelled) return;
        if (ok) router.replace("/chats");
        else setStatus("error");
        return;
      }

      // SSO: listen for PostForge token via postMessage (when embedded in iframe)
      messageHandler = (event: MessageEvent) => {
        const allowedOrigins = [window.location.origin, "https://metra-ai.org", "https://app.metra-ai.org"];
        if (!allowedOrigins.includes(event.origin)) return;
        if (event.data?.type === "postforge_sso" && event.data?.token) {
          ssoAuth(event.data.token).then((ok) => {
            if (cancelled) return;
            if (ok) router.replace("/chats");
            else setStatus("error");
          });
        }
      };
      window.addEventListener("message", messageHandler);

      // Telegram Mini App auth
      if (isTelegramWebApp()) {
        getTgWebApp()?.ready();
        getTgWebApp()?.expand();
        const forceSwitch = params.get("switch") === "1";
        const result = await tgAuth(forceSwitch);
        if (cancelled) return;
        if (result.ok && !forceSwitch) {
          router.replace("/chats");
        } else if (result.workspaces || forceSwitch) {
          if (result.workspaces) {
            setWorkspaces(result.workspaces);
          }
          setStatus("workspaces");
        } else {
          setStatus("error");
        }
      } else {
        const inIframe = window.self !== window.top;
        if (inIframe) {
          // In iframe — request SSO token from parent (PostForge)
          window.parent?.postMessage({ type: "crm_ready" }, window.location.origin);
        } else {
          // Direct navigation (not iframe) — use PostForge token from shared localStorage
          const pfToken = localStorage.getItem("access_token") || sessionStorage.getItem("access_token");
          if (pfToken) {
            const ok = await ssoAuth(pfToken);
            if (cancelled) return;
            if (ok) {
              const base = window.location.pathname.split("/login")[0] || "";
              window.location.href = base + "/chats/";
            } else {
              setStatus("error");
            }
          } else {
            setStatus("error");
          }
        }
      }

    };

    run();

    return () => {
      cancelled = true;
      if (messageHandler) window.removeEventListener("message", messageHandler);
    };
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
    assistant: "Помощник",
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
            <button
              onClick={() => { window.location.href = "https://metra-ai.org"; }}
              className="inline-flex items-center gap-2 px-4 py-2.5 bg-brand/10 border border-brand/20 text-brand text-sm font-medium rounded-xl hover:bg-brand/20 transition-all"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="15 18 9 12 15 6" />
              </svg>
              Обратно в дашборд
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

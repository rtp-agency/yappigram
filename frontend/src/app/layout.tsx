import type { Metadata, Viewport } from "next";
import Script from "next/script";
import "./globals.css";

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  userScalable: false,
  viewportFit: "cover",
  themeColor: "#0c1222",
};

export const metadata: Metadata = {
  title: "METRA CRM",
  description: "METRA CRM — Telegram CRM",
  manifest: "/manifest.json",
  appleWebApp: {
    capable: true,
    statusBarStyle: "black-translucent",
    title: "METRA CRM",
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru">
      <head>
        <Script src="https://telegram.org/js/telegram-web-app.js" strategy="beforeInteractive" />
        <script dangerouslySetInnerHTML={{ __html: `
          // Pre-React SSO handler: tokens can arrive via:
          // 1. postMessage from parent (PostForge iframe) — secure, no URL exposure
          // 2. URL hash (legacy fallback) — for direct access at crm.metra-ai.org
          (function() {
            try {
              // --- Legacy: URL hash/query params ---
              var src = window.location.hash.substring(1) || window.location.search.substring(1);
              if (src) {
                var p = new URLSearchParams(src);
                var at = p.get("access_token");
                var rt = p.get("refresh_token");
                if (at && rt) {
                  var role = p.get("role") || "operator";
                  localStorage.setItem("tokens", JSON.stringify({access_token: at, refresh_token: rt, role: role}));
                  // Clear tokens from URL immediately
                  history.replaceState(null, "", window.location.pathname);
                  var base = window.location.pathname.split("/login")[0] || "";
                  if (!base || base === "/") base = "";
                  window.location.replace(base + "/chats/");
                  return;
                }
              }

              // --- postMessage: listen for tokens from parent PostForge iframe ---
              if (window.parent !== window) {
                // Signal parent that we're ready to receive tokens
                window.parent.postMessage({ type: "postforge:ready" }, "*");

                window.addEventListener("message", function handler(e) {
                  try {
                    if (e.data && e.data.type === "postforge:auth" && e.data.access_token) {
                      localStorage.setItem("tokens", JSON.stringify({
                        access_token: e.data.access_token,
                        refresh_token: e.data.refresh_token,
                        role: e.data.role || "operator"
                      }));
                      window.removeEventListener("message", handler);
                      var base = window.location.pathname.split("/login")[0] || "";
                      if (!base || base === "/") base = "";
                      window.location.replace(base + "/chats/");
                    }
                  } catch(err) {}
                });
              }
            } catch(e) {}
          })();
        `}} />
      </head>
      <body className="overscroll-none">{children}</body>
    </html>
  );
}

// ============================================================
// API Client
// ============================================================

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface TokenPair {
  access_token: string;
  refresh_token: string;
  role: string;
}

export function getTokens(): TokenPair | null {
  if (typeof window === "undefined") return null;
  const raw = localStorage.getItem("tokens");
  return raw ? JSON.parse(raw) : null;
}

export function saveTokens(tokens: TokenPair) {
  localStorage.setItem("tokens", JSON.stringify(tokens));
}

export function clearTokens() {
  localStorage.removeItem("tokens");
}

export function getRole(): string | null {
  return getTokens()?.role || null;
}

async function refreshTokens(): Promise<string | null> {
  const tokens = getTokens();
  if (!tokens?.refresh_token) return null;

  const res = await fetch(`${API}/api/auth/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: tokens.refresh_token }),
  });

  if (!res.ok) {
    clearTokens();
    return null;
  }

  const data = await res.json();
  saveTokens(data);
  return data.access_token;
}

export async function api(path: string, options: RequestInit = {}): Promise<any> {
  const tokens = getTokens();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };

  if (tokens?.access_token) {
    headers["Authorization"] = `Bearer ${tokens.access_token}`;
  }

  let res = await fetch(`${API}${path}`, { ...options, headers });

  // Auto-refresh on 401
  if (res.status === 401 && tokens?.refresh_token) {
    const newToken = await refreshTokens();
    if (newToken) {
      headers["Authorization"] = `Bearer ${newToken}`;
      res = await fetch(`${API}${path}`, { ...options, headers });
    }
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "API Error");
  }

  if (res.status === 204) return null;
  return res.json();
}

// ============================================================
// WebSocket
// ============================================================

type WSHandler = (event: any) => void;

let _ws: WebSocket | null = null;
let _handlers: WSHandler[] = [];

export function connectWS() {
  const tokens = getTokens();
  if (!tokens?.access_token) return;
  // Don't create if already open or connecting
  if (_ws && (_ws.readyState === WebSocket.OPEN || _ws.readyState === WebSocket.CONNECTING)) return;
  _ws = null;

  const wsUrl = API.replace("http", "ws");
  _ws = new WebSocket(`${wsUrl}/ws?token=${tokens.access_token}`);

  _ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    _handlers.forEach((h) => h(data));
  };

  _ws.onerror = () => {
    // Error fires before close — just let onclose handle reconnect
  };

  _ws.onclose = (e) => {
    _ws = null;
    // Always refresh token before reconnecting — prevents stale token loops
    refreshTokens()
      .then(() => setTimeout(connectWS, 1000))
      .catch(() => setTimeout(connectWS, 5000));
  };
}

export function onWSEvent(handler: WSHandler): () => void {
  _handlers.push(handler);
  return () => {
    _handlers = _handlers.filter((h) => h !== handler);
  };
}

export function disconnectWS() {
  _ws?.close();
  _ws = null;
  _handlers = [];
}

// ============================================================
// Telegram Mini App
// ============================================================

declare global {
  interface Window {
    Telegram?: {
      WebApp: {
        initData: string;
        initDataUnsafe: any;
        ready: () => void;
        expand: () => void;
        close: () => void;
        BackButton: {
          show: () => void;
          hide: () => void;
          onClick: (cb: () => void) => void;
          offClick: (cb: () => void) => void;
        };
        MainButton: {
          show: () => void;
          hide: () => void;
          setText: (text: string) => void;
          onClick: (cb: () => void) => void;
        };
        colorScheme: "light" | "dark";
        themeParams: Record<string, string>;
      };
    };
  }
}

export function isTelegramWebApp(): boolean {
  if (typeof window === "undefined") return false;
  return !!(window.Telegram?.WebApp?.initData);
}

export function getTgInitData(): string | null {
  if (typeof window === "undefined") return null;
  return window.Telegram?.WebApp?.initData || null;
}

export function getTgWebApp() {
  return typeof window !== "undefined" ? window.Telegram?.WebApp : undefined;
}

export async function tgAuth(): Promise<boolean> {
  const initData = getTgInitData();
  if (!initData) return false;

  try {
    const res = await fetch(`${API}/api/auth/tg`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ init_data: initData }),
    });

    if (!res.ok) return false;

    const data = await res.json();
    saveTokens(data);
    return true;
  } catch {
    return false;
  }
}

export async function createGroup(title: string, tgAccountId: string, memberContactIds: string[] = []) {
  return api("/api/contacts/create-group", {
    method: "POST",
    body: JSON.stringify({ title, tg_account_id: tgAccountId, member_contact_ids: memberContactIds }),
  });
}

// ============================================================
// Types
// ============================================================

export interface Contact {
  id: string;
  alias: string;
  status: string;
  chat_type: string;
  is_forum: boolean;
  is_archived: boolean;
  tags: string[];
  notes: string | null;
  assigned_to: string | null;
  tg_account_id: string | null;
  created_at: string;
  approved_at: string | null;
  last_message_at: string | null;
}

export interface Message {
  id: string;
  contact_id: string;
  direction: string;
  content: string | null;
  media_type: string | null;
  media_path: string | null;
  sent_by: string | null;
  is_read: boolean;
  is_deleted: boolean;
  is_edited: boolean;
  reply_to_msg_id: string | null;
  reply_to_content_preview: string | null;
  forwarded_from_alias: string | null;
  sender_alias: string | null;
  inline_buttons: string | null;
  topic_id: number | null;
  topic_name: string | null;
  created_at: string;
}

export interface InlineButton {
  text: string;
  callback_data?: string;
  url?: string;
}

export function parseInlineButtons(json: string | null): InlineButton[][] {
  if (!json) return [];
  try { return JSON.parse(json); } catch { return []; }
}

export function mediaUrl(media_path: string): string {
  return `${API}/media/${media_path}`;
}

export async function uploadMedia(contactId: string, file: File, caption?: string): Promise<any> {
  const tokens = getTokens();
  const formData = new FormData();
  formData.append("file", file);

  const url = new URL(`${API}/api/messages/${contactId}/send-media`);
  if (caption) url.searchParams.set("caption", caption);

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${tokens?.access_token}`,
    },
    body: formData,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Upload failed");
  }

  return res.json();
}

export interface StaffMember {
  id: string;
  tg_user_id: number;
  tg_username: string | null;
  role: string;
  name: string;
  is_active: boolean;
  created_at: string;
}

export interface Tag {
  id: string;
  name: string;
  color: string;
}

export interface TgAccount {
  id: string;
  phone: string;
  is_active: boolean;
  connected_at: string;
}

export async function forwardMessages(fromContactId: string, messageIds: string[], toContactId: string) {
  return api(`/api/messages/${fromContactId}/forward`, {
    method: "POST",
    body: JSON.stringify({ message_ids: messageIds, to_contact_id: toContactId }),
  });
}

export async function pressInlineButton(contactId: string, messageId: string, callbackData: string) {
  return api(`/api/messages/${contactId}/press-button`, {
    method: "POST",
    body: JSON.stringify({ message_id: messageId, callback_data: callbackData }),
  });
}

export async function archiveChat(contactId: string) {
  return api(`/api/contacts/${contactId}/archive`, { method: "POST" });
}

export async function unarchiveChat(contactId: string) {
  return api(`/api/contacts/${contactId}/unarchive`, { method: "POST" });
}

export async function translateText(text: string, targetLang: string): Promise<string> {
  const res = await api("/api/translate", {
    method: "POST",
    body: JSON.stringify({ text, target_lang: targetLang }),
  });
  return res.translated;
}

export async function editMessageInTg(contactId: string, messageId: string, content: string) {
  return api(`/api/messages/${contactId}/edit`, {
    method: "POST",
    body: JSON.stringify({ message_id: messageId, content }),
  });
}

export async function deleteMessageInTg(contactId: string, messageId: string) {
  return api(`/api/messages/${contactId}/delete`, {
    method: "POST",
    body: JSON.stringify({ message_id: messageId }),
  });
}

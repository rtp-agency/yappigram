// ============================================================
// API Client
// ============================================================

const API = process.env.NEXT_PUBLIC_API_URL || "";

interface TokenPair {
  access_token: string;
  refresh_token: string;
  role: string;
}

export function getTokens(): TokenPair | null {
  if (typeof window === "undefined") return null;
  const raw = localStorage.getItem("tokens");
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    clearTokens();
    return null;
  }
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

let _refreshPromise: Promise<string | null> | null = null;

export async function api(path: string, options: RequestInit = {}): Promise<any> {
  const tokens = getTokens();
  const isFormData = options.body instanceof FormData;
  const headers: Record<string, string> = {
    ...(isFormData ? {} : { "Content-Type": "application/json" }),
    ...(options.headers as Record<string, string>),
  };

  if (tokens?.access_token) {
    headers["Authorization"] = `Bearer ${tokens.access_token}`;
  }

  let res = await fetch(`${API}${path}`, { ...options, headers });

  // Auto-refresh on 401
  if (res.status === 401 && tokens?.refresh_token) {
    if (!_refreshPromise) {
      _refreshPromise = refreshTokens().finally(() => { _refreshPromise = null; });
    }
    const newToken = await _refreshPromise;
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
let _wsRetries = 0;
const WS_MAX_RETRIES = 10;

export async function connectWS() {
  if (_ws) return;

  // Get fresh token — refresh if needed
  let tokens = getTokens();
  if (!tokens?.access_token) return;

  // Try a quick validation — if token might be expired, refresh first
  if (_wsRetries > 0 && tokens.refresh_token) {
    const fresh = await refreshTokens();
    if (fresh) {
      tokens = getTokens();
    } else {
      return; // Can't refresh — stop retrying
    }
  }

  if (!tokens?.access_token) return;

  const wsBase = API
    ? API.replace("http", "ws")
    : `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}`;
  const pathBase = typeof window !== "undefined" ? (window.location.pathname.match(/^\/[^/]+/)?.[0] || "") : "";
  _ws = new WebSocket(`${wsBase}${pathBase}/ws?token=${tokens.access_token}`);

  _ws.onopen = () => {
    _wsRetries = 0;
  };

  _ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    _handlers.forEach((h) => h(data));
  };

  _ws.onclose = () => {
    _ws = null;
    if (_wsRetries < WS_MAX_RETRIES) {
      const delay = Math.min(3000 * Math.pow(1.5, _wsRetries), 30000);
      _wsRetries++;
      setTimeout(() => connectWS(), delay);
    }
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

export async function ssoAuth(postforgeToken: string): Promise<boolean> {
  try {
    const res = await fetch(`${API}/api/auth/sso`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ postforge_token: postforgeToken }),
    });
    if (!res.ok) return false;
    const data = await res.json();
    saveTokens(data);
    return true;
  } catch {
    return false;
  }
}

export interface TgWorkspace {
  org_id: string;
  name: string;
  role: string;
}

export type TgAuthResult =
  | { ok: true; workspaces?: undefined }
  | { ok: false; workspaces?: undefined }
  | { ok: false; workspaces: TgWorkspace[] };

export async function tgAuth(forceSelect: boolean = false): Promise<TgAuthResult> {
  const initData = getTgInitData();
  if (!initData) return { ok: false };

  try {
    const res = await fetch(`${API}/api/auth/tg`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ init_data: initData, force_select: forceSelect }),
    });

    if (!res.ok) return { ok: false };

    const data = await res.json();

    // Multi-workspace: backend returned workspace list instead of tokens
    if (data.workspaces) {
      return { ok: false, workspaces: data.workspaces };
    }

    saveTokens(data);
    return { ok: true };
  } catch {
    return { ok: false };
  }
}

export async function tgSelectWorkspace(orgId: string): Promise<boolean> {
  const initData = getTgInitData();
  if (!initData) return false;

  try {
    const res = await fetch(`${API}/api/auth/tg/select`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ init_data: initData, org_id: orgId }),
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
  tags: string[];
  notes: string | null;
  assigned_to: string | null;
  tg_account_id: string | null;
  real_tg_id: number | null;
  is_archived: boolean;
  created_at: string;
  approved_at: string | null;
  last_message_at: string | null;
  last_message_content: string | null;
}

export interface Message {
  id: string;
  contact_id: string;
  tg_message_id: number | null;
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
  send_text?: string;
}

export function parseInlineButtons(json: string | null): InlineButton[][] {
  if (!json) return [];
  try {
    const parsed = JSON.parse(json);
    if (parsed.hide_keyboard) return [];
    return Array.isArray(parsed) ? parsed : [];
  } catch { return []; }
}

export function isKeyboardHidden(json: string | null): boolean {
  if (!json) return false;
  try {
    const parsed = JSON.parse(json);
    return parsed.hide_keyboard === true;
  } catch { return false; }
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
  tg_account_id: string | null;
}

export async function deleteTag(id: string) {
  return api(`/api/tags/${id}`, { method: "DELETE" });
}

export async function fetchTags(tgAccountId?: string): Promise<Tag[]> {
  const params = tgAccountId ? `?tg_account_id=${tgAccountId}` : "";
  return api(`/api/tags${params}`);
}

export async function createTag(data: { name: string; color: string; tg_account_id?: string }): Promise<Tag> {
  return api("/api/tags", { method: "POST", body: JSON.stringify(data) });
}

export async function fetchContacts(status?: string, tgAccountId?: string): Promise<Contact[]> {
  const params = new URLSearchParams();
  if (status) params.set("status", status);
  if (tgAccountId) params.set("tg_account_id", tgAccountId);
  return api(`/api/contacts?${params.toString()}`);
}

export async function fetchUnread(tgAccountId?: string): Promise<Record<string, number>> {
  const params = tgAccountId ? `?tg_account_id=${tgAccountId}` : "";
  return api(`/api/unread${params}`);
}

export async function fetchTemplates(tgAccountId?: string): Promise<Template[]> {
  const params = tgAccountId ? `?tg_account_id=${tgAccountId}` : "";
  return api(`/api/templates${params}`);
}

export interface EditHistoryEntry {
  old_content: string | null;
  new_content: string | null;
  edited_at: string;
}

export async function fetchEditHistory(contactId: string, messageId: string): Promise<EditHistoryEntry[]> {
  return api(`/api/messages/${contactId}/${messageId}/edit-history`);
}

export interface TgStatusAccount {
  id: string;
  phone: string;
  display_name: string | null;
  is_active: boolean;
  connected: boolean;
  show_real_names: boolean;
}

export async function fetchTgStatus(): Promise<TgStatusAccount[]> {
  const res = await api("/api/tg/status");
  return Array.isArray(res) ? res : (res.accounts || []);
}

export interface TgAccount {
  id: string;
  phone: string;
  is_active: boolean;
  connected_at: string;
}

export async function forwardMessages(fromContactId: string, messageIds: string[], toContactId: string, mediaOnly: boolean = false) {
  return api(`/api/messages/${fromContactId}/forward`, {
    method: "POST",
    body: JSON.stringify({ message_ids: messageIds, to_contact_id: toContactId, media_only: mediaOnly }),
  });
}

export async function pressInlineButton(contactId: string, messageId: string, callbackData: string) {
  return api(`/api/messages/${contactId}/press-button`, {
    method: "POST",
    body: JSON.stringify({ message_id: messageId, callback_data: callbackData }),
  });
}

// ============================================================
// Templates
// ============================================================

export interface Template {
  id: string;
  title: string;
  content: string;
  category: string | null;
  shortcut: string | null;
  media_path: string | null;
  media_type: string | null;
  tg_account_id: string | null;
  created_by: string | null;
  created_by_name: string | null;
  created_at: string;
}

export async function getTemplates(): Promise<Template[]> {
  return api("/api/templates");
}

export async function createTemplate(data: { title: string; content: string; category?: string; shortcut?: string; tg_account_id?: string }) {
  return api("/api/templates", { method: "POST", body: JSON.stringify(data) });
}

export async function updateTemplate(id: string, data: Partial<Template>) {
  return api(`/api/templates/${id}`, { method: "PATCH", body: JSON.stringify(data) });
}

export async function deleteTemplate(id: string) {
  return api(`/api/templates/${id}`, { method: "DELETE" });
}

// ============================================================
// Archive
// ============================================================

export async function archiveContact(contactId: string) {
  return api(`/api/contacts/${contactId}/archive`, { method: "POST" });
}

export async function unarchiveContact(contactId: string) {
  return api(`/api/contacts/${contactId}/unarchive`, { method: "POST" });
}

// ============================================================
// Avatars
// ============================================================

export function avatarUrl(contactId: string): string {
  const tokens = getTokens();
  return `${API}/api/contacts/${contactId}/avatar?token=${tokens?.access_token || ""}`;
}

// ============================================================
// Message editing
// ============================================================

export async function editMessage(contactId: string, messageId: string, content: string) {
  return api(`/api/messages/${contactId}/${messageId}/edit`, {
    method: "PATCH",
    body: JSON.stringify({ content }),
  });
}

// ============================================================
// Translation
// ============================================================

export async function translateText(text: string, targetLang: string = "en"): Promise<{ translated: string; detected_lang: string }> {
  return api("/api/translate", {
    method: "POST",
    body: JSON.stringify({ text, target_lang: targetLang }),
  });
}

// ============================================================
// Broadcasts
// ============================================================

export interface Broadcast {
  id: string;
  title: string;
  content: string | null;
  media_path: string | null;
  media_type: string | null;
  tg_account_id: string;
  tag_filter: string[];
  max_recipients: number | null;
  contact_ids: string[];
  delay_seconds: number;
  status: string;
  total_recipients: number;
  sent_count: number;
  failed_count: number;
  created_by: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export async function getBroadcasts(): Promise<Broadcast[]> {
  return api("/api/broadcasts");
}

export async function createBroadcast(data: {
  title: string;
  content?: string;
  tg_account_id: string;
  tag_filter?: string[];
  delay_seconds?: number;
  max_recipients?: number;
  contact_ids?: string[];
}) {
  return api("/api/broadcasts", { method: "POST", body: JSON.stringify(data) });
}

export async function startBroadcast(id: string) {
  return api(`/api/broadcasts/${id}/start`, { method: "POST" });
}

export async function pauseBroadcast(id: string) {
  return api(`/api/broadcasts/${id}/pause`, { method: "POST" });
}

export async function cancelBroadcast(id: string) {
  return api(`/api/broadcasts/${id}/cancel`, { method: "POST" });
}

// ============================================================
// Sync dialogs
// ============================================================

export async function syncDialogs(accountId: string) {
  return api(`/api/tg/${accountId}/sync-dialogs`, { method: "POST" });
}

// ============================================================
// Staff Timezone
// ============================================================

export async function updateTimezone(timezone: string) {
  return api(`/api/staff/me/timezone?timezone=${encodeURIComponent(timezone)}`, { method: "PATCH" });
}

// ============================================================
// Reports
// ============================================================

export interface NewChatsReport {
  total: number;
  by_day: { date: string; count: number }[];
  by_account: { account_id: string; phone: string; display_name: string | null; count: number }[];
}

export async function fetchNewChatsReport(
  fromDate: string,
  toDate: string,
  tgAccountId?: string,
  timezone?: string,
): Promise<NewChatsReport> {
  const params = new URLSearchParams({ from_date: fromDate, to_date: toDate });
  if (tgAccountId) params.set("tg_account_id", tgAccountId);
  if (timezone) params.set("timezone", timezone);
  return api(`/api/reports/new-chats?${params.toString()}`);
}


"use client";

import { useEffect, useState } from "react";
import { api, clearTokens, disconnectWS, getTemplates, createTemplate, updateTemplate, deleteTemplate, deleteTag, getRole, fetchTgStatus, createTag, updateTimezone, fetchNewChatsReport } from "@/lib";
import type { Template, TgStatusAccount, NewChatsReport } from "@/lib";
import { AppShell, AuthGuard, Button, Input } from "@/components";
import { useRouter } from "next/navigation";

const isTelegramWebApp = () => typeof window !== "undefined" && !!(window as any).Telegram?.WebApp?.initData;

const isAdminRole = () => ["super_admin", "admin"].includes(getRole() || "");

export default function SettingsPage() {
  return (
    <AuthGuard>
      <AppShell>
        <SettingsContent />
      </AppShell>
    </AuthGuard>
  );
}

function SettingsContent() {
  const [userRole, setUserRole] = useState<string>(getRole() || "operator");

  useEffect(() => {
    api("/api/staff/me").then((me: any) => { if (me?.role) setUserRole(me.role); }).catch(() => {});
  }, []);

  const isAdmin = ["super_admin", "admin"].includes(userRole);

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-8">
      <h1 className="text-2xl font-bold bg-gradient-to-r from-brand to-accent bg-clip-text text-transparent">Настройки</h1>

      {isTelegramWebApp() && <WorkspaceSection />}
      {isAdmin && <TelegramSection />}
      <TimezoneSection />
      {isAdmin && <AdminSettingsSection />}
      <TagsSection />
      <TemplatesSection isAdmin={isAdmin} />
    </div>
  );
}

function WorkspaceSection() {
  const router = useRouter();
  const [wsName, setWsName] = useState("Команда");

  useEffect(() => {
    api("/api/staff/me")
      .then((data: any) => {
        if (data?.postforge_org_id?.startsWith("personal_")) {
          setWsName("Личное пространство");
        } else if (data?.postforge_org_id) {
          setWsName("Команда");
        }
      })
      .catch(() => {});
  }, []);

  return (
    <div>
      <h2 className="text-lg font-semibold flex items-center gap-2 mb-3">
        <svg className="w-5 h-5 text-brand" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        Пространство
      </h2>
      <div className="bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-4">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-sm font-medium text-white">{wsName || "..."}</div>
            <div className="text-xs text-slate-500 mt-0.5">Текущее рабочее пространство CRM</div>
          </div>
          <Button variant="ghost" onClick={() => { clearTokens(); disconnectWS(); router.replace("/login?switch=1"); }}>
            Сменить
          </Button>
        </div>
      </div>
    </div>
  );
}

function TelegramSection() {
  const [accounts, setAccounts] = useState<any[]>([]);
  const [phone, setPhone] = useState("");
  const [code, setCode] = useState("");
  const [password2fa, setPassword2fa] = useState("");
  const [step, setStep] = useState<"idle" | "code_sent">("idle");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api("/api/tg/status").then((res: any) => {
      // Support both old format (array) and new format ({ accounts: [...] })
      const accs = Array.isArray(res) ? res : (res.accounts || []);
      setAccounts(accs.filter((a: any) => a.is_active));
    }).catch(console.error);
  }, []);

  const connect = async () => {
    setLoading(true);
    try {
      await api("/api/tg/connect", { method: "POST", body: JSON.stringify({ phone }) });
      setStep("code_sent");
    } catch (e: any) { alert(e.message); } finally { setLoading(false); }
  };

  const verify = async () => {
    setLoading(true);
    try {
      const account = await api("/api/tg/verify", {
        method: "POST",
        body: JSON.stringify({ phone, code, password_2fa: password2fa || null }),
      });
      setAccounts((prev) => [...prev, account]);
      setStep("idle");
      setPhone(""); setCode(""); setPassword2fa("");
    } catch (e: any) { alert(e.message); } finally { setLoading(false); }
  };

  const [confirmDisconnect, setConfirmDisconnect] = useState<string | null>(null);
  const disconnect = async (id: string) => {
    try {
      await api(`/api/tg/disconnect/${id}`, { method: "DELETE" });
      setAccounts((prev) => prev.filter((a) => a.id !== id));
      setConfirmDisconnect(null);
    } catch (e: any) { alert(e.message); }
  };


  return (
    <section className="animate-fade-in">
      <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <svg className="w-5 h-5 text-brand" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z" />
        </svg>
        Telegram аккаунты
      </h2>

      {accounts.map((acc) => (
        <div key={acc.id} className="bg-gradient-to-r from-surface-card to-surface border border-surface-border rounded-xl p-4 mb-2">
          <div className="flex items-center gap-3">
            <div className={`w-2 h-2 rounded-full flex-shrink-0 ${acc.is_active ? "bg-emerald-400" : "bg-red-400"}`} />
            <span className="font-medium text-sm">{acc.display_name || acc.phone}</span>
            <span className={`text-xs ${acc.is_active ? "text-emerald-400/70" : "text-red-400/70"}`}>
              {acc.is_active ? "Активен" : "Отключён"}
            </span>
          </div>
          {acc.is_active && (
            <div className="flex gap-2 mt-3 flex-wrap">
              {confirmDisconnect === acc.id ? (
                <div className="flex gap-1">
                  <Button variant="danger" onClick={() => disconnect(acc.id)}>Да, отключить</Button>
                  <Button variant="ghost" onClick={() => setConfirmDisconnect(null)}>Отмена</Button>
                </div>
              ) : (
                <Button variant="danger" onClick={() => setConfirmDisconnect(acc.id)}>Отключить</Button>
              )}
            </div>
          )}
        </div>
      ))}

      <div className="mt-4 bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-5 space-y-3">
        {step === "idle" ? (
          <>
            <Input label="Номер телефона" value={phone} onChange={setPhone} placeholder="+79001234567" />
            <Button onClick={connect} disabled={loading || !phone}>
              {loading ? "Отправка кода..." : "Подключить аккаунт"}
            </Button>
          </>
        ) : (
          <>
            <Input label="Код из Telegram" value={code} onChange={setCode} placeholder="12345" />
            <Input label="2FA пароль (если включен)" type="password" value={password2fa} onChange={setPassword2fa} />
            <Button onClick={verify} disabled={loading || !code}>
              {loading ? "Проверка..." : "Подтвердить"}
            </Button>
          </>
        )}
      </div>
    </section>
  );
}

function TagsSection() {
  const [tags, setTags] = useState<any[]>([]);
  const [accounts, setAccounts] = useState<{ id: string; phone: string; display_name: string | null }[]>([]);
  const [name, setName] = useState("");
  const [color, setColor] = useState("#0ea5e9");
  const [tagAccount, setTagAccount] = useState("");

  useEffect(() => {
    api("/api/tags").then(setTags).catch(console.error);
    fetchTgStatus().then((accs) => {
      setAccounts(accs.filter((a) => a.is_active !== false).map((a) => ({ id: a.id, phone: a.phone, display_name: a.display_name })));
    }).catch(() => {});
  }, []);

  const handleCreate = async () => {
    if (!name.trim()) return;
    try {
      const tag = await createTag({
        name: name.trim(),
        color,
        tg_account_id: tagAccount || undefined,
      });
      setTags((prev) => [...prev, tag]);
      setName("");
      setTagAccount("");
    } catch (e: any) { alert(e.message); }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteTag(id);
      setTags((prev) => prev.filter((t) => t.id !== id));
    } catch (e: any) { alert(e.message); }
  };

  return (
    <section className="animate-fade-in">
      <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <svg className="w-5 h-5 text-accent" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z" />
          <line x1="7" y1="7" x2="7.01" y2="7" />
        </svg>
        Теги
      </h2>
      <div className="flex overflow-x-auto gap-2 mb-4 pb-1" style={{ scrollbarWidth: "thin" }}>
        {tags.map((t) => (
          <span
            key={t.id}
            className="group relative px-3 py-1.5 rounded-full text-sm font-medium border animate-fade-in whitespace-nowrap shrink-0"
            style={{ backgroundColor: t.color + "15", color: t.color, borderColor: t.color + "30" }}
          >
            {t.name}
            {t.tg_account_id && (
              <span className="ml-1 text-[10px] opacity-60">
                ({(() => { const a = accounts.find((a) => a.id === t.tg_account_id); return a?.display_name || a?.phone || "—"; })()})
              </span>
            )}
            <button
              onClick={() => handleDelete(t.id)}
              className="ml-1.5 inline-flex items-center justify-center w-4 h-4 rounded-full opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-500/20"
              style={{ color: "inherit" }}
            >
              <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
              </svg>
            </button>
          </span>
        ))}
        {tags.length === 0 && <span className="text-sm text-slate-500">Тегов пока нет</span>}
      </div>
      <div className="flex gap-2 items-end flex-wrap">
        <Input label="Название тега" value={name} onChange={setName} placeholder="VIP" />
        <div className="flex flex-col gap-1.5">
          <label className="text-sm text-slate-400 font-medium">Цвет</label>
          <input
            type="color"
            value={color}
            onChange={(e) => setColor(e.target.value)}
            className="w-10 h-10 rounded-xl cursor-pointer bg-transparent border border-surface-border"
          />
        </div>
        {accounts.length > 0 && (
          <div className="flex flex-col gap-1.5">
            <label className="text-sm text-slate-400 font-medium">Аккаунт</label>
            <select
              value={tagAccount}
              onChange={(e) => setTagAccount(e.target.value)}
              className="bg-surface-card border border-surface-border rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-brand/50 transition-all duration-200 text-slate-300"
            >
              <option value="">Общий (все)</option>
              {accounts.map((acc) => (
                <option key={acc.id} value={acc.id}>{acc.display_name || acc.phone}</option>
              ))}
            </select>
          </div>
        )}
        <Button onClick={handleCreate}>Добавить</Button>
      </div>
    </section>
  );
}

function TemplatesSection({ isAdmin }: { isAdmin: boolean }) {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [accounts, setAccounts] = useState<{ id: string; phone: string; display_name: string | null }[]>([]);
  const [filterAccount, setFilterAccount] = useState<string | "all">("all");
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [category, setCategory] = useState("");
  const [shortcut, setShortcut] = useState("");
  const [assignAccount, setAssignAccount] = useState("");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [sendAs, setSendAs] = useState("auto");

  useEffect(() => {
    getTemplates().then(setTemplates).catch(console.error);
    api("/api/tg/status").then((res: any) => {
      const accs = Array.isArray(res) ? res : (res.accounts || []);
      setAccounts(accs.map((a: any) => ({ id: a.id, phone: a.phone, display_name: a.display_name || null })));
    }).catch(() => {});
  }, []);

  const filteredTemplates = filterAccount === "all"
    ? templates
    : templates.filter((t) => t.tg_account_id === filterAccount);

  const [creating, setCreating] = useState(false);
  const handleCreate = async () => {
    if (!title.trim() || !content.trim() || creating) return;
    setCreating(true);
    try {
      let tpl = await createTemplate({
        title: title.trim(),
        content: content.trim(),
        category: category.trim() || undefined,
        shortcut: shortcut.trim() || undefined,
        tg_account_id: assignAccount || undefined,
      });
      // Upload media if selected
      if (mediaFile) {
        const formData = new FormData();
        formData.append("file", mediaFile);
        const mediaResult = await api(`/api/templates/${tpl.id}/upload-media?send_as=${sendAs}`, {
          method: "POST",
          body: formData,
          headers: {},
        });
        tpl = { ...tpl, media_path: mediaResult.media_path, media_type: mediaResult.media_type };
      }
      setTemplates((prev) => [...prev, tpl]);
      setTitle(""); setContent(""); setCategory(""); setShortcut("");
      setMediaFile(null); setSendAs("auto");
    } catch (e: any) { alert(e.message); } finally { setCreating(false); }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteTemplate(id);
      setTemplates((prev) => prev.filter((t) => t.id !== id));
    } catch (e: any) { alert(e.message); }
  };

  // Edit template state
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editTitle, setEditTitle] = useState("");
  const [editContent, setEditContent] = useState("");
  const [editCategory, setEditCategory] = useState("");
  const [editShortcut, setEditShortcut] = useState("");
  const [editSaving, setEditSaving] = useState(false);

  const startEdit = (tpl: Template) => {
    setEditingId(tpl.id);
    setEditTitle(tpl.title);
    setEditContent(tpl.content);
    setEditCategory(tpl.category || "");
    setEditShortcut(tpl.shortcut || "");
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditTitle(""); setEditContent(""); setEditCategory(""); setEditShortcut("");
  };

  const handleEditSave = async () => {
    if (!editingId || !editTitle.trim() || !editContent.trim() || editSaving) return;
    setEditSaving(true);
    try {
      const updated = await updateTemplate(editingId, {
        title: editTitle.trim(),
        content: editContent.trim(),
        category: editCategory.trim() || null,
        shortcut: editShortcut.trim() || null,
      });
      setTemplates((prev) => prev.map((t) => t.id === editingId ? { ...t, ...updated } : t));
      cancelEdit();
    } catch (e: any) { alert(e.message); } finally { setEditSaving(false); }
  };

  return (
    <section className="animate-fade-in">
      <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <svg className="w-5 h-5 text-purple-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
        </svg>
        Шаблоны ответов
      </h2>

      {/* Filter by account — only for admins */}
      {isAdmin && accounts.length > 1 && (
        <div className="flex gap-1 mb-4 bg-surface border border-surface-border rounded-xl p-1 w-fit flex-wrap">
          <button
            onClick={() => setFilterAccount("all")}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
              filterAccount === "all" ? "bg-brand/15 text-brand" : "text-slate-400 hover:text-slate-300"
            }`}
          >
            Все
          </button>
          {accounts.map((acc) => (
            <button
              key={acc.id}
              onClick={() => setFilterAccount(acc.id)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                filterAccount === acc.id ? "bg-brand/15 text-brand" : "text-slate-400 hover:text-slate-300"
              }`}
            >
              {acc.display_name || acc.phone}
            </button>
          ))}
        </div>
      )}

      <div className="space-y-2 mb-4">
        {filteredTemplates.map((tpl) => (
          editingId === tpl.id ? (
            <div key={tpl.id} className="bg-gradient-to-br from-surface-card to-surface border border-brand/30 rounded-xl p-4 space-y-3 animate-fade-in">
              <div className="grid grid-cols-2 gap-3">
                <Input label="Название" value={editTitle} onChange={setEditTitle} placeholder="Приветствие" />
                <Input label="Категория" value={editCategory} onChange={setEditCategory} placeholder="Общие" />
              </div>
              <div>
                <label className="text-sm text-slate-400 font-medium block mb-1.5">Текст шаблона</label>
                <textarea
                  value={editContent}
                  onChange={(e) => setEditContent(e.target.value)}
                  rows={3}
                  className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 resize-none"
                />
              </div>
              <Input label="Шорткат" value={editShortcut} onChange={setEditShortcut} placeholder="/hello" />
              <div className="flex gap-2">
                <Button onClick={handleEditSave} disabled={!editTitle.trim() || !editContent.trim() || editSaving}>
                  {editSaving ? "Сохранение..." : "Сохранить"}
                </Button>
                <Button variant="ghost" onClick={cancelEdit}>Отмена</Button>
              </div>
            </div>
          ) : (
          <div key={tpl.id} className="bg-surface-card border border-surface-border rounded-xl p-3 flex items-start justify-between gap-3 animate-fade-in">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2 mb-1">
                <span className="font-medium text-sm text-brand">{tpl.title}</span>
                {tpl.media_type && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-brand/10 text-brand">
                    {tpl.media_type === "photo" ? "📷" : tpl.media_type === "video" ? "🎬" : tpl.media_type === "video_note" ? "🔵" : tpl.media_type === "voice" ? "🎤" : "📄"}
                  </span>
                )}
                {tpl.category && <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-slate-400">{tpl.category}</span>}
                {tpl.shortcut && <span className="text-[10px] text-slate-500 font-mono">{tpl.shortcut}</span>}
                {tpl.tg_account_id && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400">
                    {(() => { const a = accounts.find((a) => a.id === tpl.tg_account_id); return a?.display_name || a?.phone || "—"; })()}
                  </span>
                )}
                {tpl.created_by_name && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-slate-500">{tpl.created_by_name}</span>
                )}
              </div>
              <p className="text-xs text-slate-400 break-words">{tpl.content.slice(0, 150)}{tpl.content.length > 150 ? "..." : ""}</p>
            </div>
            {isAdmin && (
              <div className="flex items-center gap-1 shrink-0">
                <button onClick={() => startEdit(tpl)} className="text-slate-600 hover:text-brand transition-colors p-1">
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
                  </svg>
                </button>
                <button onClick={() => handleDelete(tpl.id)} className="text-slate-600 hover:text-red-400 transition-colors p-1">
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                  </svg>
                </button>
              </div>
            )}
          </div>
          )
        ))}
        {filteredTemplates.length === 0 && <span className="text-sm text-slate-500">Шаблонов пока нет</span>}
      </div>

      {isAdmin && (
        <div className="bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-5 space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <Input label="Название" value={title} onChange={setTitle} placeholder="Приветствие" />
            <Input label="Категория" value={category} onChange={setCategory} placeholder="Общие" />
          </div>
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <label className="text-sm text-slate-400 font-medium">Текст шаблона</label>
              <button
                type="button"
                onClick={() => setContent((prev) => prev + (prev.endsWith("\n") ? "" : "\n") + "---\n")}
                className="text-[10px] px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 transition-colors"
                title="Разделить на несколько сообщений"
              >
                + Разделитель
              </button>
            </div>
            <textarea
              value={content}
              onChange={(e) => setContent(e.target.value)}
              placeholder={"Здравствуйте! Чем могу помочь?\n---\nВот наше предложение:"}
              rows={4}
              className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 resize-none"
            />
            {content.includes("\n---\n") && (
              <div className="text-[10px] text-amber-400 mt-1">📜 Скрипт — будет отправлено {content.split("\n---\n").filter(Boolean).length} сообщений</div>
            )}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Шорткат (необязательно)" value={shortcut} onChange={setShortcut} placeholder="/hello" />
            <div className="flex flex-col gap-1.5">
              <label className="text-sm text-slate-400 font-medium">Аккаунт</label>
              <select
                value={assignAccount}
                onChange={(e) => setAssignAccount(e.target.value)}
                className="bg-surface-card border border-surface-border rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-brand/50 transition-all duration-200 text-slate-300"
              >
                <option value="">Общий (все аккаунты)</option>
                {accounts.map((acc) => (
                  <option key={acc.id} value={acc.id}>{acc.display_name || acc.phone}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Media upload */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">Медиа (опционально)</label>
            <div className="flex flex-wrap gap-2 items-center">
              <label className="cursor-pointer px-3 py-2 rounded-xl border border-surface-border bg-surface text-sm text-slate-400 hover:border-brand/30 transition-colors">
                {mediaFile ? mediaFile.name : "📎 Файл"}
                <input type="file" className="hidden" accept="image/*,video/*,audio/*,.ogg"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    if (f) {
                      setMediaFile(f);
                      if (f.type.startsWith("image/")) setSendAs("photo");
                      else if (f.type.startsWith("video/")) setSendAs("video");
                      else if (f.type.startsWith("audio/")) setSendAs("voice");
                      else setSendAs("document");
                    }
                  }}
                />
              </label>
              {mediaFile && (
                <>
                  <select value={sendAs} onChange={(e) => setSendAs(e.target.value)}
                    className="px-2 py-2 rounded-xl border border-surface-border bg-surface text-xs text-slate-400 focus:outline-none">
                    <option value="photo">📷 Фото</option>
                    <option value="video">🎬 Видео</option>
                    <option value="video_note">🔵 Кружок</option>
                    <option value="voice">🎤 Голосовое</option>
                    <option value="document">📄 Документ</option>
                  </select>
                  <button onClick={() => { setMediaFile(null); setSendAs("auto"); }} className="text-red-400 text-xs hover:text-red-300">✕</button>
                </>
              )}
            </div>
          </div>

          <Button onClick={handleCreate} disabled={!title.trim() || !content.trim() || creating}>{creating ? "Создание..." : "Создать шаблон"}</Button>
        </div>
      )}
    </section>
  );
}

const TIMEZONE_OPTIONS = [
  { group: "Европа", zones: [
    { value: "Europe/Moscow", label: "Москва (UTC+3)" },
    { value: "Europe/Berlin", label: "Берлин (UTC+1/+2)" },
    { value: "Europe/London", label: "Лондон (UTC+0/+1)" },
    { value: "Europe/Paris", label: "Париж (UTC+1/+2)" },
    { value: "Europe/Istanbul", label: "Стамбул (UTC+3)" },
    { value: "Europe/Kiev", label: "Киев (UTC+2/+3)" },
  ]},
  { group: "Азия", zones: [
    { value: "Asia/Dubai", label: "Дубай (UTC+4)" },
    { value: "Asia/Bangkok", label: "Бангкок (UTC+7)" },
    { value: "Asia/Singapore", label: "Сингапур (UTC+8)" },
    { value: "Asia/Tokyo", label: "Токио (UTC+9)" },
    { value: "Asia/Shanghai", label: "Шанхай (UTC+8)" },
    { value: "Asia/Kolkata", label: "Калькутта (UTC+5:30)" },
  ]},
  { group: "Америка", zones: [
    { value: "America/New_York", label: "Нью-Йорк (UTC-5/-4)" },
    { value: "America/Chicago", label: "Чикаго (UTC-6/-5)" },
    { value: "America/Denver", label: "Денвер (UTC-7/-6)" },
    { value: "America/Los_Angeles", label: "Лос-Анджелес (UTC-8/-7)" },
    { value: "America/Sao_Paulo", label: "Сан-Паулу (UTC-3)" },
  ]},
  { group: "Другое", zones: [
    { value: "UTC", label: "UTC" },
    { value: "Pacific/Auckland", label: "Окленд (UTC+12/+13)" },
  ]},
];

function TimezoneSection() {
  const [currentTz, setCurrentTz] = useState("UTC");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api("/api/staff/me").then((me: any) => {
      if (me?.timezone) setCurrentTz(me.timezone);
    }).catch(() => {});
  }, []);

  const handleChange = async (tz: string) => {
    setCurrentTz(tz);
    setSaving(true);
    try {
      await updateTimezone(tz);
    } catch (e: any) {
      alert(e.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="animate-fade-in">
      <h2 className="text-lg font-semibold mb-3 flex items-center gap-2">
        <svg className="w-5 h-5 text-blue-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
        </svg>
        Часовой пояс
      </h2>
      <div className="bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-4">
        <div className="flex items-center gap-3">
          <select
            value={currentTz}
            onChange={(e) => handleChange(e.target.value)}
            disabled={saving}
            className="flex-1 bg-surface border border-surface-border rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-brand/50 transition-all duration-200 text-slate-300"
          >
            {TIMEZONE_OPTIONS.map((group) => (
              <optgroup key={group.group} label={group.group}>
                {group.zones.map((tz) => (
                  <option key={tz.value} value={tz.value}>{tz.label}</option>
                ))}
              </optgroup>
            ))}
          </select>
          {saving && <span className="text-xs text-slate-500">Сохранение...</span>}
        </div>
      </div>
    </section>
  );
}

function AdminSettingsSection() {
  const [accounts, setAccounts] = useState<TgStatusAccount[]>([]);
  const [saving, setSaving] = useState<string | null>(null);

  useEffect(() => {
    fetchTgStatus().then((accs) => {
      setAccounts(accs.filter((a) => a.is_active !== false));
    }).catch(console.error);
  }, []);

  // Fallback: if no accounts loaded from new endpoint, use old global setting
  const [globalShowRealNames, setGlobalShowRealNames] = useState(false);
  useEffect(() => {
    api("/api/settings/crm").then((s: any) => {
      setGlobalShowRealNames(s.show_real_names ?? false);
    }).catch(() => {});
  }, []);

  const toggleForAccount = async (accountId: string, val: boolean) => {
    setSaving(accountId);
    setAccounts((prev) => prev.map((a) => a.id === accountId ? { ...a, show_real_names: val } : a));
    try {
      await api(`/api/settings/crm?show_real_names=${val}&tg_account_id=${accountId}`, { method: "PATCH" });
    } catch (e: any) {
      alert(e.message);
      // Revert on error
      setAccounts((prev) => prev.map((a) => a.id === accountId ? { ...a, show_real_names: !val } : a));
    }
    setSaving(null);
  };

  const toggleGlobal = async (val: boolean) => {
    setGlobalShowRealNames(val);
    setSaving("global");
    try {
      await api(`/api/settings/crm?show_real_names=${val}`, { method: "PATCH" });
    } catch (e: any) { alert(e.message); }
    setSaving(null);
  };

  return (
    <section className="animate-fade-in">
      <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <svg className="w-5 h-5 text-amber-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
          <circle cx="12" cy="7" r="4" />
        </svg>
        Отображение контактов
      </h2>

      {accounts.length > 0 ? (
        <div className="space-y-3">
          {accounts.map((acc) => (
            <div key={acc.id} className="bg-gradient-to-r from-surface-card to-surface border border-surface-border rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium text-slate-300">{acc.display_name || acc.phone}</span>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => toggleForAccount(acc.id, false)}
                  disabled={saving === acc.id}
                  className={`flex-1 p-3 rounded-xl border transition-all text-left ${
                    !acc.show_real_names
                      ? "bg-brand/10 border-brand/30 text-brand"
                      : "border-surface-border text-slate-400 hover:border-brand/20"
                  }`}
                >
                  <div className="font-medium text-xs mb-0.5">Псевдонимы</div>
                  <div className="text-[10px] opacity-70">Анонимные имена</div>
                </button>
                <button
                  onClick={() => toggleForAccount(acc.id, true)}
                  disabled={saving === acc.id}
                  className={`flex-1 p-3 rounded-xl border transition-all text-left ${
                    acc.show_real_names
                      ? "bg-brand/10 border-brand/30 text-brand"
                      : "border-surface-border text-slate-400 hover:border-brand/20"
                  }`}
                >
                  <div className="font-medium text-xs mb-0.5">Настоящие имена</div>
                  <div className="text-[10px] opacity-70">Реальные имена из TG</div>
                </button>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="flex gap-3">
          <button
            onClick={() => toggleGlobal(false)}
            disabled={saving === "global"}
            className={`flex-1 p-4 rounded-xl border transition-all ${
              !globalShowRealNames
                ? "bg-brand/10 border-brand/30 text-brand"
                : "border-surface-border text-slate-400 hover:border-brand/20"
            }`}
          >
            <div className="font-medium text-sm mb-1">Анонимные псевдонимы</div>
            <div className="text-xs opacity-70">Операторы видят только псевдонимы клиентов</div>
          </button>
          <button
            onClick={() => toggleGlobal(true)}
            disabled={saving === "global"}
            className={`flex-1 p-4 rounded-xl border transition-all ${
              globalShowRealNames
                ? "bg-brand/10 border-brand/30 text-brand"
                : "border-surface-border text-slate-400 hover:border-brand/20"
            }`}
          >
            <div className="font-medium text-sm mb-1">Настоящие имена</div>
            <div className="text-xs opacity-70">Операторы видят реальные имена из Telegram</div>
          </button>
        </div>
      )}
    </section>
  );
}

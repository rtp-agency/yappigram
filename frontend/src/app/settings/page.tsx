"use client";

import { useEffect, useState } from "react";
import { api, clearAllCrmStorage, disconnectWS, getTemplates, createTemplate, updateTemplate, deleteTemplate, deleteTag, getRole, fetchTgStatus, createTag, updateTimezone, fetchNewChatsReport } from "@/lib";
import type { Template, TemplateBlock, TgStatusAccount, NewChatsReport } from "@/lib";
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
  const canManageContent = ["super_admin", "admin", "assistant"].includes(userRole);

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-8">
      <h1 className="text-2xl font-bold bg-gradient-to-r from-brand to-accent bg-clip-text text-transparent">Настройки</h1>

      {/* Dashboard link — for embedded/mini-app users */}
      {(isTelegramWebApp() || typeof window !== "undefined" && (() => { try { return window.self !== window.top || sessionStorage.getItem("crm_is_embedded") === "1"; } catch { return true; } })()) && (
        <button
          onClick={() => { try { window.parent.location.href = "/"; } catch { window.location.href = "https://metra-ai.org"; } }}
          className="w-full flex items-center gap-3 px-4 py-3 bg-gradient-to-r from-brand/10 to-accent/5 border border-brand/20 rounded-xl text-sm font-medium text-brand hover:bg-brand/15 transition-all"
        >
          <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
          Обратно в дашборд Metra AI
        </button>
      )}

      {isTelegramWebApp() && <WorkspaceSection />}
      {isAdmin && <TelegramSection />}
      <TimezoneSection />
      {isAdmin && <AdminSettingsSection />}
      <TagsSection />
      <TemplatesSection canManage={canManageContent} />
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
          <Button variant="ghost" onClick={() => { clearAllCrmStorage(); disconnectWS(); router.replace("/login?switch=1"); }}>
            Сменить
          </Button>
        </div>
      </div>
    </div>
  );
}

function TelegramSection() {
  const [accounts, setAccounts] = useState<any[]>([]);
  const [billingInfo, setBillingInfo] = useState<any>(null);
  const [phone, setPhone] = useState("");
  const [code, setCode] = useState("");
  const [password2fa, setPassword2fa] = useState("");
  const [step, setStep] = useState<"idle" | "code_sent">("idle");
  const [loading, setLoading] = useState(false);
  // Confirm-modal state — users should explicitly acknowledge that
  // linking a number will debit their METRA AI balance for the first month.
  const [confirmCharge, setConfirmCharge] = useState(false);

  const loadData = () => {
    api("/api/tg/status").then((res: any) => {
      const accs = Array.isArray(res) ? res : (res.accounts || []);
      setAccounts(accs.filter((a: any) => a.is_active));
    }).catch(console.error);
    api("/api/tg/billing").then((res: any) => {
      setBillingInfo(res);
    }).catch(console.error);
  };

  useEffect(() => { loadData(); }, []);

  // When billing is on, surface a modal so the user can't blindly click
  // past a 45-coin charge. When it's off (free period), skip straight to
  // sending the Telegram code — no charge is going to happen.
  const handleConnectClick = () => {
    if (billingInfo?.billing_enabled) {
      setConfirmCharge(true);
    } else {
      void connect();
    }
  };

  const connect = async () => {
    setConfirmCharge(false);
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

      {accounts.map((acc) => {
        // Find billing record for this account
        const billing = billingInfo?.accounts?.find(
          (b: any) => b.crm_account_id === acc.id || b.phone_number === acc.phone
        );
        return (
          <div key={acc.id} className="bg-gradient-to-r from-surface-card to-surface border border-surface-border rounded-xl p-4 mb-2">
            <div className="flex items-center gap-3">
              <div className={`w-2 h-2 rounded-full flex-shrink-0 ${acc.is_active ? "bg-emerald-400" : "bg-red-400"}`} />
              <span className="font-medium text-sm">{acc.display_name || acc.phone}</span>
              <span className={`text-xs ${acc.is_active ? "text-emerald-400/70" : "text-red-400/70"}`}>
                {acc.is_active ? "Активен" : "Отключён"}
              </span>
            </div>
            {/* Billing info */}
            {billing && (
              <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-text-muted">
                {billing.connected_at && (
                  <span>Подключён: {new Date(billing.connected_at).toLocaleDateString("ru-RU")}</span>
                )}
                {billing.next_charge_at && (
                  <span>Следующая оплата: {new Date(billing.next_charge_at).toLocaleDateString("ru-RU")}</span>
                )}
                <span>{Math.round(Number(billingInfo?.cost_per_month || 45))} коинов/мес</span>
              </div>
            )}
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
        );
      })}

      <div className="mt-4 bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-5 space-y-3">
        {step === "idle" ? (
          <>
            {/* Billing cost notice */}
            <div className="p-3 rounded-lg bg-amber-500/5 border border-amber-500/20 text-xs text-amber-200/80">
              <p className="font-medium mb-1">Стоимость: {Math.round(Number(billingInfo?.cost_per_month || 45))} Metra Coins / месяц</p>
              <p className="text-amber-200/60">
                {billingInfo?.billing_enabled
                  ? `Ваш баланс: ${billingInfo?.balance || "0"} коинов`
                  : "Бесплатный период — списания начнутся позже"
                }
              </p>
            </div>
            <Input label="Номер телефона" value={phone} onChange={(v) => {
              // Auto-format: strip non-digits, ensure + prefix
              const digits = v.replace(/[^\d+]/g, "");
              setPhone(digits.startsWith("+") ? digits : "+" + digits);
            }} placeholder="+79001234567" />
            <Button
              onClick={handleConnectClick}
              disabled={loading || !phone || (billingInfo?.billing_enabled && !billingInfo?.can_afford_new)}
            >
              {loading ? "Отправка кода..." :
                billingInfo?.billing_enabled && !billingInfo?.can_afford_new
                  ? "Недостаточно средств"
                  : "Подключить аккаунт"
              }
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

      {/* Explicit charge-confirmation modal.
          Shown only when billing_enabled=true so the user can't silently
          drop 45 coins by clicking "Подключить". Skipped during the free
          period when no debit would actually happen. */}
      {confirmCharge && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4"
          onClick={() => setConfirmCharge(false)}
        >
          <div
            className="bg-surface-card border border-surface-border rounded-2xl max-w-md w-full p-6 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center gap-3 mb-3">
              <div className="w-10 h-10 rounded-full bg-amber-500/15 border border-amber-500/30 flex items-center justify-center">
                <svg className="w-5 h-5 text-amber-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="10" />
                  <line x1="12" y1="8" x2="12" y2="12" />
                  <line x1="12" y1="16" x2="12.01" y2="16" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-text">Подтвердите оплату</h3>
            </div>

            <p className="text-sm text-text-muted mb-4">
              Подключение номера <b className="text-text">{phone}</b> к CRM спишет с вашего баланса METRA AI:
            </p>

            <div className="flex items-baseline justify-between bg-dark-900/40 border border-surface-border rounded-xl px-4 py-3 mb-4">
              <span className="text-sm text-text-muted">Стоимость</span>
              <span className="text-xl font-bold text-amber-400">
                {Math.round(Number(billingInfo?.cost_per_month || 45))} coins
              </span>
            </div>

            <div className="text-xs text-text-muted space-y-1 mb-5">
              <p>· Подписка на 30 дней с автопродлением</p>
              <p>· После подключения средства не возвращаются</p>
              <p>· Ваш текущий баланс: <b className="text-text">{billingInfo?.balance || "0"} coins</b></p>
            </div>

            <div className="flex gap-2">
              <Button variant="ghost" onClick={() => setConfirmCharge(false)}>
                Отмена
              </Button>
              <Button onClick={connect} disabled={loading}>
                {loading ? "Подтверждение..." : "Да, списать и подключить"}
              </Button>
            </div>
          </div>
        </div>
      )}
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
              <option value="">Выберите аккаунт</option>
              {accounts.map((acc) => (
                <option key={acc.id} value={acc.id}>{acc.display_name || acc.phone}</option>
              ))}
            </select>
          </div>
        )}
        <Button onClick={handleCreate} disabled={!name.trim() || (accounts.length > 0 && !tagAccount)}>Добавить</Button>
      </div>
    </section>
  );
}

// ============================================================
// Template Block Editor
// ============================================================

interface MediaFileEntry {
  path: string;
  type: string;
  file?: File;
}

interface EditorBlock {
  id: string;
  type: "text" | "photo" | "video" | "video_note" | "voice" | "document" | "media_group";
  content: string;
  media_path: string | null;
  media_type: string | null;
  mediaFile?: File | null;
  media_files?: MediaFileEntry[];
  mediaNewFiles?: File[];
  delay_after: number;
}

const BLOCK_TYPE_LABELS: Record<string, { icon: string; label: string }> = {
  text: { icon: "💬", label: "Текст" },
  photo: { icon: "📷", label: "Фото" },
  video: { icon: "🎬", label: "Видео" },
  media_group: { icon: "🖼", label: "Альбом" },
  video_note: { icon: "🔵", label: "Кружок" },
  voice: { icon: "🎤", label: "Голосовое" },
  document: { icon: "📄", label: "Документ" },
};

function newBlock(type: EditorBlock["type"] = "text"): EditorBlock {
  return { id: crypto.randomUUID(), type, content: "", media_path: null, media_type: null, mediaFile: null, delay_after: 0 };
}

function TemplatesSection({ canManage }: { canManage: boolean }) {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [accounts, setAccounts] = useState<{ id: string; phone: string; display_name: string | null }[]>([]);
  const [filterAccount, setFilterAccount] = useState<string | "all">("");

  // Editor state
  const [editorOpen, setEditorOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [title, setTitle] = useState("");
  const [category, setCategory] = useState("");
  const [shortcut, setShortcut] = useState("");
  const [assignAccount, setAssignAccount] = useState("");
  const [blocks, setBlocks] = useState<EditorBlock[]>([newBlock("text")]);
  const [saving, setSaving] = useState(false);
  const [showPreview, setShowPreview] = useState(false);
  const [dragIdx, setDragIdx] = useState<number | null>(null);
  const [dragOverIdx, setDragOverIdx] = useState<number | null>(null);

  useEffect(() => {
    getTemplates().then(setTemplates).catch(console.error);
    api("/api/tg/status").then((res: any) => {
      const accs = Array.isArray(res) ? res : (res.accounts || []);
      const mapped = accs.map((a: any) => ({ id: a.id, phone: a.phone, display_name: a.display_name || null }));
      setAccounts(mapped);
      if (mapped.length > 0 && !filterAccount) setFilterAccount(mapped[0].id);
    }).catch(() => {});
  }, []);

  const filteredTemplates = !filterAccount
    ? templates
    : templates.filter((t) => t.tg_account_id === filterAccount || !t.tg_account_id);

  // Open editor for new template
  const openNew = () => {
    setEditingId(null);
    setTitle(""); setCategory(""); setShortcut(""); setAssignAccount("");
    setBlocks([newBlock("text")]);
    setEditorOpen(true);
    setShowPreview(false);
  };

  // Open editor for existing template
  const openEdit = (tpl: Template) => {
    setEditingId(tpl.id);
    setTitle(tpl.title);
    setCategory(tpl.category || "");
    setShortcut(tpl.shortcut || "");
    setAssignAccount(tpl.tg_account_id || "");
    if (tpl.blocks_json && tpl.blocks_json.length > 0) {
      setBlocks(tpl.blocks_json.map(b => ({ ...b, content: b.content || "", media_path: b.media_path || null, media_type: b.media_type || null, mediaFile: null, delay_after: b.delay_after || 0 })));
    } else {
      // Convert legacy template to blocks
      const legacyBlocks: EditorBlock[] = [];
      if (tpl.content) {
        const parts = tpl.content.split("\n---\n");
        parts.forEach((p, i) => {
          const b = newBlock("text");
          b.content = p;
          if (i === 0 && tpl.media_path) {
            b.type = (tpl.media_type as EditorBlock["type"]) || "photo";
            b.media_path = tpl.media_path;
            b.media_type = tpl.media_type;
          }
          legacyBlocks.push(b);
        });
      }
      setBlocks(legacyBlocks.length > 0 ? legacyBlocks : [newBlock("text")]);
    }
    setEditorOpen(true);
    setShowPreview(false);
  };

  const closeEditor = () => {
    setEditorOpen(false);
    setEditingId(null);
  };

  // Block manipulation
  const updateBlock = (idx: number, patch: Partial<EditorBlock>) => {
    setBlocks(prev => prev.map((b, i) => i === idx ? { ...b, ...patch } : b));
  };

  const removeBlock = (idx: number) => {
    setBlocks(prev => prev.length <= 1 ? prev : prev.filter((_, i) => i !== idx));
  };

  const addBlock = (type: EditorBlock["type"] = "text") => {
    setBlocks(prev => [...prev, newBlock(type)]);
  };

  // Drag & drop
  const handleDragStart = (idx: number) => { setDragIdx(idx); };
  const handleDragOver = (e: React.DragEvent, idx: number) => { e.preventDefault(); setDragOverIdx(idx); };
  const handleDrop = (idx: number) => {
    if (dragIdx === null || dragIdx === idx) { setDragIdx(null); setDragOverIdx(null); return; }
    setBlocks(prev => {
      const arr = [...prev];
      const [moved] = arr.splice(dragIdx, 1);
      arr.splice(idx, 0, moved);
      return arr;
    });
    setDragIdx(null);
    setDragOverIdx(null);
  };
  const handleDragEnd = () => { setDragIdx(null); setDragOverIdx(null); };

  // Media file handling per block
  const handleBlockFile = (idx: number, file: File) => {
    const currentType = blocks[idx].type;
    // Keep the block type if it's already a specific media type (e.g. video_note stays video_note)
    if (currentType !== "text") {
      updateBlock(idx, { mediaFile: file });
    } else {
      let type: EditorBlock["type"] = "document";
      if (file.type.startsWith("image/")) type = "photo";
      else if (file.type.startsWith("video/")) type = "video";
      else if (file.type.startsWith("audio/") || file.type.includes("ogg")) type = "voice";
      updateBlock(idx, { mediaFile: file, type });
    }
  };

  // Save
  const handleSave = async () => {
    if (!title.trim() || saving) return;
    const hasContent = blocks.some(b => b.content.trim() || b.mediaFile || b.media_path || (b.media_files?.length || 0) > 0 || (b.mediaNewFiles?.length || 0) > 0);
    if (!hasContent) return;
    setSaving(true);
    try {
      const blocksData = blocks.map(b => ({
        id: b.id, type: b.type, content: b.content, media_path: b.media_path, media_type: b.media_type,
        media_files: b.media_files || [], delay_after: b.delay_after,
      }));

      let tpl: Template;
      if (editingId) {
        tpl = await updateTemplate(editingId, {
          title: title.trim(), category: category.trim() || null, shortcut: shortcut.trim() || null,
          blocks_json: blocksData as any,
        });
      } else {
        tpl = await createTemplate({
          title: title.trim(), content: blocks.map(b => b.content).filter(Boolean).join("\n---\n") || "(media)",
          category: category.trim() || undefined, shortcut: shortcut.trim() || undefined,
          tg_account_id: assignAccount || undefined, blocks_json: blocksData as any,
        });
      }

      // Upload media for blocks that have new files — all in parallel
      const blocksWithFiles = blocks.filter(b => b.mediaFile);
      if (blocksWithFiles.length > 0) {
        const uploads = await Promise.all(blocksWithFiles.map(block => {
          const formData = new FormData();
          formData.append("file", block.mediaFile!);
          const sendAs = block.type === "text" ? "auto" : block.type;
          return api(`/api/templates/${tpl.id}/upload-block-media?block_id=${block.id}&send_as=${sendAs}`, {
            method: "POST", body: formData, headers: {},
          });
        }));
        // Single update with all media paths
        const mediaMap = new Map(uploads.map((r: any) => [r.block_id, r]));
        const updatedBlocks = (tpl.blocks_json || blocksData).map((b: any) => {
          const upload = mediaMap.get(b.id);
          return upload ? { ...b, media_path: upload.media_path, media_type: upload.media_type } : b;
        });
        tpl = await updateTemplate(tpl.id, { blocks_json: updatedBlocks as any });
      }

      // Upload media_group files (multiple files per block)
      const groupBlocks = blocks.filter(b => b.type === "media_group" && (b.mediaNewFiles?.length || 0) > 0);
      if (groupBlocks.length > 0) {
        let currentBlocks = tpl.blocks_json || blocksData;
        for (const block of groupBlocks) {
          const uploads: any[] = [];
          for (const file of (block.mediaNewFiles || [])) {
            const formData = new FormData();
            formData.append("file", file);
            const sendAs = file.type.startsWith("video") ? "video" : "photo";
            const result = await api(`/api/templates/${tpl.id}/upload-block-media?block_id=${block.id}_${uploads.length}&send_as=${sendAs}`, {
              method: "POST", body: formData, headers: {},
            });
            uploads.push({ path: result.media_path, type: result.media_type });
          }
          // Merge with existing media_files
          currentBlocks = currentBlocks.map((b: any) => {
            if (b.id === block.id) {
              return { ...b, media_files: [...(b.media_files || []), ...uploads] };
            }
            return b;
          });
        }
        tpl = await updateTemplate(tpl.id, { blocks_json: currentBlocks as any });
      }

      setTemplates(prev => editingId
        ? prev.map(t => t.id === editingId ? { ...t, ...tpl } : t)
        : [...prev, tpl]
      );
      closeEditor();
    } catch (e: any) { alert(e.message); } finally { setSaving(false); }
  };

  const handleDelete = async (id: string) => {
    if (!confirm("Удалить шаблон?")) return;
    try {
      await deleteTemplate(id);
      setTemplates(prev => prev.filter(t => t.id !== id));
    } catch (e: any) { alert(e.message); }
  };

  const getBlocksSummary = (tpl: Template) => {
    const b = tpl.blocks_json;
    if (!b || b.length === 0) {
      const msgCount = tpl.content.split("\n---\n").filter(Boolean).length;
      return { count: msgCount, types: [tpl.media_type ? tpl.media_type : "text"] };
    }
    return { count: b.length, types: [...new Set(b.map(x => x.type))] };
  };

  // Telegram preview
  const CRM_MEDIA_BASE = "https://crm.metra-ai.org/api/media/";

  return (
    <section className="animate-fade-in">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold flex items-center gap-2">
          <svg className="w-5 h-5 text-purple-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
          </svg>
          Конструктор шаблонов
        </h2>
        {canManage && !editorOpen && (
          <button onClick={openNew} className="px-4 py-2 rounded-xl bg-brand text-white text-sm font-medium hover:bg-brand/80 transition-colors">
            + Новый шаблон
          </button>
        )}
      </div>

      {/* Account filter */}
      {accounts.length > 1 && (
        <div className="flex gap-1 mb-4 bg-surface border border-surface-border rounded-xl p-1 w-fit flex-wrap">
          {accounts.map(acc => (
            <button key={acc.id} onClick={() => setFilterAccount(acc.id)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${filterAccount === acc.id ? "bg-brand/15 text-brand" : "text-slate-400 hover:text-slate-300"}`}>
              {acc.display_name || acc.phone}
            </button>
          ))}
        </div>
      )}

      {/* Template list */}
      {!editorOpen && (
        <div className="space-y-2 mb-4">
          {filteredTemplates.map(tpl => {
            const summary = getBlocksSummary(tpl);
            return (
              <div key={tpl.id} className="bg-surface-card border border-surface-border rounded-xl p-3 flex items-start justify-between gap-3 animate-fade-in hover:border-surface-border/80 transition-colors">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-1 flex-wrap">
                    <span className="font-medium text-sm text-brand">{tpl.title}</span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-500/10 text-purple-400">
                      {summary.count} {summary.count === 1 ? "блок" : summary.count < 5 ? "блока" : "блоков"}
                    </span>
                    {summary.types.filter(t => t !== "text").map(t => (
                      <span key={t} className="text-[10px] px-1.5 py-0.5 rounded bg-brand/10 text-brand">
                        {BLOCK_TYPE_LABELS[t]?.icon || ""} {BLOCK_TYPE_LABELS[t]?.label || t}
                      </span>
                    ))}
                    {tpl.category && <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-slate-400">{tpl.category}</span>}
                    {tpl.shortcut && <span className="text-[10px] text-slate-500 font-mono">{tpl.shortcut}</span>}
                    {tpl.tg_account_id && (
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400">
                        {(() => { const a = accounts.find(a => a.id === tpl.tg_account_id); return a?.display_name || a?.phone || "—"; })()}
                      </span>
                    )}
                  </div>
                  <p className="text-xs text-slate-400 break-words line-clamp-2">{tpl.content.slice(0, 150)}{tpl.content.length > 150 ? "..." : ""}</p>
                </div>
                {canManage && (
                  <div className="flex items-center gap-1 shrink-0">
                    <button onClick={() => openEdit(tpl)} className="text-slate-600 hover:text-brand transition-colors p-1.5 rounded-lg hover:bg-brand/5" title="Редактировать">
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
                      </svg>
                    </button>
                    <button onClick={() => handleDelete(tpl.id)} className="text-slate-600 hover:text-red-400 transition-colors p-1.5 rounded-lg hover:bg-red-500/5" title="Удалить">
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                      </svg>
                    </button>
                  </div>
                )}
              </div>
            );
          })}
          {filteredTemplates.length === 0 && <p className="text-sm text-slate-500 text-center py-8">Шаблонов пока нет</p>}
        </div>
      )}

      {/* Block editor */}
      {editorOpen && (
        <div className="bg-gradient-to-br from-surface-card to-surface border border-brand/20 rounded-2xl p-5 space-y-4 animate-fade-in">
          <div className="flex items-center justify-between">
            <h3 className="text-base font-semibold text-white">{editingId ? "Редактирование шаблона" : "Новый шаблон"}</h3>
            <div className="flex items-center gap-2">
              <button onClick={() => setShowPreview(!showPreview)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${showPreview ? "bg-brand/15 text-brand" : "text-slate-400 hover:text-slate-300 bg-surface border border-surface-border"}`}>
                {showPreview ? "Скрыть превью" : "Превью"}
              </button>
              <button onClick={closeEditor} className="text-slate-500 hover:text-slate-300 p-1">
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></svg>
              </button>
            </div>
          </div>

          {/* Template meta */}
          <div className="grid grid-cols-2 gap-3">
            <Input label="Название" value={title} onChange={setTitle} placeholder="Приветствие" />
            <Input label="Категория" value={category} onChange={setCategory} placeholder="Общие" />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Шорткат" value={shortcut} onChange={setShortcut} placeholder="/hello" />
            {accounts.length > 0 && (
              <div className="flex flex-col gap-1.5">
                <label className="text-sm text-slate-400 font-medium">Аккаунт</label>
                <select value={assignAccount} onChange={e => setAssignAccount(e.target.value)}
                  className="bg-surface-card border border-surface-border rounded-xl px-3.5 py-2.5 text-sm focus:outline-none focus:border-brand/50 text-slate-300">
                  <option value="">Выберите аккаунт</option>
                  {accounts.map(acc => <option key={acc.id} value={acc.id}>{acc.display_name || acc.phone}</option>)}
                </select>
              </div>
            )}
          </div>

          {/* Blocks */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-2">Блоки сообщений</label>
            <div className="space-y-2">
              {blocks.map((block, idx) => (
                <div key={block.id}
                  draggable
                  onDragStart={() => handleDragStart(idx)}
                  onDragOver={e => handleDragOver(e, idx)}
                  onDrop={() => handleDrop(idx)}
                  onDragEnd={handleDragEnd}
                  className={`relative bg-surface border rounded-xl p-3 transition-all ${
                    dragOverIdx === idx ? "border-brand shadow-lg shadow-brand/10" : "border-surface-border"
                  } ${dragIdx === idx ? "opacity-40" : ""}`}
                >
                  {/* Block header */}
                  <div className="flex items-center gap-2 mb-2">
                    <div className="cursor-grab active:cursor-grabbing text-slate-600 hover:text-slate-400 p-0.5" title="Перетащите для изменения порядка">
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor"><circle cx="9" cy="6" r="1.5"/><circle cx="15" cy="6" r="1.5"/><circle cx="9" cy="12" r="1.5"/><circle cx="15" cy="12" r="1.5"/><circle cx="9" cy="18" r="1.5"/><circle cx="15" cy="18" r="1.5"/></svg>
                    </div>
                    <span className="text-xs font-medium text-slate-500">#{idx + 1}</span>
                    <select value={block.type} onChange={e => updateBlock(idx, { type: e.target.value as EditorBlock["type"] })}
                      className="bg-surface-card border border-surface-border rounded-lg px-2 py-1 text-xs text-slate-300 focus:outline-none focus:border-brand/50">
                      {Object.entries(BLOCK_TYPE_LABELS).map(([k, v]) => <option key={k} value={k}>{v.icon} {v.label}</option>)}
                    </select>
                    {/* Delay */}
                    <div className="flex items-center gap-1 ml-auto">
                      <svg className="w-3.5 h-3.5 text-slate-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                      <input type="number" min={0} max={30} step={0.5} value={block.delay_after}
                        onChange={e => updateBlock(idx, { delay_after: parseFloat(e.target.value) || 0 })}
                        className="w-14 bg-surface-card border border-surface-border rounded-lg px-2 py-1 text-xs text-slate-300 focus:outline-none focus:border-brand/50 text-center"
                        title="Задержка после этого блока (сек)"
                      />
                      <span className="text-[10px] text-slate-600">сек</span>
                    </div>
                    {blocks.length > 1 && (
                      <button onClick={() => removeBlock(idx)} className="text-slate-600 hover:text-red-400 transition-colors p-1 ml-1" title="Удалить блок">
                        <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></svg>
                      </button>
                    )}
                  </div>

                  {/* Block content */}
                  {(block.type === "text" || block.type === "photo" || block.type === "video" || block.type === "document" || block.type === "media_group") && (
                    <textarea value={block.content} onChange={e => updateBlock(idx, { content: e.target.value })}
                      placeholder={block.type === "text" ? "Текст сообщения..." : "Подпись к медиа (необязательно)..."}
                      rows={block.type === "text" ? 3 : 2}
                      className="w-full bg-surface-card border border-surface-border rounded-xl px-3 py-2 text-sm focus:outline-none focus:border-brand/50 resize-none mb-2"
                    />
                  )}

                  {/* Media upload — single file for regular blocks */}
                  {block.type !== "text" && block.type !== "media_group" && (
                    <div className="flex items-center gap-2 flex-wrap">
                      {block.media_path && !block.mediaFile && (
                        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-brand/5 border border-brand/20 text-xs text-brand">
                          {BLOCK_TYPE_LABELS[block.type]?.icon} {block.media_path.split("/").pop()}
                          <button onClick={() => updateBlock(idx, { media_path: null, media_type: null })} className="text-red-400 hover:text-red-300 ml-1">x</button>
                        </div>
                      )}
                      {block.mediaFile && (
                        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/5 border border-emerald-500/20 text-xs text-emerald-400">
                          {BLOCK_TYPE_LABELS[block.type]?.icon} {block.mediaFile.name}
                          <button onClick={() => updateBlock(idx, { mediaFile: null })} className="text-red-400 hover:text-red-300 ml-1">x</button>
                        </div>
                      )}
                      {!block.media_path && !block.mediaFile && (
                        <label className="cursor-pointer px-3 py-1.5 rounded-lg border border-dashed border-surface-border text-xs text-slate-400 hover:border-brand/30 hover:text-slate-300 transition-colors">
                          + Загрузить {BLOCK_TYPE_LABELS[block.type]?.label.toLowerCase()}
                          <input type="file" className="hidden" accept={
                            block.type === "photo" ? "image/*" :
                            block.type === "video" || block.type === "video_note" ? "video/*" :
                            block.type === "voice" ? "audio/*,.ogg" : "*"
                          } onChange={e => { const f = e.target.files?.[0]; if (f) handleBlockFile(idx, f); }} />
                        </label>
                      )}
                    </div>
                  )}

                  {/* Media group — multiple files */}
                  {block.type === "media_group" && (
                    <div className="space-y-2">
                      <div className="flex items-center gap-2 flex-wrap">
                        {(block.media_files || []).map((mf, fi) => (
                          <div key={fi} className="flex items-center gap-1 px-2 py-1 rounded-lg bg-brand/5 border border-brand/20 text-xs text-brand">
                            {mf.type === "video" ? "🎬" : "📷"} {mf.path?.split("/").pop() || mf.file?.name}
                            <button onClick={() => {
                              const updated = [...(block.media_files || [])];
                              updated.splice(fi, 1);
                              updateBlock(idx, { media_files: updated });
                            }} className="text-red-400 hover:text-red-300 ml-0.5">x</button>
                          </div>
                        ))}
                        {(block.mediaNewFiles || []).map((f, fi) => (
                          <div key={`new-${fi}`} className="flex items-center gap-1 px-2 py-1 rounded-lg bg-emerald-500/5 border border-emerald-500/20 text-xs text-emerald-400">
                            {f.type.startsWith("video") ? "🎬" : "📷"} {f.name}
                            <button onClick={() => {
                              const updated = [...(block.mediaNewFiles || [])];
                              updated.splice(fi, 1);
                              updateBlock(idx, { mediaNewFiles: updated });
                            }} className="text-red-400 hover:text-red-300 ml-0.5">x</button>
                          </div>
                        ))}
                      </div>
                      {((block.media_files?.length || 0) + (block.mediaNewFiles?.length || 0)) < 10 && (
                        <label className="cursor-pointer inline-flex px-3 py-1.5 rounded-lg border border-dashed border-surface-border text-xs text-slate-400 hover:border-brand/30 hover:text-slate-300 transition-colors">
                          + Добавить фото/видео (до 10)
                          <input type="file" className="hidden" accept="image/*,video/*" multiple onChange={e => {
                            const files = Array.from(e.target.files || []);
                            const maxAdd = 10 - (block.media_files?.length || 0) - (block.mediaNewFiles?.length || 0);
                            const toAdd = files.slice(0, maxAdd);
                            updateBlock(idx, { mediaNewFiles: [...(block.mediaNewFiles || []), ...toAdd] });
                            e.target.value = "";
                          }} />
                        </label>
                      )}
                      <p className="text-[10px] text-slate-600">{(block.media_files?.length || 0) + (block.mediaNewFiles?.length || 0)}/10 файлов</p>
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Add block buttons */}
            <div className="flex gap-2 mt-3 flex-wrap">
              {Object.entries(BLOCK_TYPE_LABELS).map(([type, { icon, label }]) => (
                <button key={type} onClick={() => addBlock(type as EditorBlock["type"])}
                  className="px-3 py-1.5 rounded-lg border border-dashed border-surface-border text-xs text-slate-400 hover:border-brand/30 hover:text-brand transition-colors">
                  {icon} {label}
                </button>
              ))}
            </div>
          </div>

          {/* Telegram Preview */}
          {showPreview && (
            <div className="mt-4">
              <label className="text-sm text-slate-400 font-medium block mb-2">Предпросмотр в Telegram</label>
              <div className="bg-[#0e1621] rounded-xl p-4 max-w-sm mx-auto border border-surface-border">
                <div className="space-y-1.5">
                  {blocks.map((block, idx) => (
                    <div key={block.id}>
                      {idx > 0 && blocks[idx - 1].delay_after > 0 && (
                        <div className="flex items-center justify-center gap-2 py-1">
                          <div className="h-px bg-slate-700 flex-1" />
                          <span className="text-[9px] text-slate-600 whitespace-nowrap">
                            <svg className="w-3 h-3 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                            {blocks[idx - 1].delay_after}с
                          </span>
                          <div className="h-px bg-slate-700 flex-1" />
                        </div>
                      )}
                      <div className={`max-w-[85%] ml-auto ${block.type === "voice" ? "" : ""}`}>
                        {block.type === "media_group" ? (
                          <div className="bg-[#2b5278] rounded-xl overflow-hidden">
                            <div className="grid grid-cols-2 gap-0.5 p-0.5">
                              {[...(block.media_files || []), ...(block.mediaNewFiles || []).map(f => ({ file: f, type: f.type.startsWith("video") ? "video" : "photo" }))].slice(0, 4).map((mf: any, i: number) => (
                                <div key={i} className="aspect-square bg-gradient-to-br from-brand/10 to-purple-500/10 flex items-center justify-center overflow-hidden">
                                  {mf.file ? (
                                    mf.type === "video" ? <span className="text-lg">🎬</span> : <img src={URL.createObjectURL(mf.file)} alt="" className="w-full h-full object-cover" />
                                  ) : (
                                    <span className="text-lg">{mf.type === "video" ? "🎬" : "📷"}</span>
                                  )}
                                </div>
                              ))}
                            </div>
                            {block.content && (
                              <div className="px-3 py-2">
                                <p className="text-sm text-white/90 whitespace-pre-wrap break-words">{block.content}</p>
                                <div className="flex justify-end mt-0.5"><span className="text-[10px] text-white/30">12:00 ✓✓</span></div>
                              </div>
                            )}
                          </div>
                        ) : block.type === "video_note" ? (
                          <div className="flex justify-end">
                            <div className="w-48 h-48 rounded-full bg-gradient-to-br from-brand/20 to-purple-500/20 border-2 border-brand/30 flex items-center justify-center">
                              {block.mediaFile ? (
                                <span className="text-[10px] text-slate-400 text-center px-4">{block.mediaFile.name}</span>
                              ) : block.media_path ? (
                                <span className="text-[10px] text-brand">🔵 Кружок</span>
                              ) : (
                                <span className="text-2xl opacity-30">🔵</span>
                              )}
                            </div>
                          </div>
                        ) : block.type === "voice" ? (
                          <div className="bg-[#2b5278] rounded-xl px-3 py-2 flex items-center gap-2 ml-auto w-fit max-w-full">
                            <div className="w-8 h-8 rounded-full bg-brand/30 flex items-center justify-center shrink-0">
                              <svg className="w-4 h-4 text-brand" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>
                            </div>
                            <div className="flex-1 min-w-[100px]">
                              <div className="flex items-center gap-0.5 h-6">
                                {Array.from({ length: 28 }, (_, i) => (
                                  <div key={i} className="w-[3px] bg-brand/40 rounded-full" style={{ height: `${Math.random() * 16 + 4}px` }} />
                                ))}
                              </div>
                              <span className="text-[10px] text-white/40">0:00</span>
                            </div>
                          </div>
                        ) : (
                          <div className="bg-[#2b5278] rounded-xl overflow-hidden">
                            {block.type === "photo" && (block.mediaFile || block.media_path) && (
                              <div className="w-full h-32 bg-gradient-to-br from-brand/10 to-purple-500/10 flex items-center justify-center">
                                {block.mediaFile ? (
                                  <img src={URL.createObjectURL(block.mediaFile)} alt="" className="w-full h-32 object-cover" />
                                ) : (
                                  <img src={CRM_MEDIA_BASE + block.media_path} alt="" className="w-full h-32 object-cover"
                                    onError={e => { (e.target as HTMLImageElement).style.display = "none"; }} />
                                )}
                              </div>
                            )}
                            {block.type === "video" && (block.mediaFile || block.media_path) && (
                              <div className="w-full h-32 bg-gradient-to-br from-brand/10 to-purple-500/10 flex items-center justify-center">
                                <svg className="w-10 h-10 text-white/20" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>
                              </div>
                            )}
                            {block.type === "document" && (block.mediaFile || block.media_path) && (
                              <div className="px-3 py-2 flex items-center gap-2 border-b border-white/5">
                                <div className="w-8 h-8 rounded-lg bg-brand/20 flex items-center justify-center shrink-0">
                                  <svg className="w-4 h-4 text-brand" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/></svg>
                                </div>
                                <span className="text-xs text-white/60 truncate">{block.mediaFile?.name || block.media_path?.split("/").pop()}</span>
                              </div>
                            )}
                            {(block.content || block.type === "text") && (
                              <div className="px-3 py-2">
                                <p className="text-sm text-white/90 whitespace-pre-wrap break-words">{block.content || <span className="text-white/20 italic">пустой текст</span>}</p>
                                <div className="flex justify-end mt-0.5">
                                  <span className="text-[10px] text-white/30">12:00 ✓✓</span>
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Actions */}
          <div className="flex gap-2 pt-2">
            <Button onClick={handleSave} disabled={!title.trim() || saving || (accounts.length > 0 && !assignAccount && !editingId)}>
              {saving ? "Сохранение..." : editingId ? "Сохранить" : "Создать шаблон"}
            </Button>
            <Button variant="ghost" onClick={closeEditor}>Отмена</Button>
          </div>
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
  const [tags, setTags] = useState<any[]>([]);
  const [templates, setTemplates] = useState<any[]>([]);

  useEffect(() => {
    fetchTgStatus().then((accs) => {
      setAccounts(accs.filter((a) => a.is_active !== false));
    }).catch(console.error);
    api("/api/tags").then((r: any) => setTags(r || [])).catch(() => {});
    api("/api/templates").then((r: any) => setTemplates(r || [])).catch(() => {});
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
    <>
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

    {/* Auto-tag + Auto-greeting */}
    <section className="bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-6">
      <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <svg className="w-5 h-5 text-emerald-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2v4m0 12v4M4.93 4.93l2.83 2.83m8.48 8.48l2.83 2.83M2 12h4m12 0h4M4.93 19.07l2.83-2.83m8.48-8.48l2.83-2.83" />
        </svg>
        Автоматизация (первое касание)
      </h2>
      <p className="text-xs text-slate-500 mb-4">
        При первом сообщении от нового контакта: автоматически присваиваются теги и отправляется приветствие.
      </p>

      {accounts.length > 0 && accounts.map((acc) => (
        <AutoSettingsCard key={acc.id} account={acc} allTags={tags} templates={templates} />
      ))}
    </section>
    </>
  );
}


// ── Auto-tag + auto-greeting card per TG account ──

function AutoSettingsCard({ account, allTags, templates }: {
  account: { id: string; display_name: string | null; phone: string };
  allTags: { id: string; name: string; color: string; tg_account_id?: string | null }[];
  templates: { id: string; title: string; tg_account_id?: string | null }[];
}) {
  const [autoTags, setAutoTags] = useState<string[]>([]);
  const [greetingId, setGreetingId] = useState<string>("");
  const [loaded, setLoaded] = useState(false);
  const [saving, setSaving] = useState(false);

  // Load current settings
  useEffect(() => {
    api(`/api/tg/${account.id}/auto-settings`).then((data: any) => {
      setAutoTags(data.auto_tags || []);
      setGreetingId(data.auto_greeting_template_id || "");
      setLoaded(true);
    }).catch(() => setLoaded(true));
  }, [account.id]);

  const save = async () => {
    setSaving(true);
    try {
      await api(`/api/tg/${account.id}/auto-settings`, {
        method: "PATCH",
        body: JSON.stringify({
          auto_tags: autoTags,
          auto_greeting_template_id: greetingId || "null",
        }),
      });
    } catch {}
    setSaving(false);
  };

  const toggleTag = (name: string) => {
    setAutoTags(prev => prev.includes(name) ? prev.filter(t => t !== name) : [...prev, name]);
  };

  const acctTemplates = templates.filter(t => !t.tg_account_id || t.tg_account_id === account.id);

  if (!loaded) return null;

  return (
    <div className="bg-surface border border-surface-border rounded-xl p-4 mb-3">
      <div className="flex items-center justify-between mb-3">
        <span className="text-sm font-medium text-slate-300">{account.display_name || account.phone}</span>
        <button
          onClick={save}
          disabled={saving}
          className="px-3 py-1 text-xs bg-brand/20 text-brand rounded-lg hover:bg-brand/30 disabled:opacity-50"
        >
          {saving ? "..." : "Сохранить"}
        </button>
      </div>

      {/* Auto-tags */}
      <div className="mb-3">
        <label className="block text-xs text-slate-500 mb-1.5">Авто-теги (присваиваются новому контакту)</label>
        <div className="flex flex-wrap gap-1.5">
          {allTags.filter(t => !t.tg_account_id || t.tg_account_id === account.id).map(tag => (
            <button
              key={tag.id}
              onClick={() => toggleTag(tag.name)}
              className={`px-2 py-0.5 rounded-full text-[11px] font-medium border transition-all ${
                autoTags.includes(tag.name)
                  ? "border-transparent shadow-sm"
                  : "border-surface-border opacity-40 hover:opacity-80"
              }`}
              style={{ backgroundColor: tag.color + "25", color: tag.color }}
            >
              {autoTags.includes(tag.name) ? "✓ " : "+ "}{tag.name}
            </button>
          ))}
          {allTags.length === 0 && <span className="text-[10px] text-slate-600">Нет тегов. Создайте выше.</span>}
        </div>
      </div>

      {/* Auto-greeting template */}
      <div>
        <label className="block text-xs text-slate-500 mb-1.5">Авто-приветствие (шаблон)</label>
        <select
          value={greetingId}
          onChange={(e) => setGreetingId(e.target.value)}
          className="w-full bg-surface-card border border-surface-border rounded-lg px-3 py-2 text-xs text-slate-300 focus:outline-none focus:border-brand/50"
        >
          <option value="">Без приветствия</option>
          {acctTemplates.map(t => (
            <option key={t.id} value={t.id}>{t.title}</option>
          ))}
        </select>
        <p className="text-[10px] text-slate-600 mt-1">
          Шаблон отправляется автоматически при первом сообщении от нового контакта.
        </p>
      </div>
    </div>
  );
}

"use client";

import { useEffect, useState } from "react";
import {
  api,
  getBroadcasts,
  createBroadcast,
  startBroadcast,
  pauseBroadcast,
  cancelBroadcast,
  onWSEvent,
  connectWS,
  type Broadcast,
  type Contact,
  type Tag,
  type TgAccount,
} from "@/lib";
import { AppShell, AuthGuard, Button, Input } from "@/components";

export default function BroadcastsPage() {
  return (
    <AuthGuard>
      <AppShell>
        <BroadcastsContent />
      </AppShell>
    </AuthGuard>
  );
}

type RecipientMode = "all" | "tags" | "random" | "manual";

function BroadcastsContent() {
  const [broadcasts, setBroadcasts] = useState<Broadcast[]>([]);
  const [showCreate, setShowCreate] = useState(false);
  const [accounts, setAccounts] = useState<TgAccount[]>([]);
  const [tags, setTags] = useState<Tag[]>([]);
  const [contacts, setContacts] = useState<Contact[]>([]);

  // Create form
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [sendAs, setSendAs] = useState<string>("auto");
  const [uploadedMedia, setUploadedMedia] = useState<{path: string; type: string} | null>(null);
  const [uploading, setUploading] = useState(false);
  const [selectedAccount, setSelectedAccount] = useState("");
  const [recipientMode, setRecipientMode] = useState<RecipientMode>("all");
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [maxRecipients, setMaxRecipients] = useState(20);
  const [manualContacts, setManualContacts] = useState<Set<string>>(new Set());
  const [contactSearch, setContactSearch] = useState("");
  const [delay, setDelay] = useState(1);
  const [creating, setCreating] = useState(false);
  const [editingBroadcast, setEditingBroadcast] = useState<Broadcast | null>(null);

  useEffect(() => {
    connectWS();
    getBroadcasts().then(setBroadcasts).catch(console.error);
    api("/api/tg/status").then((accs: TgAccount[]) => {
      const active = accs.filter((a) => a.is_active);
      setAccounts(active);
      if (active.length > 0) setSelectedAccount(active[0].id);
    }).catch(console.error);
    api("/api/tags").then(setTags).catch(console.error);
    api("/api/contacts?status=approved").then(setContacts).catch(console.error);
  }, []);

  useEffect(() => {
    const unsub = onWSEvent((event) => {
      if (event.type === "broadcast_progress") {
        setBroadcasts((prev) =>
          prev.map((bc) =>
            bc.id === event.broadcast_id
              ? { ...bc, sent_count: event.sent, failed_count: event.failed }
              : bc
          )
        );
      }
      if (event.type === "broadcast_status") {
        setBroadcasts((prev) =>
          prev.map((bc) =>
            bc.id === event.broadcast_id
              ? { ...bc, status: event.status }
              : bc
          )
        );
      }
    });
    return unsub;
  }, []);

  const handleCreate = async () => {
    if (!title.trim() || !selectedAccount) return;
    setCreating(true);
    try {
      let bc = await createBroadcast({
        title: title.trim(),
        content: content.trim() || undefined,
        tg_account_id: selectedAccount,
        tag_filter: (recipientMode === "tags" || recipientMode === "random") ? selectedTags : [],
        delay_seconds: delay,
        max_recipients: (recipientMode === "random") ? maxRecipients : undefined,
        contact_ids: recipientMode === "manual" ? Array.from(manualContacts) : [],
      });
      // Upload media if selected
      if (mediaFile) {
        setUploading(true);
        const formData = new FormData();
        formData.append("file", mediaFile);
        const mediaResult = await api(`/api/broadcasts/${bc.id}/upload-media?send_as=${sendAs}`, {
          method: "POST",
          body: formData,
          headers: {}, // let browser set Content-Type with boundary
        });
        bc = { ...bc, media_path: mediaResult.media_path, media_type: mediaResult.media_type };
        setUploading(false);
      }
      setBroadcasts((prev) => [bc, ...prev]);
      setShowCreate(false);
      setTitle(""); setContent(""); setSelectedTags([]); setManualContacts(new Set());
      setRecipientMode("all"); setMediaFile(null); setSendAs("auto"); setUploadedMedia(null);
    } catch (e: any) { alert(e.message); }
    setCreating(false);
  };

  const handleStart = async (id: string) => {
    try {
      await startBroadcast(id);
      setBroadcasts((prev) => prev.map((bc) => (bc.id === id ? { ...bc, status: "running" } : bc)));
    } catch (e: any) { alert(e.message); }
  };

  const handlePause = async (id: string) => {
    try {
      await pauseBroadcast(id);
      setBroadcasts((prev) => prev.map((bc) => (bc.id === id ? { ...bc, status: "paused" } : bc)));
    } catch (e: any) { alert(e.message); }
  };

  const handleCancel = async (id: string) => {
    try {
      await cancelBroadcast(id);
      setBroadcasts((prev) => prev.map((bc) => (bc.id === id ? { ...bc, status: "cancelled" } : bc)));
    } catch (e: any) { alert(e.message); }
  };

  const handleDelete = async (id: string) => {
    if (!confirm("Удалить рассылку?")) return;
    try {
      await api(`/api/broadcasts/${id}`, { method: "DELETE" });
      setBroadcasts((prev) => prev.filter((bc) => bc.id !== id));
    } catch (e: any) { alert(e.message); }
  };

  const handleEdit = (bc: Broadcast) => {
    setEditingBroadcast(bc);
    setTitle(bc.title);
    setContent(bc.content || "");
    setSelectedAccount(bc.tg_account_id);
    setSelectedTags(bc.tag_filter || []);
    setDelay(bc.delay_seconds);
    setMaxRecipients(bc.max_recipients || 20);
    setManualContacts(new Set(bc.contact_ids || []));
    if (bc.contact_ids?.length) setRecipientMode("manual");
    else if (bc.max_recipients) setRecipientMode("random");
    else if (bc.tag_filter?.length) setRecipientMode("tags");
    else setRecipientMode("all");
    setShowCreate(true);
  };

  const handleSaveEdit = async () => {
    if (!editingBroadcast || !title.trim() || !selectedAccount) return;
    setCreating(true);
    try {
      const updated = await api(`/api/broadcasts/${editingBroadcast.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          title: title.trim(),
          content: content.trim() || null,
          tg_account_id: selectedAccount,
          tag_filter: (recipientMode === "tags" || recipientMode === "random") ? selectedTags : [],
          delay_seconds: delay,
          max_recipients: recipientMode === "random" ? maxRecipients : null,
          contact_ids: recipientMode === "manual" ? Array.from(manualContacts) : [],
        }),
      });
      setBroadcasts((prev) => prev.map((bc) => (bc.id === editingBroadcast.id ? updated : bc)));
      setShowCreate(false);
      setEditingBroadcast(null);
      setTitle(""); setContent(""); setSelectedTags([]); setManualContacts(new Set());
    } catch (e: any) { alert(e.message); }
    setCreating(false);
  };

  const statusColor: Record<string, string> = {
    draft: "text-slate-400", running: "text-emerald-400", paused: "text-amber-400",
    completed: "text-brand", cancelled: "text-red-400",
  };
  const statusLabel: Record<string, string> = {
    draft: "Черновик", running: "Отправка", paused: "Пауза",
    completed: "Завершено", cancelled: "Отменено",
  };

  const privateContacts = contacts.filter((c) => c.chat_type === "private" && !c.is_archived);
  const filteredManualContacts = privateContacts.filter((c) =>
    !contactSearch || c.alias.toLowerCase().includes(contactSearch.toLowerCase())
  );

  const modeLabels: Record<RecipientMode, string> = {
    all: "Все контакты",
    tags: "По тегам",
    random: "Случайные N",
    manual: "Вручную",
  };

  return (
    <div className="p-6 max-w-3xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold bg-gradient-to-r from-brand to-accent bg-clip-text text-transparent">
          Рассылки
        </h1>
        <Button onClick={() => setShowCreate(true)}>Создать рассылку</Button>
      </div>

      {showCreate && (
        <div className="bg-gradient-to-br from-surface-card to-surface border border-surface-border rounded-2xl p-5 mb-6 space-y-4 animate-slide-up">
          <h2 className="font-semibold text-lg">{editingBroadcast ? "Редактирование рассылки" : "Новая рассылка"}</h2>
          <Input label="Название" value={title} onChange={setTitle} placeholder="Акция" />
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">Текст сообщения</label>
            <textarea
              value={content}
              onChange={(e) => setContent(e.target.value)}
              placeholder="Текст рассылки..."
              rows={4}
              className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 resize-none"
            />
          </div>
          {/* Media upload */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">Медиа (опционально)</label>
            <div className="flex flex-wrap gap-2 items-center">
              <label className="cursor-pointer px-3 py-2 rounded-xl border border-surface-border bg-surface text-sm text-slate-400 hover:border-brand/30 transition-colors">
                {mediaFile ? mediaFile.name : "📎 Выбрать файл"}
                <input
                  type="file"
                  className="hidden"
                  accept="image/*,video/*,audio/*,.ogg"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    if (f) {
                      setMediaFile(f);
                      // auto-detect send_as
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
                  <select
                    value={sendAs}
                    onChange={(e) => setSendAs(e.target.value)}
                    className="px-2 py-2 rounded-xl border border-surface-border bg-surface text-xs text-slate-400 focus:outline-none"
                  >
                    <option value="photo">📷 Фото</option>
                    <option value="video">🎬 Видео</option>
                    <option value="video_note">🔵 Кружок</option>
                    <option value="voice">🎤 Голосовое</option>
                    <option value="document">📄 Документ</option>
                  </select>
                  <button onClick={() => { setMediaFile(null); setSendAs("auto"); }} className="text-red-400 text-xs hover:text-red-300">✕ Убрать</button>
                </>
              )}
            </div>
            {sendAs === "video_note" && <p className="text-[10px] text-slate-500 mt-1">Видео будет обрезано в квадрат (макс. 60 сек, без звука)</p>}
            {sendAs === "voice" && <p className="text-[10px] text-slate-500 mt-1">Аудио будет конвертировано в OGG Opus</p>}
          </div>

          {accounts.length > 0 && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">Telegram аккаунт</label>
              <select value={selectedAccount} onChange={(e) => setSelectedAccount(e.target.value)}
                className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50">
                {accounts.map((acc) => <option key={acc.id} value={acc.id}>{acc.phone}</option>)}
              </select>
            </div>
          )}

          {/* Recipient mode selector */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">Получатели</label>
            <div className="flex gap-1.5 flex-wrap">
              {(Object.keys(modeLabels) as RecipientMode[]).map((mode) => (
                <button key={mode} onClick={() => setRecipientMode(mode)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                    recipientMode === mode
                      ? "bg-brand/20 border-brand/40 text-brand"
                      : "border-surface-border text-slate-400 hover:border-slate-500"
                  }`}>
                  {modeLabels[mode]}
                </button>
              ))}
            </div>
          </div>

          {/* Tag filter (for tags & random modes) */}
          {(recipientMode === "tags" || recipientMode === "random") && tags.length > 0 && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">
                Фильтр по тегам {recipientMode === "tags" ? "(пусто = все)" : ""}
              </label>
              <div className="flex flex-wrap gap-1.5">
                {tags.map((tag) => (
                  <button key={tag.id}
                    onClick={() => setSelectedTags((prev) =>
                      prev.includes(tag.name) ? prev.filter((t) => t !== tag.name) : [...prev, tag.name]
                    )}
                    className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-all ${
                      selectedTags.includes(tag.name)
                        ? "border-transparent shadow-sm"
                        : "border-surface-border opacity-50 hover:opacity-80"
                    }`}
                    style={{ backgroundColor: tag.color + "25", color: tag.color, borderColor: selectedTags.includes(tag.name) ? tag.color + "40" : undefined }}>
                    {selectedTags.includes(tag.name) ? "✓ " : ""}{tag.name}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Max recipients (random mode) */}
          {recipientMode === "random" && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">
                Количество получателей: {maxRecipients}
              </label>
              <input type="range" min={1} max={Math.max(privateContacts.length, 1)} value={Math.min(maxRecipients, Math.max(privateContacts.length, 1))}
                onChange={(e) => setMaxRecipients(Number(e.target.value))} className="w-full accent-brand" />
              <div className="flex justify-between text-[10px] text-slate-500 mt-1">
                <span>1</span>
                <span>{privateContacts.length} контактов</span>
              </div>
            </div>
          )}

          {/* Manual contact selector */}
          {recipientMode === "manual" && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">
                Выберите контакты ({manualContacts.size} выбрано)
              </label>
              <input type="text" value={contactSearch} onChange={(e) => setContactSearch(e.target.value)}
                placeholder="Поиск контактов..."
                className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2 text-sm focus:outline-none focus:border-brand/50 mb-2" />
              <div className="max-h-48 overflow-y-auto space-y-1 border border-surface-border rounded-xl p-2">
                {filteredManualContacts.map((c) => (
                  <label key={c.id} className="flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-surface-hover cursor-pointer">
                    <input type="checkbox" checked={manualContacts.has(c.id)}
                      onChange={() => setManualContacts((prev) => {
                        const next = new Set(prev);
                        next.has(c.id) ? next.delete(c.id) : next.add(c.id);
                        return next;
                      })}
                      className="accent-brand" />
                    <span className="text-sm">{c.alias}</span>
                    {c.tags?.length > 0 && (
                      <span className="text-[10px] text-slate-500">{c.tags.join(", ")}</span>
                    )}
                  </label>
                ))}
                {filteredManualContacts.length === 0 && (
                  <p className="text-xs text-slate-500 text-center py-2">Контакты не найдены</p>
                )}
              </div>
            </div>
          )}

          {/* Delay slider */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">
              Задержка: {delay >= 60 ? `${Math.floor(delay / 60)} мин ${delay % 60 ? delay % 60 + " сек" : ""}` : `${delay} сек`}
            </label>
            <input type="range" min={1} max={3600} value={delay}
              onChange={(e) => setDelay(Number(e.target.value))} className="w-full accent-brand" />
            <div className="flex justify-between text-[10px] text-slate-500 mt-1">
              <span>1 сек</span><span>60 мин</span>
            </div>
          </div>

          <div className="flex gap-2 justify-end">
            <Button variant="ghost" onClick={() => { setShowCreate(false); setEditingBroadcast(null); setTitle(""); setContent(""); }}>Отмена</Button>
            <Button onClick={editingBroadcast ? handleSaveEdit : handleCreate} disabled={creating || !title.trim() || !selectedAccount ||
              (recipientMode === "manual" && manualContacts.size === 0)}>
              {creating ? "Сохранение..." : editingBroadcast ? "Сохранить" : "Создать"}
            </Button>
          </div>
        </div>
      )}

      <div className="space-y-3">
        {broadcasts.map((bc) => {
          const progress = bc.total_recipients > 0 ? Math.round(((bc.sent_count + bc.failed_count) / bc.total_recipients) * 100) : 0;
          return (
            <div key={bc.id} className="bg-surface-card border border-surface-border rounded-xl p-4 animate-fade-in">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <h3 className="font-medium">{bc.title}</h3>
                  <span className={`text-xs font-medium ${statusColor[bc.status] || "text-slate-400"}`}>
                    {statusLabel[bc.status] || bc.status}
                  </span>
                  {bc.max_recipients && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-brand/10 text-brand">
                      макс. {bc.max_recipients}
                    </span>
                  )}
                </div>
                <div className="flex gap-1.5">
                  {bc.status === "draft" && (
                    <>
                      <Button variant="ghost" onClick={() => handleEdit(bc)}>
                        <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/><path d="m15 5 4 4"/></svg>
                      </Button>
                      <Button variant="primary" onClick={() => handleStart(bc.id)}>Запустить</Button>
                    </>
                  )}
                  {bc.status === "running" && <Button variant="secondary" onClick={() => handlePause(bc.id)}>Пауза</Button>}
                  {bc.status === "paused" && <Button variant="primary" onClick={() => handleStart(bc.id)}>Продолжить</Button>}
                  {(bc.status === "running" || bc.status === "paused") && (
                    <Button variant="danger" onClick={() => handleCancel(bc.id)}>Отменить</Button>
                  )}
                  {bc.status !== "running" && (
                    <Button variant="ghost" onClick={() => handleDelete(bc.id)}>
                      <svg className="w-3.5 h-3.5 text-red-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>
                    </Button>
                  )}
                </div>
              </div>
              {bc.media_type && (
                <span className="text-[10px] px-1.5 py-0.5 rounded bg-brand/10 text-brand mr-1">
                  {bc.media_type === "photo" ? "📷 Фото" : bc.media_type === "video" ? "🎬 Видео" : bc.media_type === "video_note" ? "🔵 Кружок" : bc.media_type === "voice" ? "🎤 Голосовое" : "📄 Файл"}
                </span>
              )}
              {bc.content && <p className="text-sm text-slate-400 mb-2 line-clamp-2">{bc.content}</p>}
              {bc.total_recipients > 0 && (
                <div className="mt-2">
                  <div className="flex items-center justify-between text-xs text-slate-500 mb-1">
                    <span>Отправлено: {bc.sent_count} / {bc.total_recipients}</span>
                    {bc.failed_count > 0 && <span className="text-red-400">Ошибки: {bc.failed_count}</span>}
                    <span>{progress}%</span>
                  </div>
                  <div className="w-full bg-surface-border rounded-full h-1.5">
                    <div className="bg-brand rounded-full h-1.5 transition-all duration-500" style={{ width: `${progress}%` }} />
                  </div>
                </div>
              )}
              {bc.tag_filter.length > 0 && (
                <div className="flex gap-1 mt-2">
                  {bc.tag_filter.map((t) => (
                    <span key={t} className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-slate-400">{t}</span>
                  ))}
                </div>
              )}
              <div className="text-[10px] text-slate-600 mt-2">
                {new Date(bc.created_at).toLocaleString()} | Задержка: {bc.delay_seconds}с
              </div>
            </div>
          );
        })}
        {broadcasts.length === 0 && !showCreate && (
          <div className="text-center py-16 text-slate-500">
            <p className="text-sm">Рассылок пока нет</p>
            <p className="text-xs mt-1">Создайте первую рассылку для массовой отправки сообщений</p>
          </div>
        )}
      </div>
    </div>
  );
}

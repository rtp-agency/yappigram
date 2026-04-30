"use client";

import { useEffect, useMemo, useRef, useState } from "react";
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
  // Archived contacts cached per tg_account_id. Lazy-loaded only when the
  // user opts into "включая архивные" with that account selected. Switching
  // accounts triggers a fresh fetch for the new one if not already cached.
  const [archivedByAccount, setArchivedByAccount] = useState<Record<string, Contact[]>>({});
  const [archivedLoadingFor, setArchivedLoadingFor] = useState<string | null>(null);
  // In-flight fetch tracker. useRef instead of useState because we DON'T want
  // setting/clearing this to trigger an effect re-run — that's exactly what
  // killed the previous version (cleanup ran with cancelled=true before the
  // fetch resolved → finally never reset loading → eternal "загрузка...").
  const archivedFetchInFlight = useRef<Set<string>>(new Set());

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
  // Exclude list — contacts with any of these tags are dropped at send time.
  // Kept mutually exclusive with selectedTags in the UI: picking a tag as
  // "exclude" auto-removes it from "include" and vice versa.
  const [excludedTags, setExcludedTags] = useState<string[]>([]);
  // In tags mode, flip this on to cherry-pick contacts FROM within the
  // tag-filtered set instead of sending to everyone matching the tags.
  const [cherryPick, setCherryPick] = useState(false);
  // When on, archived contacts are eligible for the recipient set. Tags
  // sit on contacts regardless of archive state, so this lets a "tag-only
  // in-archive" cohort still be reached. Default off — preserves the
  // historical behavior, prevents accidental bulk-send to archived chats.
  const [includeArchived, setIncludeArchived] = useState(false);
  const [maxRecipients, setMaxRecipients] = useState(20);
  const [manualContacts, setManualContacts] = useState<Set<string>>(new Set());
  const [contactSearch, setContactSearch] = useState("");
  // Minimum delay raised from 1s to 5s: with the per-account flood cap
  // already sitting at ~1 msg/sec, a 1s user delay meant every broadcast
  // ran right at the ban threshold. 5s gives real headroom.
  const BROADCAST_MIN_DELAY = 5;
  const [delay, setDelay] = useState(BROADCAST_MIN_DELAY);
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

  // Lazy-load archived contacts on demand, scoped to the currently
  // selected TG account.
  //
  // Two design choices that matter:
  //  1. Per-account scope (?tg_account_id=...) — /api/contacts without
  //     account filter returns archived chats across the whole org. With
  //     thousands of contacts and dozens of accounts that's seconds of
  //     network + JSON parse for data the user can't even broadcast to
  //     (a broadcast targets exactly one account). The narrower request
  //     is what the chats page already does.
  //  2. useRef in-flight tracker (NOT useState) — the previous version
  //     had `archivedLoading` in the effect deps; setArchivedLoading(true)
  //     re-triggered the effect, the cleanup set cancelled=true on the
  //     just-started fetch, and the .finally() then no-op'd because
  //     cancelled was true. Loading state stuck on forever. The real
  //     in-flight signal must NOT live in deps.
  //
  // Cache key is tg_account_id. Switching accounts triggers a fresh
  // fetch for the new one; previously-loaded accounts stay cached.
  useEffect(() => {
    if (!includeArchived || !selectedAccount) return;
    if (archivedByAccount[selectedAccount]) return; // cached
    if (archivedFetchInFlight.current.has(selectedAccount)) return;

    const acctId = selectedAccount;
    archivedFetchInFlight.current.add(acctId);
    setArchivedLoadingFor(acctId);
    let cancelled = false;

    api(`/api/contacts?status=approved&archived=true&tg_account_id=${encodeURIComponent(acctId)}`)
      .then((data: Contact[]) => {
        if (cancelled) return;
        setArchivedByAccount((prev) => ({ ...prev, [acctId]: data }));
      })
      .catch((e: any) => {
        if (cancelled) return;
        console.error("Failed to load archived contacts", e);
      })
      .finally(() => {
        archivedFetchInFlight.current.delete(acctId);
        if (cancelled) return;
        // Only clear the loading marker if it's still pointing at THIS
        // fetch (account may have changed during the request).
        setArchivedLoadingFor((curr) => (curr === acctId ? null : curr));
      });

    return () => { cancelled = true; };
  }, [includeArchived, selectedAccount, archivedByAccount]);

  // Derived state for the rest of the component — keeps the rest of the
  // file readable as if archivedContacts/archivedLoading were plain values.
  const archivedContacts = selectedAccount
    ? (archivedByAccount[selectedAccount] || [])
    : [];
  const archivedLoading = archivedLoadingFor === selectedAccount && !!selectedAccount;

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

  // Drop any manual-pick IDs that aren't in the currently eligible
  // pool. Avoids persisting "phantom" IDs the server would silently
  // skip at send time (e.g. user picked archived contacts, then turned
  // off include_archived without unchecking them).
  //
  // CAVEAT: if the user just opened an edit modal for a broadcast with
  // include_archived=ON and the archived list for the currently selected
  // account hasn't lazy-loaded yet, the eligible pool is incomplete and
  // we MUST NOT drop those IDs — doing so would silently lose legitimately
  // saved archived picks. The pool is "complete" when:
  //   - includeArchived=false → pool is by definition non-archived only
  //   - includeArchived=true → archived for selectedAccount is cached
  // While the archived fetch is still in flight we return all IDs as
  // "kept" with dropped=0 (no warning). The next save after the load
  // completes will sanitize correctly.
  const sanitizeManualContacts = (): { kept: string[]; dropped: number } => {
    const archivedReady = !!selectedAccount && !!archivedByAccount[selectedAccount];
    const poolComplete = !includeArchived || archivedReady;
    if (!poolComplete) {
      return { kept: Array.from(manualContacts), dropped: 0 };
    }
    const eligible = new Set(privateContacts.map((c) => c.id));
    const kept: string[] = [];
    let dropped = 0;
    manualContacts.forEach((id) => {
      if (eligible.has(id)) kept.push(id);
      else dropped += 1;
    });
    return { kept, dropped };
  };

  const handleCreate = async () => {
    if (!title.trim() || !selectedAccount) return;
    setCreating(true);
    try {
      // In tags mode with cherryPick toggled on, we stuff the user's
      // picks into contact_ids and leave tag_filter on as a belt-and-
      // braces safety net (server will intersect). Without cherryPick,
      // contact_ids stays empty → server falls back to tag_filter alone.
      // Manual mode is unchanged: pure contact_ids, no tag_filter.
      // Cherry-pick requires at least one include tag actually selected —
      // otherwise the UI block is hidden and the user's old picks would
      // get silently submitted as a manual-mode broadcast.
      const sanitized = sanitizeManualContacts();
      const useCherryPick = recipientMode === "tags" && cherryPick && sanitized.kept.length > 0 && selectedTags.length > 0;
      if (sanitized.dropped > 0 && (recipientMode === "manual" || useCherryPick)) {
        const ok = confirm(
          `${sanitized.dropped} выбранных контактов не входят в текущий фильтр (например, архивные при выключенном «Включая архивные»). ` +
          `Они будут исключены из рассылки. Продолжить?`,
        );
        if (!ok) { setCreating(false); return; }
      }
      let bc = await createBroadcast({
        title: title.trim(),
        content: content.trim() || undefined,
        tg_account_id: selectedAccount,
        tag_filter: (recipientMode === "tags" || recipientMode === "random") ? selectedTags : [],
        tag_exclude: recipientMode === "all" ? [] : excludedTags,
        // Persist the toggle for ALL modes — manual selection of an
        // archived contact is otherwise silently dropped server-side.
        include_archived: includeArchived,
        delay_seconds: Math.max(BROADCAST_MIN_DELAY, delay),
        max_recipients: (recipientMode === "random") ? maxRecipients : undefined,
        contact_ids: recipientMode === "manual" || useCherryPick ? sanitized.kept : [],
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
      setTitle(""); setContent(""); setSelectedTags([]); setExcludedTags([]);
      setManualContacts(new Set()); setCherryPick(false);
      setIncludeArchived(false);
      setRecipientMode("all"); setMediaFile(null); setSendAs("auto"); setUploadedMedia(null);
      setDelay(BROADCAST_MIN_DELAY);
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
    setExcludedTags(bc.tag_exclude || []);
    setIncludeArchived(!!bc.include_archived);
    // Clamp: any grandfathered draft saved with delay < 5 gets bumped
    // to the new floor when reopened for editing.
    setDelay(Math.max(BROADCAST_MIN_DELAY, bc.delay_seconds));
    setMaxRecipients(bc.max_recipients || 20);
    setManualContacts(new Set(bc.contact_ids || []));
    // A broadcast with BOTH tag_filter AND contact_ids was saved in
    // "tags + cherry-pick" mode. Restore that instead of downgrading
    // to plain manual.
    const hasTags = (bc.tag_filter?.length ?? 0) > 0;
    const hasIds = (bc.contact_ids?.length ?? 0) > 0;
    if (hasTags && hasIds) {
      setRecipientMode("tags");
      setCherryPick(true);
    } else if (hasIds) {
      setRecipientMode("manual");
      setCherryPick(false);
    } else if (bc.max_recipients) {
      setRecipientMode("random");
      setCherryPick(false);
    } else if (hasTags) {
      setRecipientMode("tags");
      setCherryPick(false);
    } else {
      setRecipientMode("all");
      setCherryPick(false);
    }
    setShowCreate(true);
  };

  const handleSaveEdit = async () => {
    if (!editingBroadcast || !title.trim() || !selectedAccount) return;
    setCreating(true);
    try {
      // Cherry-pick requires at least one include tag actually selected —
      // otherwise the UI block is hidden and the user's old picks would
      // get silently submitted as a manual-mode broadcast.
      const sanitized = sanitizeManualContacts();
      const useCherryPick = recipientMode === "tags" && cherryPick && sanitized.kept.length > 0 && selectedTags.length > 0;
      if (sanitized.dropped > 0 && (recipientMode === "manual" || useCherryPick)) {
        const ok = confirm(
          `${sanitized.dropped} выбранных контактов не входят в текущий фильтр (например, архивные при выключенном «Включая архивные»). ` +
          `Они будут удалены из рассылки. Продолжить?`,
        );
        if (!ok) { setCreating(false); return; }
      }
      const updated = await api(`/api/broadcasts/${editingBroadcast.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          title: title.trim(),
          content: content.trim() || null,
          tg_account_id: selectedAccount,
          tag_filter: (recipientMode === "tags" || recipientMode === "random") ? selectedTags : [],
          tag_exclude: recipientMode === "all" ? [] : excludedTags,
          include_archived: includeArchived,
          delay_seconds: Math.max(BROADCAST_MIN_DELAY, delay),
          max_recipients: recipientMode === "random" ? maxRecipients : null,
          contact_ids: recipientMode === "manual" || useCherryPick ? sanitized.kept : [],
        }),
      });
      setBroadcasts((prev) => prev.map((bc) => (bc.id === editingBroadcast.id ? updated : bc)));
      setShowCreate(false);
      setEditingBroadcast(null);
      setTitle(""); setContent(""); setSelectedTags([]); setExcludedTags([]);
      setManualContacts(new Set()); setCherryPick(false);
      setIncludeArchived(false);
    } catch (e: any) { alert(e.message); }
    setCreating(false);
  };

  const statusColor: Record<string, string> = {
    draft: "text-slate-400", running: "text-emerald-400", paused: "text-amber-400",
    completed: "text-brand", cancelled: "text-red-400", failed: "text-red-500",
  };
  const statusLabel: Record<string, string> = {
    draft: "Черновик", running: "Отправка", paused: "Пауза",
    completed: "Завершено", cancelled: "Отменено", failed: "Сбой",
  };

  // When the user opts into archived contacts, merge the lazy-loaded
  // archived list into the eligible pool. Otherwise behave exactly as
  // before: only non-archived.
  //
  // Both pools are scoped to the currently selected TG account — without
  // this filter, a user with multiple accounts would see contacts from
  // OTHER accounts in the cherry-pick list, even though the broadcast can
  // only target the chosen tg_account_id (server would silently drop them
  // anyway). Memoized so spreading the two arrays doesn't re-run filters
  // on every keystroke / WS event.
  const privateContacts = useMemo(() => {
    const inAccount = (c: Contact) => !selectedAccount || c.tg_account_id === selectedAccount;
    const nonArchived = contacts.filter((c) => c.chat_type === "private" && !c.is_archived && inAccount(c));
    if (!includeArchived) return nonArchived;
    const acctArchived = selectedAccount ? (archivedByAccount[selectedAccount] || []) : [];
    const archived = acctArchived.filter((c) => c.chat_type === "private" && c.is_archived && inAccount(c));
    return [...nonArchived, ...archived];
    // archivedByAccount keyed on selectedAccount — both must be deps so a
    // freshly-fetched account's archived list is picked up.
  }, [contacts, archivedByAccount, includeArchived, selectedAccount]);
  const filteredManualContacts = useMemo(() => privateContacts.filter((c) =>
    !contactSearch || c.alias.toLowerCase().includes(contactSearch.toLowerCase())
  ), [privateContacts, contactSearch]);
  // Tag-matching pool for the cherry-pick block. Pre-computing here
  // (instead of in the JSX IIFE) keeps the filter from re-running on
  // every keystroke / WS event when neither tags nor the contact pool
  // changed. Dropped from work when not in tags mode.
  const matchingContacts = useMemo(() => {
    if (recipientMode !== "tags" || selectedTags.length === 0) return [];
    return privateContacts.filter((c) =>
      c.tags?.some((t) => selectedTags.includes(t)) &&
      !c.tags?.some((t) => excludedTags.includes(t))
    );
  }, [privateContacts, recipientMode, selectedTags, excludedTags]);

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
                {/* Show project display_name when set, fallback to phone.
                    Phone is appended in parens so users with multiple accounts
                    under the same project label can still tell them apart. */}
                {accounts.map((acc) => (
                  <option key={acc.id} value={acc.id}>
                    {acc.display_name ? `${acc.display_name} (${acc.phone})` : acc.phone}
                  </option>
                ))}
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

          {/* Archive opt-in. Off by default keeps the historical behavior
              (archived chats are skipped). Tags exist on contacts regardless
              of archive state, so flipping this on lets a tag whose holders
              all sit in archive still be reached. */}
          <div>
            <label className="flex items-center gap-2 text-sm text-slate-400 font-medium cursor-pointer select-none">
              <input
                type="checkbox"
                checked={includeArchived}
                onChange={(e) => setIncludeArchived(e.target.checked)}
                className="accent-brand"
              />
              Включая архивные контакты
              {archivedLoading && <span className="text-[10px] text-slate-500">загрузка...</span>}
            </label>
            <p className="text-[10px] text-slate-500 mt-0.5 ml-6">
              Если выключено — архивные диалоги пропускаются, даже если у них есть нужный тег.
            </p>
          </div>

          {/* Tag INCLUDE filter (for tags & random modes) */}
          {(recipientMode === "tags" || recipientMode === "random") && tags.length > 0 && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">
                Фильтр по тегам {recipientMode === "tags" ? "(пусто = все)" : ""}
              </label>
              <div className="flex flex-wrap gap-1.5">
                {tags.map((tag) => {
                  const isIncluded = selectedTags.includes(tag.name);
                  return (
                    <button key={tag.id}
                      onClick={() => {
                        // Clicking a tag for INCLUDE auto-pulls it out of EXCLUDE
                        // so the two lists stay disjoint.
                        setExcludedTags((prev) => prev.filter((t) => t !== tag.name));
                        setSelectedTags((prev) =>
                          prev.includes(tag.name) ? prev.filter((t) => t !== tag.name) : [...prev, tag.name]
                        );
                      }}
                      className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-all ${
                        isIncluded
                          ? "border-transparent shadow-sm"
                          : "border-surface-border opacity-50 hover:opacity-80"
                      }`}
                      style={{ backgroundColor: tag.color + "25", color: tag.color, borderColor: isIncluded ? tag.color + "40" : undefined }}>
                      {isIncluded ? "✓ " : ""}{tag.name}
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Tag EXCLUDE filter — available in any mode except "all".
              Contacts carrying ANY of these tags are dropped at send time,
              even if they got through the include filter or manual pick. */}
          {recipientMode !== "all" && tags.length > 0 && (
            <div>
              <label className="text-sm text-slate-400 font-medium block mb-1.5">
                Исключить теги <span className="text-slate-500 font-normal">(люди с этими тегами не получат рассылку)</span>
              </label>
              <div className="flex flex-wrap gap-1.5">
                {tags.map((tag) => {
                  const isExcluded = excludedTags.includes(tag.name);
                  return (
                    <button key={tag.id}
                      onClick={() => {
                        // Clicking a tag for EXCLUDE auto-pulls it from INCLUDE.
                        setSelectedTags((prev) => prev.filter((t) => t !== tag.name));
                        setExcludedTags((prev) =>
                          prev.includes(tag.name) ? prev.filter((t) => t !== tag.name) : [...prev, tag.name]
                        );
                      }}
                      className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-all ${
                        isExcluded
                          ? "border-red-500/40 bg-red-500/10 text-red-400"
                          : "border-surface-border text-slate-400 opacity-70 hover:opacity-100"
                      }`}>
                      {isExcluded ? "✗ " : ""}{tag.name}
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Cherry-pick toggle — in tags mode, lets the user pick individual
              contacts out of the tag-matching set instead of sending to all. */}
          {recipientMode === "tags" && selectedTags.length > 0 && (() => {
            const filtered = matchingContacts.filter((c) =>
              !contactSearch || c.alias.toLowerCase().includes(contactSearch.toLowerCase())
            );
            return (
              <div>
                <label className="flex items-center gap-2 text-sm text-slate-400 font-medium mb-1.5 cursor-pointer select-none">
                  <input type="checkbox" checked={cherryPick}
                    onChange={(e) => setCherryPick(e.target.checked)}
                    className="accent-brand" />
                  Выбрать выборочно из этих контактов ({matchingContacts.length} подходит)
                </label>
                {cherryPick && (
                  <>
                    <div className="flex items-center gap-2 mb-2">
                      <input type="text" value={contactSearch} onChange={(e) => setContactSearch(e.target.value)}
                        placeholder="Поиск контактов..."
                        className="flex-1 bg-surface border border-surface-border rounded-xl px-3 py-2 text-sm focus:outline-none focus:border-brand/50" />
                      <button type="button"
                        onClick={() => {
                          // Toggle select-all within the currently filtered view.
                          const filteredIds = filtered.map((c) => c.id);
                          const allSelected = filteredIds.every((id) => manualContacts.has(id));
                          setManualContacts((prev) => {
                            const next = new Set(prev);
                            if (allSelected) filteredIds.forEach((id) => next.delete(id));
                            else filteredIds.forEach((id) => next.add(id));
                            return next;
                          });
                        }}
                        className="text-[11px] px-2 py-1 rounded-lg border border-surface-border text-slate-400 hover:border-slate-500 whitespace-nowrap">
                        {filtered.every((c) => manualContacts.has(c.id)) && filtered.length > 0 ? "Снять всё" : "Выбрать всё"}
                      </button>
                    </div>
                    <div className="max-h-48 overflow-y-auto space-y-1 border border-surface-border rounded-xl p-2">
                      {filtered.map((c) => (
                        <label key={c.id} className="flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-surface-hover cursor-pointer">
                          <input type="checkbox" checked={manualContacts.has(c.id)}
                            onChange={() => setManualContacts((prev) => {
                              const next = new Set(prev);
                              next.has(c.id) ? next.delete(c.id) : next.add(c.id);
                              return next;
                            })}
                            className="accent-brand" />
                          <span className="text-sm">{c.alias}</span>
                          {c.is_archived && (
                            <span className="text-[10px] px-1 py-0.5 rounded bg-amber-500/10 border border-amber-500/30 text-amber-400">архив</span>
                          )}
                          {c.tags?.length > 0 && (
                            <span className="text-[10px] text-slate-500">{c.tags.join(", ")}</span>
                          )}
                        </label>
                      ))}
                      {filtered.length === 0 && (
                        <p className="text-xs text-slate-500 text-center py-2">Нет подходящих контактов</p>
                      )}
                    </div>
                    <p className="text-[10px] text-slate-500 mt-1">Выбрано: {manualContacts.size}</p>
                  </>
                )}
              </div>
            );
          })()}

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
                    {c.is_archived && (
                      <span className="text-[10px] px-1 py-0.5 rounded bg-amber-500/10 border border-amber-500/30 text-amber-400">архив</span>
                    )}
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

          {/* Delay slider — minimum raised to 5s (anti-flood floor). */}
          <div>
            <label className="text-sm text-slate-400 font-medium block mb-1.5">
              Задержка: {delay >= 60 ? `${Math.floor(delay / 60)} мин ${delay % 60 ? delay % 60 + " сек" : ""}` : `${delay} сек`}
            </label>
            <input type="range" min={BROADCAST_MIN_DELAY} max={3600} value={Math.max(BROADCAST_MIN_DELAY, delay)}
              onChange={(e) => setDelay(Number(e.target.value))} className="w-full accent-brand" />
            <div className="flex justify-between text-[10px] text-slate-500 mt-1">
              <span>{BROADCAST_MIN_DELAY} сек</span><span>60 мин</span>
            </div>
            <p className="text-[10px] text-slate-500 mt-1">
              Минимум 5 секунд — чтобы Telegram не словил flood-ban на аккаунт.
            </p>
          </div>

          <div className="flex gap-2 justify-end">
            <Button variant="ghost" onClick={() => {
              setShowCreate(false); setEditingBroadcast(null);
              setTitle(""); setContent("");
              setSelectedTags([]); setExcludedTags([]); setManualContacts(new Set());
              setCherryPick(false); setIncludeArchived(false); setRecipientMode("all");
              setDelay(BROADCAST_MIN_DELAY); setMediaFile(null); setSendAs("auto"); setUploadedMedia(null);
            }}>Отмена</Button>
            <Button onClick={editingBroadcast ? handleSaveEdit : handleCreate} disabled={creating || !title.trim() || !selectedAccount ||
              (recipientMode === "manual" && manualContacts.size === 0) ||
              (recipientMode === "tags" && cherryPick && manualContacts.size === 0)}>
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
              {/* Account + author meta — resolved server-side via TgAccount /
                  Staff joins. Either label may be missing on legacy rows
                  (older drafts before the API extension); UI degrades to "—"
                  rather than hiding the row entirely so the operator always
                  knows who/where the broadcast belongs to. */}
              {(bc.tg_account_phone || bc.tg_account_display_name || bc.created_by_name) && (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500 mb-1">
                  <span className="inline-flex items-center gap-1">
                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/>
                    </svg>
                    {bc.tg_account_display_name || bc.tg_account_phone || "—"}
                  </span>
                  <span className="inline-flex items-center gap-1">
                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>
                      <circle cx="12" cy="7" r="4"/>
                    </svg>
                    {bc.created_by_name || "—"}
                  </span>
                </div>
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
              {(bc.tag_filter.length > 0 || (bc.tag_exclude?.length ?? 0) > 0 || bc.include_archived) && (
                <div className="flex gap-1 mt-2 flex-wrap">
                  {bc.tag_filter.map((t) => (
                    <span key={`inc-${t}`} className="text-[10px] px-1.5 py-0.5 rounded bg-surface-hover text-slate-400">
                      {t}
                    </span>
                  ))}
                  {(bc.tag_exclude || []).map((t) => (
                    <span key={`exc-${t}`} className="text-[10px] px-1.5 py-0.5 rounded bg-red-500/10 border border-red-500/20 text-red-400">
                      ✗ {t}
                    </span>
                  ))}
                  {bc.include_archived && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-500/10 border border-amber-500/30 text-amber-400">
                      📦 +архив
                    </span>
                  )}
                </div>
              )}
              {bc.last_error && (
                <div className={`mt-2 px-2 py-1.5 rounded text-[11px] border ${
                  bc.status === "failed"
                    ? "bg-red-500/10 border-red-500/30 text-red-300"
                    : "bg-amber-500/10 border-amber-500/30 text-amber-300"
                }`}>
                  <div className="font-medium mb-0.5">
                    {bc.status === "failed" ? "Рассылка остановлена с ошибкой:" : "Последняя ошибка отправки:"}
                  </div>
                  <div className="break-words">{bc.last_error}</div>
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

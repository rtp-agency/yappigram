"use client";

import { useEffect, useRef, useState } from "react";
import {
  api,
  archiveChat,
  connectWS,
  createGroup,
  deleteMessageInTg,
  editMessageInTg,
  forwardMessages,
  getRole,
  mediaUrl,
  onWSEvent,
  parseInlineButtons,
  pressInlineButton,
  translateText,
  unarchiveChat,
  uploadMedia,
  type Contact,
  type Message,
  type Tag,
  type TgAccount,
} from "@/lib";
import { AppShell, AuthGuard, Badge, Button } from "@/components";

export default function ChatsPage() {
  return (
    <AuthGuard>
      <AppShell>
        <ChatsContent />
      </AppShell>
    </AuthGuard>
  );
}

function ChatsContent() {
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [selected, setSelected] = useState<Contact | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [text, setText] = useState("");
  const [search, setSearch] = useState("");
  const [editingAlias, setEditingAlias] = useState(false);
  const [aliasValue, setAliasValue] = useState("");
  const [allTags, setAllTags] = useState<Tag[]>([]);
  const [showTags, setShowTags] = useState(false);

  // Reply state
  const [replyTo, setReplyTo] = useState<Message | null>(null);

  // Context menu for message actions (edit/delete/reply)
  const [contextMenu, setContextMenu] = useState<{ msg: Message; x: number; y: number } | null>(null);
  const [editingMsg, setEditingMsg] = useState<Message | null>(null);
  const [editText, setEditText] = useState("");

  // Forward state
  const [forwardMode, setForwardMode] = useState(false);
  const [forwardSelected, setForwardSelected] = useState<Set<string>>(new Set());
  const [showForwardPicker, setShowForwardPicker] = useState(false);

  // Bot callback toast
  const [botToast, setBotToast] = useState<string | null>(null);

  // Create group
  const [showCreateGroup, setShowCreateGroup] = useState(false);
  const [groupTitle, setGroupTitle] = useState("");
  const [tgAccounts, setTgAccounts] = useState<TgAccount[]>([]);
  const [selectedAccount, setSelectedAccount] = useState("");
  const [creatingGroup, setCreatingGroup] = useState(false);
  const [selectedMembers, setSelectedMembers] = useState<Set<string>>(new Set());
  const isAdmin = ["super_admin", "admin"].includes(getRole() || "");

  // Add member to group
  const [showAddMember, setShowAddMember] = useState(false);
  const [addingMember, setAddingMember] = useState(false);

  // Unread tracking: contact_id -> count
  const [unread, setUnread] = useState<Map<string, number>>(new Map());
  const [notification, setNotification] = useState<{ alias: string; text: string } | null>(null);

  // Pinned chats (per-user)
  const [pinned, setPinned] = useState<Set<string>>(new Set());

  // Scroll-to-bottom tracking
  const [showScrollBtn, setShowScrollBtn] = useState(false);
  const messagesContainerRef = useRef<HTMLDivElement>(null);

  // Fullscreen photo viewer
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  // Archive
  const [showArchived, setShowArchived] = useState(false);

  // Translation
  const LANG_OPTIONS = [
    { value: "", label: "Off" },
    { value: "ru", label: "RU" },
    { value: "en", label: "EN" },
    { value: "es", label: "ES" },
    { value: "de", label: "DE" },
    { value: "fr", label: "FR" },
    { value: "zh-CN", label: "ZH" },
    { value: "ar", label: "AR" },
    { value: "pt", label: "PT" },
    { value: "ja", label: "JA" },
    { value: "ko", label: "KO" },
    { value: "uk", label: "UK" },
    { value: "tr", label: "TR" },
  ];
  const [incomingLang, setIncomingLang] = useState("");
  const [outgoingLang, setOutgoingLang] = useState("");
  const [translations, setTranslations] = useState<Map<string, string>>(new Map());

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const selectedRef = useRef<Contact | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => { api("/api/pinned").then((ids: string[]) => setPinned(new Set(ids))).catch(console.error); }, []);
  useEffect(() => {
    api("/api/unread").then((data: Record<string, number>) => {
      setUnread(new Map(Object.entries(data)));
    }).catch(console.error);
  }, []);
  useEffect(() => { selectedRef.current = selected; }, [selected]);
  useEffect(() => { api("/api/tags").then(setAllTags).catch(console.error); }, []);

  // Load contacts when archive toggle changes
  useEffect(() => {
    const params = showArchived ? "status=approved&archived=true" : "status=approved";
    api(`/api/contacts?${params}`).then((data: Contact[]) =>
      setContacts(data.sort((a, b) => (b.last_message_at || "").localeCompare(a.last_message_at || "")))
    ).catch(console.error);
  }, [showArchived]);

  useEffect(() => {
    connectWS();

    const unsub = onWSEvent((event) => {
      if (event.type === "new_message") {
        const isCurrentChat = selectedRef.current?.id === event.contact_id;
        setContacts((prev) =>
          prev
            .map((c) => c.id === event.contact_id ? { ...c, last_message_at: new Date().toISOString() } : c)
            .sort((a, b) => (b.last_message_at || "").localeCompare(a.last_message_at || ""))
        );
        if (isCurrentChat) {
          setMessages((prev) => {
            if (prev.some((m) => m.id === event.message.id)) return prev;
            return [...prev, event.message];
          });
          // Mark as read immediately since user is viewing this chat
          api(`/api/messages/${event.contact_id}/read`, { method: "PATCH" }).catch(console.error);
        } else {
          // Mark as unread + increment count
          setUnread((prev) => {
            const next = new Map(prev);
            next.set(event.contact_id, (next.get(event.contact_id) || 0) + 1);
            return next;
          });
          // Find contact alias for notification
          setContacts((prev) => {
            const contact = prev.find((c) => c.id === event.contact_id);
            if (contact && event.message?.content) {
              setNotification({ alias: contact.alias, text: event.message.content.slice(0, 80) });
              setTimeout(() => setNotification(null), 3000);
            }
            return prev;
          });
        }
      }
      if (event.type === "message_edited") {
        setMessages((prev) =>
          prev.map((m) => m.id === event.message_id
            ? { ...m, content: event.content, inline_buttons: event.inline_buttons, is_edited: true }
            : m
          )
        );
      }
      if (event.type === "message_deleted") {
        setMessages((prev) =>
          prev.map((m) => m.id === event.message_id ? { ...m, is_deleted: true } : m)
        );
      }
      if (event.type === "contact_deleted") {
        setContacts((prev) => prev.filter((c) => c.id !== event.contact_id));
      }
    });

    return unsub;
  }, []);

  const selectedId = selected?.id ?? null;

  useEffect(() => {
    if (!selectedId) return;
    api(`/api/messages/${selectedId}`).then(setMessages).catch(console.error);
    setReplyTo(null);
    setForwardMode(false);
    setForwardSelected(new Set());
    // Clear unread for this chat — persist to DB
    setUnread((prev) => { const n = new Map(prev); n.delete(selectedId); return n; });
    api(`/api/messages/${selectedId}/read`, { method: "PATCH" }).catch(console.error);
  }, [selectedId]);

  useEffect(() => {
    if (!selectedId) return;
    const interval = setInterval(() => {
      api(`/api/messages/${selectedId}`).then((msgs: Message[]) => {
        setMessages((prev) => {
          if (msgs.length !== prev.length || JSON.stringify(msgs.map(m => m.is_deleted)) !== JSON.stringify(prev.map(m => m.is_deleted))) return msgs;
          return prev;
        });
      }).catch(() => {});
    }, 3000);
    return () => clearInterval(interval);
  }, [selectedId]);

  const justOpenedChat = useRef(false);

  // When selecting a new chat, flag so first message load scrolls to bottom
  useEffect(() => {
    if (selectedId) justOpenedChat.current = true;
  }, [selectedId]);

  useEffect(() => {
    const container = messagesContainerRef.current;
    if (!container) return;
    // Always scroll to bottom when chat first opens
    if (justOpenedChat.current) {
      justOpenedChat.current = false;
      messagesEndRef.current?.scrollIntoView({ behavior: "instant" });
      return;
    }
    // Auto-scroll only if user is near the bottom (within 150px)
    const isNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 150;
    if (isNearBottom) {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  // Clear translations when language changes
  useEffect(() => {
    setTranslations(new Map());
  }, [incomingLang]);

  // Auto-hide bot toast
  useEffect(() => {
    if (!botToast) return;
    const t = setTimeout(() => setBotToast(null), 4000);
    return () => clearTimeout(t);
  }, [botToast]);

  const handleEditMessage = async () => {
    if (!editingMsg || !editText.trim() || !selected) return;
    try {
      await editMessageInTg(selected.id, editingMsg.id, editText.trim());
      setMessages((prev) => prev.map((m) => m.id === editingMsg.id ? { ...m, content: editText.trim(), is_edited: true } : m));
      setEditingMsg(null);
      setEditText("");
    } catch (e: any) { alert(e.message); }
  };

  const handleDeleteMessage = async (msg: Message) => {
    if (!selected || !confirm("Delete this message?")) return;
    try {
      await deleteMessageInTg(selected.id, msg.id);
      setMessages((prev) => prev.map((m) => m.id === msg.id ? { ...m, is_deleted: true } : m));
    } catch (e: any) { alert(e.message); }
  };

  const sendingRef = useRef(false);
  const sendMessage = async () => {
    const content = text.trim();
    if (!content || !selected || sendingRef.current) return;
    sendingRef.current = true;
    // Clear input immediately for snappy UX
    setText("");
    setReplyTo(null);
    if (inputRef.current) inputRef.current.style.height = "auto";
    try {
      const body: any = { content };
      if (replyTo) body.reply_to_msg_id = replyTo.id;
      if (outgoingLang) body.translate_to = outgoingLang;

      const msg = await api(`/api/messages/${selected.id}/send`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      setMessages((prev) => {
        if (prev.some((m) => m.id === msg.id)) return prev;
        return [...prev, msg];
      });
    } catch (e: any) { alert(e.message); } finally { sendingRef.current = false; }
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !selected) return;
    try {
      const msg = await uploadMedia(selected.id, file, text.trim() || undefined);
      setMessages((prev) => [...prev, msg]);
      setText("");
    } catch (err: any) { alert(err.message); }
    e.target.value = "";
  };

  const renameContact = async () => {
    if (!selected || !aliasValue.trim()) return;
    try {
      const updated = await api(`/api/contacts/${selected.id}`, {
        method: "PATCH",
        body: JSON.stringify({ alias: aliasValue.trim() }),
      });
      setSelected(updated);
      setContacts((prev) => prev.map((c) => (c.id === updated.id ? updated : c)));
      setEditingAlias(false);
    } catch (e: any) { alert(e.message); }
  };

  const toggleTag = async (tagName: string) => {
    if (!selected) return;
    const has = selected.tags.includes(tagName);
    const newTags = has ? selected.tags.filter((t) => t !== tagName) : [...selected.tags, tagName];
    try {
      const updated = await api(`/api/contacts/${selected.id}`, {
        method: "PATCH",
        body: JSON.stringify({ tags: newTags }),
      });
      setSelected(updated);
      setContacts((prev) => prev.map((c) => (c.id === updated.id ? updated : c)));
    } catch (e: any) { alert(e.message); }
  };

  const toggleForwardSelect = (msgId: string) => {
    setForwardSelected((prev) => {
      const next = new Set(prev);
      if (next.has(msgId)) next.delete(msgId); else next.add(msgId);
      return next;
    });
  };

  const doForward = async (toContactId: string) => {
    if (!selected || forwardSelected.size === 0) return;
    try {
      await forwardMessages(selected.id, Array.from(forwardSelected), toContactId);
      setForwardMode(false);
      setForwardSelected(new Set());
      setShowForwardPicker(false);
    } catch (e: any) { alert(e.message); }
  };

  const handlePressButton = async (msgId: string, callbackData: string) => {
    if (!selected) return;
    try {
      const res = await pressInlineButton(selected.id, msgId, callbackData);
      if (res.response) setBotToast(res.response);
    } catch (e: any) { alert(e.message); }
  };

  const addMember = async (memberContactId: string) => {
    if (!selected) return;
    setAddingMember(true);
    try {
      await api(`/api/contacts/${selected.id}/add-member`, {
        method: "POST",
        body: JSON.stringify({ member_contact_id: memberContactId }),
      });
      setShowAddMember(false);
    } catch (e: any) { alert(e.message); }
    finally { setAddingMember(false); }
  };

  const deleteContact = async (contactId: string) => {
    if (!confirm("Delete this chat from CRM? (Telegram chat will not be affected)")) return;
    try {
      await api(`/api/contacts/${contactId}`, { method: "DELETE" });
      setContacts((prev) => prev.filter((c) => c.id !== contactId));
      if (selected?.id === contactId) {
        setSelected(null);
        setMessages([]);
      }
    } catch (e: any) { alert(e.message); }
  };

  const togglePin = async (contactId: string) => {
    const isPinned = pinned.has(contactId);
    try {
      await api(`/api/pinned/${contactId}`, { method: isPinned ? "DELETE" : "POST" });
      setPinned((prev) => {
        const next = new Set(prev);
        isPinned ? next.delete(contactId) : next.add(contactId);
        return next;
      });
    } catch (e: any) { console.error(e); }
  };

  const filteredContacts = contacts
    .filter((c) => c.alias.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      const ap = pinned.has(a.id) ? 1 : 0;
      const bp = pinned.has(b.id) ? 1 : 0;
      if (ap !== bp) return bp - ap;
      return (b.last_message_at || "").localeCompare(a.last_message_at || "");
    });

  const isGroup = selected?.chat_type === "group" || selected?.chat_type === "channel" || selected?.chat_type === "supergroup";

  return (
    <div className="flex h-full">
      {/* Contact list */}
      <div className={`w-full md:w-80 border-r border-surface-border flex flex-col bg-gradient-to-b from-surface-card/50 to-transparent ${selected ? "hidden md:flex" : ""}`}>
        <div className="p-4 border-b border-surface-border">
          <div className="flex gap-2">
            <div className="relative flex-1">
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
              </svg>
              <input
                placeholder="Search chats..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full bg-surface-card border border-surface-border rounded-xl pl-10 pr-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 focus:shadow-[0_0_12px_rgba(14,165,233,0.08)] transition-all placeholder:text-slate-600"
              />
            </div>
            {isAdmin && (
              <button
                onClick={() => {
                  setShowCreateGroup(true);
                  api("/api/tg/status").then((accs: TgAccount[]) => {
                    setTgAccounts(accs.filter(a => a.is_active));
                    if (accs.length > 0) setSelectedAccount(accs[0].id);
                  }).catch(console.error);
                }}
                className="w-10 h-10 flex items-center justify-center bg-brand/10 border border-brand/20 text-brand rounded-xl hover:bg-brand/20 transition-all shrink-0"
                title="Create group"
              >
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
                </svg>
              </button>
            )}
            <button
              onClick={() => { setShowArchived(!showArchived); setSelected(null); setMessages([]); }}
              className={`w-10 h-10 flex items-center justify-center border rounded-xl transition-all shrink-0 ${
                showArchived
                  ? "bg-yellow-500/10 border-yellow-500/30 text-yellow-400"
                  : "bg-surface-card border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
              }`}
              title={showArchived ? "Show active chats" : "Show archived"}
            >
              <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="21 8 21 21 3 21 3 8" /><rect x="1" y="3" width="22" height="5" /><line x1="10" y1="12" x2="14" y2="12" />
              </svg>
            </button>
          </div>
          {showArchived && (
            <div className="px-4 py-1.5 bg-yellow-500/5 text-yellow-400 text-xs font-medium text-center">
              Showing archived chats
            </div>
          )}
        </div>
        <div className="flex-1 overflow-auto">
          {filteredContacts.map((c) => (
            <div
              key={c.id}
              onClick={() => { setSelected(c); setShowTags(false); setEditingAlias(false); }}
              className={`px-4 py-3.5 cursor-pointer border-b border-surface-border/50 transition-all duration-150 ${
                selected?.id === c.id
                  ? "bg-brand/5 border-l-2 border-l-brand"
                  : "hover:bg-surface-hover border-l-2 border-l-transparent"
              }`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 min-w-0">
                  {/* Group/channel icon */}
                  {(c.chat_type === "group" || c.chat_type === "channel" || c.chat_type === "supergroup") && (
                    <svg className="w-4 h-4 text-slate-400 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
                      <circle cx="9" cy="7" r="4" />
                      <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
                      <path d="M16 3.13a4 4 0 0 1 0 7.75" />
                    </svg>
                  )}
                  <span className={`font-medium text-sm truncate ${unread.has(c.id) ? "text-white" : ""}`}>{c.alias}</span>
                  {unread.has(c.id) && (
                    <span className="min-w-[20px] h-5 px-1.5 rounded-full bg-brand text-white text-[11px] font-bold flex items-center justify-center shrink-0">
                      {unread.get(c.id)! > 99 ? "99+" : unread.get(c.id)}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {c.last_message_at && (
                    <span className={`text-xs ${unread.has(c.id) ? "text-brand font-medium" : "text-slate-500"}`}>
                      {new Date(c.last_message_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                    </span>
                  )}
                  <button
                      onClick={(e) => { e.stopPropagation(); togglePin(c.id); }}
                      className={`transition-colors p-1 ${pinned.has(c.id) ? "text-brand" : "text-slate-600 hover:text-brand"}`}
                      title={pinned.has(c.id) ? "Unpin" : "Pin"}
                    >
                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill={pinned.has(c.id) ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M12 17v5" /><path d="M9 2h6l-1 7h4l-7 8 1-5H8l1-10z" />
                      </svg>
                    </button>
                  <button
                      onClick={async (e) => {
                        e.stopPropagation();
                        try {
                          if (showArchived) {
                            await unarchiveChat(c.id);
                          } else {
                            await archiveChat(c.id);
                          }
                          setContacts((prev) => prev.filter((x) => x.id !== c.id));
                          if (selected?.id === c.id) { setSelected(null); setMessages([]); }
                        } catch (err: any) { alert(err.message); }
                      }}
                      className="text-slate-600 hover:text-yellow-400 transition-colors p-1"
                      title={showArchived ? "Unarchive" : "Archive"}
                    >
                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        {showArchived ? (
                          <><polyline points="21 8 21 21 3 21 3 8" /><rect x="1" y="3" width="22" height="5" /><polyline points="9 14 12 11 15 14" /></>
                        ) : (
                          <><polyline points="21 8 21 21 3 21 3 8" /><rect x="1" y="3" width="22" height="5" /><line x1="10" y1="12" x2="14" y2="12" /></>
                        )}
                      </svg>
                    </button>
                  {isAdmin && (
                    <button
                      onClick={(e) => { e.stopPropagation(); deleteContact(c.id); }}
                      className="text-slate-600 hover:text-red-400 transition-colors p-1 -mr-1"
                      title="Delete from CRM"
                    >
                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                      </svg>
                    </button>
                  )}
                </div>
              </div>
              {c.tags.length > 0 && (
                <div className="flex gap-1 mt-1.5">
                  {c.tags.map((t) => {
                    const tagInfo = allTags.find((at) => at.name === t);
                    return <Badge key={t} text={t} color={tagInfo?.color} />;
                  })}
                </div>
              )}
            </div>
          ))}
          {filteredContacts.length === 0 && (
            <div className="flex flex-col items-center justify-center mt-16 text-slate-500">
              <svg className="w-12 h-12 mb-3 text-slate-700" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
              </svg>
              <p className="text-sm">No chats found</p>
            </div>
          )}
        </div>
      </div>

      {/* Chat area */}
      <div className={`flex-1 flex flex-col ${!selected ? "hidden md:flex" : ""}`}>
        {selected ? (
          <>
            {/* Header */}
            <div className="px-4 py-3 border-b border-surface-border bg-surface-card/30 backdrop-blur-sm flex items-center gap-3">
              <button onClick={() => setSelected(null)} className="md:hidden text-slate-400 hover:text-white transition-colors p-1">
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="15 18 9 12 15 6" />
                </svg>
              </button>
              <div className="flex-1 min-w-0">
                {editingAlias ? (
                  <div className="flex items-center gap-2 animate-fade-in">
                    <input
                      value={aliasValue}
                      onChange={(e) => setAliasValue(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") renameContact();
                        if (e.key === "Escape") setEditingAlias(false);
                      }}
                      className="bg-surface-card border border-brand/30 rounded-lg px-2.5 py-1 text-sm focus:outline-none focus:border-brand/50"
                      autoFocus
                    />
                    <button onClick={renameContact} className="text-accent text-sm font-medium hover:text-accent/80 transition-colors">Save</button>
                    <button onClick={() => setEditingAlias(false)} className="text-slate-500 text-sm hover:text-slate-300 transition-colors">Cancel</button>
                  </div>
                ) : (
                  <div className="flex items-center gap-2">
                    {isGroup && (
                      <svg className="w-4 h-4 text-slate-400 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
                        <path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
                      </svg>
                    )}
                    <div
                      className="font-semibold cursor-pointer hover:text-brand transition-colors truncate"
                      onClick={() => { setAliasValue(selected.alias); setEditingAlias(true); }}
                      title="Click to rename"
                    >
                      {selected.alias}
                    </div>
                  </div>
                )}
                <div className="flex gap-1 mt-1 items-center flex-wrap">
                  {selected.tags.map((t) => {
                    const tagInfo = allTags.find((at) => at.name === t);
                    return <Badge key={t} text={t} color={tagInfo?.color} />;
                  })}
                  <button
                    onClick={() => setShowTags(!showTags)}
                    className={`w-5 h-5 flex items-center justify-center rounded-full text-xs border transition-all duration-200 ${
                      showTags
                        ? "bg-brand/20 border-brand/30 text-brand"
                        : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                    }`}
                    title="Manage tags"
                  >
                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                      <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z" strokeLinecap="round" strokeLinejoin="round" />
                      <line x1="7" y1="7" x2="7.01" y2="7" />
                    </svg>
                  </button>
                </div>
                {showTags && (
                  <div className="flex gap-1.5 mt-2 flex-wrap animate-slide-up">
                    {allTags.map((tag) => (
                      <button
                        key={tag.id}
                        onClick={() => toggleTag(tag.name)}
                        className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-all duration-200 ${
                          selected.tags.includes(tag.name)
                            ? "border-transparent shadow-sm"
                            : "border-surface-border opacity-40 hover:opacity-80"
                        }`}
                        style={{ backgroundColor: tag.color + "25", color: tag.color, borderColor: selected.tags.includes(tag.name) ? tag.color + "40" : undefined }}
                      >
                        {selected.tags.includes(tag.name) ? "- " : "+ "}{tag.name}
                      </button>
                    ))}
                    {allTags.length === 0 && (
                      <span className="text-xs text-slate-500">No tags yet. Create them in Settings.</span>
                    )}
                  </div>
                )}
              </div>

              {/* Translation selectors */}
              <div className="flex items-center gap-1.5 shrink-0">
                <div className="flex items-center gap-1">
                  <span className="text-[10px] text-slate-500">IN</span>
                  <select
                    value={incomingLang}
                    onChange={(e) => { setIncomingLang(e.target.value); setTranslations(new Map()); }}
                    className="bg-surface-card border border-surface-border rounded-lg px-1.5 py-1 text-xs focus:outline-none focus:border-brand/50 w-16"
                  >
                    {LANG_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                  </select>
                </div>
                <div className="flex items-center gap-1">
                  <span className="text-[10px] text-slate-500">OUT</span>
                  <select
                    value={outgoingLang}
                    onChange={(e) => setOutgoingLang(e.target.value)}
                    className="bg-surface-card border border-surface-border rounded-lg px-1.5 py-1 text-xs focus:outline-none focus:border-brand/50 w-16"
                  >
                    {LANG_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                  </select>
                </div>
              </div>

              {/* Add member (groups only) */}
              {isGroup && isAdmin && (
                <button
                  onClick={() => setShowAddMember(!showAddMember)}
                  className={`p-2 rounded-xl border transition-all duration-200 ${
                    showAddMember
                      ? "bg-brand/10 border-brand/30 text-brand"
                      : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                  }`}
                  title="Add member"
                >
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="8.5" cy="7" r="4" />
                    <line x1="20" y1="8" x2="20" y2="14" /><line x1="23" y1="11" x2="17" y2="11" />
                  </svg>
                </button>
              )}

              {/* Forward mode toggle */}
              <button
                onClick={() => { setForwardMode(!forwardMode); setForwardSelected(new Set()); }}
                className={`p-2 rounded-xl border transition-all duration-200 ${
                  forwardMode
                    ? "bg-brand/10 border-brand/30 text-brand"
                    : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                }`}
                title={forwardMode ? "Cancel forward" : "Forward messages"}
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" />
                  <polyline points="14 8 18 12 14 16" />
                  <line x1="10" y1="12" x2="18" y2="12" />
                </svg>
              </button>
            </div>

            {/* Add member bar */}
            {showAddMember && (
              <div className="px-4 py-2 bg-brand/5 border-b border-brand/20 animate-slide-up">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs text-brand font-medium">Add contact to group:</span>
                  <button onClick={() => setShowAddMember(false)} className="text-slate-500 hover:text-white p-1">
                    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                    </svg>
                  </button>
                </div>
                <div className="flex flex-wrap gap-1.5 max-h-32 overflow-auto">
                  {contacts.filter((c) => c.chat_type === "private").map((c) => (
                    <button
                      key={c.id}
                      onClick={() => addMember(c.id)}
                      disabled={addingMember}
                      className="px-2.5 py-1 rounded-lg text-xs border border-surface-border bg-surface-card hover:border-brand/30 hover:text-brand transition-all disabled:opacity-50"
                    >
                      {c.alias}
                    </button>
                  ))}
                  {contacts.filter((c) => c.chat_type === "private").length === 0 && (
                    <span className="text-xs text-slate-500">No private contacts</span>
                  )}
                </div>
              </div>
            )}

            {/* Forward bar */}
            {forwardMode && forwardSelected.size > 0 && (
              <div className="px-4 py-2 bg-brand/5 border-b border-brand/20 flex items-center justify-between animate-slide-up">
                <span className="text-sm text-brand">{forwardSelected.size} message(s) selected</span>
                <div className="flex gap-2">
                  <Button onClick={() => setShowForwardPicker(true)} variant="primary">
                    Forward
                  </Button>
                  <Button onClick={() => { setForwardMode(false); setForwardSelected(new Set()); }} variant="ghost">
                    Cancel
                  </Button>
                </div>
              </div>
            )}

            {/* Messages */}
            <div
              ref={messagesContainerRef}
              className="flex-1 overflow-auto overflow-x-hidden p-4 space-y-2 relative"
              onScroll={(e) => {
                const el = e.currentTarget;
                setShowScrollBtn(el.scrollHeight - el.scrollTop - el.clientHeight > 300);
              }}
            >
              {messages.map((m) => {
                const buttons = parseInlineButtons(m.inline_buttons);
                return (
                  <div key={m.id} className="flex items-start gap-2">
                    {/* Forward checkbox */}
                    {forwardMode && (
                      <label className="flex items-center pt-2 cursor-pointer shrink-0">
                        <input
                          type="checkbox"
                          checked={forwardSelected.has(m.id)}
                          onChange={() => toggleForwardSelect(m.id)}
                          className="w-4 h-4 rounded border-surface-border accent-brand"
                        />
                      </label>
                    )}

                    <div
                      className={`max-w-[75%] min-w-0 ${m.direction === "outgoing" ? "ml-auto" : ""}`}
                      onDoubleClick={() => { if (!forwardMode) { setReplyTo(m); inputRef.current?.focus(); } }}
                      onContextMenu={(e) => { e.preventDefault(); setContextMenu({ msg: m, x: e.clientX, y: e.clientY }); }}
                      onTouchStart={(e) => {
                        const timer = setTimeout(() => { setContextMenu({ msg: m, x: e.touches[0].clientX, y: e.touches[0].clientY }); }, 500);
                        (e.currentTarget as any)._lp = timer;
                      }}
                      onTouchEnd={(e) => { clearTimeout((e.currentTarget as any)._lp); }}
                      onTouchMove={(e) => { clearTimeout((e.currentTarget as any)._lp); }}
                    >
                      {/* Topic badge for forum supergroups */}
                      {m.topic_id && m.topic_id !== 1 && (
                        <div className="text-[10px] text-purple-400 font-medium mb-0.5 ml-1 flex items-center gap-1">
                          <svg className="w-2.5 h-2.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                            <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />
                          </svg>
                          {m.topic_name || `Topic #${m.topic_id}`}
                        </div>
                      )}
                      {/* Sender alias for group messages */}
                      {m.direction === "incoming" && isGroup && m.sender_alias && (
                        <div className="text-xs text-accent font-medium mb-0.5 ml-1">{m.sender_alias}</div>
                      )}

                      <div
                        id={`msg-${m.id}`}
                        className={`px-3.5 py-2.5 rounded-2xl text-sm overflow-hidden break-words ${
                          m.direction === "outgoing" ? "rounded-br-md" : "rounded-bl-md"
                        } ${
                          m.is_deleted
                            ? "bg-red-900/20 border border-red-500/30 shadow-[0_0_12px_rgba(239,68,68,0.15)] text-white"
                            : m.is_edited
                              ? m.direction === "outgoing"
                                ? "bg-gradient-to-br from-brand to-brand-dark text-white shadow-[0_2px_8px_rgba(14,165,233,0.2)] ring-1 ring-yellow-500/20"
                                : "bg-yellow-900/10 border border-yellow-500/20 text-white"
                              : m.direction === "outgoing"
                                ? "bg-gradient-to-br from-brand to-brand-dark text-white shadow-[0_2px_8px_rgba(14,165,233,0.2)]"
                                : "bg-surface-card border border-surface-border text-white"
                        }`}
                      >
                        {/* Forwarded from banner */}
                        {m.forwarded_from_alias && (
                          <div className="flex items-center gap-1.5 mb-1.5 text-xs opacity-60 italic">
                            <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <polyline points="15 17 20 12 15 7" />
                              <path d="M4 18v-2a4 4 0 0 1 4-4h12" />
                            </svg>
                            Forwarded from {m.forwarded_from_alias}
                          </div>
                        )}

                        {/* Reply quote */}
                        {m.reply_to_content_preview && (
                          <div
                            className={`mb-2 pl-2.5 border-l-2 text-xs py-1 rounded-r cursor-pointer break-words ${
                              m.direction === "outgoing"
                                ? "border-white/30 bg-white/10 text-white/70"
                                : "border-brand/40 bg-brand/5 text-slate-400"
                            }`}
                            onClick={() => {
                              if (m.reply_to_msg_id) {
                                const el = document.getElementById(`msg-${m.reply_to_msg_id}`);
                                el?.scrollIntoView({ behavior: "smooth", block: "center" });
                                el?.classList.add("ring-1", "ring-brand/40");
                                setTimeout(() => el?.classList.remove("ring-1", "ring-brand/40"), 2000);
                              }
                            }}
                          >
                            {m.reply_to_content_preview}
                          </div>
                        )}

                        {/* Deleted indicator */}
                        {m.is_deleted && (
                          <div className="flex items-center gap-1.5 mb-1 text-xs text-red-400/80">
                            <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
                            </svg>
                            Deleted in Telegram
                          </div>
                        )}

                        {/* Media */}
                        {m.media_type && m.media_path && (
                          <div className="mb-2">
                            {m.media_type === "photo" && (
                              <img
                                src={mediaUrl(m.media_path)}
                                alt=""
                                className="rounded-xl max-w-full max-h-64 object-cover cursor-pointer hover:opacity-90 transition-opacity"
                                onClick={(e) => { e.stopPropagation(); setLightboxSrc(mediaUrl(m.media_path!)); }}
                              />
                            )}
                            {m.media_type === "video" && (
                              <video src={mediaUrl(m.media_path)} controls className="rounded-xl max-w-full max-h-64" />
                            )}
                            {m.media_type === "voice" && (
                              <audio src={mediaUrl(m.media_path)} controls className="w-full" />
                            )}
                            {m.media_type === "document" && (() => {
                              const ext = m.media_path!.split('.').pop()?.toLowerCase() || '';
                              const isImage = ['jpg','jpeg','png','gif','webp','bmp','svg'].includes(ext);
                              return isImage ? (
                                <img
                                  src={mediaUrl(m.media_path!)}
                                  alt=""
                                  className="rounded-xl max-w-full max-h-64 object-cover cursor-pointer hover:opacity-90 transition-opacity"
                                  onClick={(e) => { e.stopPropagation(); setLightboxSrc(mediaUrl(m.media_path!)); }}
                                />
                              ) : (
                                <a href={mediaUrl(m.media_path)} target="_blank" rel="noreferrer" className="flex items-center gap-2 text-brand-light hover:underline">
                                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
                                    <polyline points="14 2 14 8 20 8" />
                                  </svg>
                                  {m.media_path!.split('/').pop() || 'Download file'}
                                </a>
                              );
                            })()}
                          </div>
                        )}

                        {/* Content */}
                        {m.content && <span className={`break-words whitespace-pre-wrap [overflow-wrap:anywhere] ${m.is_deleted ? "line-through" : ""}`}>{m.content}</span>}

                        {/* Translate button + translated text for incoming */}
                        {m.direction === "incoming" && m.content && incomingLang && (
                          translations.has(m.id) ? (
                            <div className="mt-1 pt-1 border-t border-slate-600/30 text-xs text-sky-300/80 break-words whitespace-pre-wrap [overflow-wrap:anywhere]">
                              {translations.get(m.id)}
                            </div>
                          ) : (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                translateText(m.content!, incomingLang).then((t) => setTranslations((prev) => new Map(prev).set(m.id, t))).catch(() => {});
                              }}
                              className="mt-1 text-[10px] text-sky-400/70 hover:text-sky-300 transition-colors"
                            >
                              Translate
                            </button>
                          )
                        )}

                        {/* Timestamp + edited + read status */}
                        <div className={`flex items-center justify-end gap-1 text-[10px] mt-1 ${m.direction === "outgoing" ? "text-white/40" : "text-slate-500"}`}>
                          {m.is_edited && <span className="italic mr-1">edited</span>}
                          {new Date(m.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                          {m.direction === "outgoing" && (
                            <svg className={`w-3.5 h-3.5 ${m.is_read ? "text-sky-300" : ""}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                              {m.is_read ? (
                                <>
                                  <polyline points="1 12 5 16 12 6" />
                                  <polyline points="8 12 12 16 20 6" />
                                </>
                              ) : (
                                <polyline points="4 12 9 17 20 6" />
                              )}
                            </svg>
                          )}
                        </div>

                        {/* Inline bot buttons */}
                        {buttons.length > 0 && (
                          <div className="mt-2 space-y-1">
                            {buttons.map((row, ri) => (
                              <div key={ri} className="flex gap-1">
                                {row.map((btn, bi) => (
                                  <button
                                    key={bi}
                                    onClick={() => {
                                      if (btn.url) {
                                        window.open(btn.url, "_blank");
                                      } else if (btn.callback_data) {
                                        handlePressButton(m.id, btn.callback_data);
                                      }
                                    }}
                                    className="flex-1 px-2 py-1.5 text-xs font-medium rounded-lg bg-brand/10 border border-brand/20 text-brand hover:bg-brand/20 transition-all min-h-[36px]"
                                  >
                                    {btn.text}
                                  </button>
                                ))}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
              <div ref={messagesEndRef} />
              {/* Scroll to bottom button */}
              {showScrollBtn && (
                <button
                  onClick={() => messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })}
                  className="sticky bottom-2 left-1/2 -translate-x-1/2 w-10 h-10 bg-surface-card border border-surface-border rounded-full flex items-center justify-center shadow-lg hover:bg-surface-hover transition-all z-10"
                >
                  <svg className="w-5 h-5 text-slate-300" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="6 9 12 15 18 9" />
                  </svg>
                </button>
              )}
            </div>

            {/* Reply strip */}
            {replyTo && (
              <div className="px-4 py-2 bg-brand/5 border-t border-brand/20 flex items-center gap-3 animate-slide-up">
                <div className="w-1 h-8 bg-brand rounded-full shrink-0" />
                <div className="flex-1 min-w-0">
                  <div className="text-xs text-brand font-medium">
                    Reply to {replyTo.direction === "outgoing" ? "yourself" : (isGroup && replyTo.sender_alias ? replyTo.sender_alias : selected.alias)}
                  </div>
                  <div className="text-xs text-slate-400 truncate">
                    {replyTo.content || (replyTo.media_type ? `[${replyTo.media_type}]` : "...")}
                  </div>
                </div>
                <button
                  onClick={() => setReplyTo(null)}
                  className="text-slate-500 hover:text-white transition-colors p-1"
                >
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              </div>
            )}

            {/* Input */}
            <div className="p-3 border-t border-surface-border bg-surface-card/30 backdrop-blur-sm">
              <div className="flex gap-2 items-center bg-surface-card border border-surface-border rounded-2xl px-2">
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.zip"
                  onChange={handleFileUpload}
                  className="hidden"
                />
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="text-slate-500 hover:text-brand transition-colors p-2"
                  title="Attach file"
                >
                  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48" />
                  </svg>
                </button>
                {outgoingLang && (
                  <button
                    onClick={async () => {
                      if (!text.trim()) return;
                      try {
                        const t = await translateText(text.trim(), outgoingLang);
                        setText(t);
                      } catch (e: any) { alert(e.message); }
                    }}
                    className="text-sky-400 hover:text-sky-300 transition-colors p-2"
                    title={`Translate to ${outgoingLang.toUpperCase()}`}
                  >
                    <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M5 8l6 6M4 14l6-6 2-3M2 5h12M7 2h1M22 22l-5-10-5 10M14 18h6" />
                    </svg>
                  </button>
                )}
                <textarea
                  ref={inputRef}
                  value={text}
                  onChange={(e) => setText(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      sendMessage();
                    }
                  }}
                  placeholder="Type a message..."
                  rows={1}
                  className="flex-1 bg-transparent py-3 text-sm focus:outline-none placeholder:text-slate-600 resize-none max-h-32 overflow-y-auto"
                  style={{ height: "auto" }}
                  onInput={(e) => {
                    const target = e.target as HTMLTextAreaElement;
                    target.style.height = "auto";
                    target.style.height = Math.min(target.scrollHeight, 128) + "px";
                  }}
                />
                <button
                  onClick={sendMessage}
                  disabled={!text.trim()}
                  className="text-brand hover:text-brand-light disabled:text-slate-600 transition-colors p-2"
                >
                  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
                  </svg>
                </button>
              </div>
            </div>
          </>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-slate-500 animate-fade-in">
            <svg className="w-16 h-16 mb-4 text-slate-700" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
            </svg>
            <p className="text-sm font-medium">Select a chat to start messaging</p>
          </div>
        )}
      </div>

      {/* Forward contact picker modal */}
      {showForwardPicker && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => setShowForwardPicker(false)}>
          <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-sm mx-4 max-h-[60vh] flex flex-col animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <div className="p-4 border-b border-surface-border">
              <h3 className="font-semibold">Forward to...</h3>
            </div>
            <div className="flex-1 overflow-auto">
              {contacts.filter((c) => c.id !== selected?.id && c.status === "approved").map((c) => (
                <button
                  key={c.id}
                  onClick={() => doForward(c.id)}
                  className="w-full text-left px-4 py-3 hover:bg-surface-hover transition-colors border-b border-surface-border/50 flex items-center gap-2"
                >
                  {(c.chat_type === "group" || c.chat_type === "channel" || c.chat_type === "supergroup") && (
                    <svg className="w-4 h-4 text-slate-400 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
                      <path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
                    </svg>
                  )}
                  <span className="text-sm font-medium">{c.alias}</span>
                </button>
              ))}
            </div>
            <div className="p-3 border-t border-surface-border">
              <Button onClick={() => setShowForwardPicker(false)} variant="ghost" className="w-full">
                Cancel
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Create group modal */}
      {showCreateGroup && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => setShowCreateGroup(false)}>
          <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-sm mx-4 p-5 animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <h3 className="text-lg font-semibold mb-4">Create Group</h3>
            <input
              placeholder="Group name..."
              value={groupTitle}
              onChange={(e) => setGroupTitle(e.target.value)}
              className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 mb-3"
            />
            {tgAccounts.length > 0 && (
              <select
                value={selectedAccount}
                onChange={(e) => setSelectedAccount(e.target.value)}
                className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 mb-3"
              >
                {tgAccounts.map((acc) => (
                  <option key={acc.id} value={acc.id}>{acc.phone}</option>
                ))}
              </select>
            )}
            <div className="mb-4">
              <p className="text-xs text-slate-400 mb-2">Add contacts to group:</p>
              <div className="flex flex-wrap gap-1.5 max-h-40 overflow-auto">
                {contacts.filter((c) => c.chat_type === "private").map((c) => (
                  <button
                    key={c.id}
                    onClick={() => setSelectedMembers((prev) => {
                      const next = new Set(prev);
                      next.has(c.id) ? next.delete(c.id) : next.add(c.id);
                      return next;
                    })}
                    className={`px-2.5 py-1 rounded-lg text-xs border transition-all ${
                      selectedMembers.has(c.id)
                        ? "bg-brand/10 border-brand/30 text-brand"
                        : "border-surface-border bg-surface-card text-slate-400 hover:border-slate-600"
                    }`}
                  >
                    {c.alias}
                  </button>
                ))}
                {contacts.filter((c) => c.chat_type === "private").length === 0 && (
                  <span className="text-xs text-slate-500">No private contacts</span>
                )}
              </div>
              {selectedMembers.size > 0 && (
                <p className="text-xs text-brand mt-1.5">{selectedMembers.size} selected</p>
              )}
            </div>
            <div className="flex gap-2 justify-end">
              <Button variant="ghost" onClick={() => { setShowCreateGroup(false); setSelectedMembers(new Set()); }}>Cancel</Button>
              <Button
                disabled={!groupTitle.trim() || !selectedAccount || creatingGroup}
                onClick={async () => {
                  setCreatingGroup(true);
                  try {
                    const newContact = await createGroup(groupTitle.trim(), selectedAccount, Array.from(selectedMembers));
                    setContacts((prev) => [newContact, ...prev]);
                    setShowCreateGroup(false);
                    setGroupTitle("");
                    setSelectedMembers(new Set());
                    setSelected(newContact);
                  } catch (e: any) { alert(e.message); }
                  setCreatingGroup(false);
                }}
              >
                {creatingGroup ? "Creating..." : "Create"}
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* New message notification */}
      {notification && (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 bg-surface-card border border-brand/30 rounded-2xl px-4 py-3 shadow-lg animate-slide-up z-50 max-w-xs w-[90%]">
          <div className="text-xs text-brand font-medium mb-0.5">{notification.alias}</div>
          <div className="text-sm text-slate-300 truncate">{notification.text}</div>
        </div>
      )}

      {/* Bot callback toast */}
      {botToast && (
        <div className="fixed bottom-20 left-1/2 -translate-x-1/2 bg-surface-card border border-brand/30 rounded-2xl px-4 py-3 shadow-lg animate-slide-up z-50 max-w-xs">
          <div className="text-xs text-brand font-medium mb-0.5">Bot response</div>
          <div className="text-sm text-white">{botToast}</div>
        </div>
      )}

      {/* Context menu for messages */}
      {contextMenu && (
        <div
          className="fixed inset-0 z-50"
          onClick={() => setContextMenu(null)}
        >
          <div
            className="absolute bg-surface-card border border-surface-border rounded-xl shadow-xl py-1 min-w-[140px] animate-fade-in"
            style={{ left: Math.min(contextMenu.x, window.innerWidth - 160), top: Math.min(contextMenu.y, window.innerHeight - 160) }}
            onClick={(e) => e.stopPropagation()}
          >
            <button
              onClick={() => { setReplyTo(contextMenu.msg); setContextMenu(null); inputRef.current?.focus(); }}
              className="w-full px-4 py-2.5 text-sm text-left hover:bg-surface-hover transition-colors flex items-center gap-2"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="9 17 4 12 9 7"/><path d="M20 18v-2a4 4 0 00-4-4H4"/></svg>
              Reply
            </button>
            {contextMenu.msg.direction === "outgoing" && !contextMenu.msg.is_deleted && (
              <button
                onClick={() => { setEditingMsg(contextMenu.msg); setEditText(contextMenu.msg.content || ""); setContextMenu(null); }}
                className="w-full px-4 py-2.5 text-sm text-left hover:bg-surface-hover transition-colors flex items-center gap-2 text-yellow-400"
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                Edit
              </button>
            )}
            {!contextMenu.msg.is_deleted && (
              <button
                onClick={() => { handleDeleteMessage(contextMenu.msg); setContextMenu(null); }}
                className="w-full px-4 py-2.5 text-sm text-left hover:bg-surface-hover transition-colors flex items-center gap-2 text-red-400"
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
                Delete
              </button>
            )}
          </div>
        </div>
      )}

      {/* Edit message modal */}
      {editingMsg && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onClick={() => setEditingMsg(null)}>
          <div className="bg-surface-card border border-surface-border rounded-2xl p-4 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
            <h3 className="text-sm font-semibold mb-3">Edit message</h3>
            <textarea
              value={editText}
              onChange={(e) => setEditText(e.target.value)}
              className="w-full bg-surface border border-surface-border rounded-xl p-3 text-sm resize-none focus:outline-none focus:border-brand/50 min-h-[80px]"
              autoFocus
            />
            <div className="flex justify-end gap-2 mt-3">
              <button onClick={() => setEditingMsg(null)} className="px-4 py-2 text-sm text-slate-400 hover:text-white transition-colors">Cancel</button>
              <button onClick={handleEditMessage} className="px-4 py-2 text-sm bg-brand rounded-xl text-white hover:bg-brand-dark transition-colors">Save</button>
            </div>
          </div>
        </div>
      )}

      {/* Fullscreen photo lightbox */}
      {lightboxSrc && (
        <div
          className="fixed inset-0 bg-black/90 flex items-center justify-center z-[60] animate-fade-in"
          onClick={() => setLightboxSrc(null)}
        >
          <button
            onClick={() => setLightboxSrc(null)}
            className="absolute top-4 right-4 text-white/70 hover:text-white transition-colors p-2"
          >
            <svg className="w-8 h-8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
          <img
            src={lightboxSrc}
            alt=""
            className="max-w-[95vw] max-h-[90vh] object-contain rounded-lg"
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </div>
  );
}

"use client";

import { useEffect, useRef, useState } from "react";
import {
  api,
  archiveContact,
  avatarUrl,
  connectWS,
  createGroup,
  editMessage,
  fetchContacts,
  fetchEditHistory,
  fetchTemplates,
  fetchTgStatus,
  fetchUnread,
  forwardMessages,
  getRole,
  mediaUrl,
  onWSEvent,
  isKeyboardHidden,
  parseInlineButtons,
  pressInlineButton,
  translateText,
  unarchiveContact,
  uploadMedia,
  type Contact,
  type EditHistoryEntry,
  type Message,
  type Tag,
  type Template,
  type TgAccount,
  type TgStatusAccount,
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
  const [role, setRole] = useState(getRole() || "operator");
  const isAdmin = ["super_admin", "admin"].includes(role);

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

  // Archive filter
  const [showArchived, setShowArchived] = useState(false);

  // Templates
  const [templates, setTemplates] = useState<Template[]>([]);
  const [showTemplates, setShowTemplates] = useState(false);

  // Tag filter
  const [filterTag, setFilterTag] = useState<string | null>(null);

  // Emoji picker
  const [showEmoji, setShowEmoji] = useState(false);

  // Translation
  const [translating, setTranslating] = useState<string | null>(null);
  const [translations, setTranslations] = useState<Map<string, string>>(new Map());
  const [translateLangIn, setTranslateLangIn] = useState("ru");
  const [translateLangOut, setTranslateLangOut] = useState("en");
  const [translatingInput, setTranslatingInput] = useState(false);

  // Edit message
  const [editingMsg, setEditingMsg] = useState<Message | null>(null);
  const [editText, setEditText] = useState("");

  // Edit history popup
  const [editHistoryMsg, setEditHistoryMsg] = useState<{ contactId: string; messageId: string } | null>(null);
  const [editHistory, setEditHistory] = useState<EditHistoryEntry[]>([]);
  const [loadingEditHistory, setLoadingEditHistory] = useState(false);

  // Account switcher
  const [accountsList, setAccountsList] = useState<TgStatusAccount[]>([]);
  const [filterAccountId, setFilterAccountId] = useState<string | null>(() => {
    if (typeof window === "undefined") return null;
    return sessionStorage.getItem("crm_selected_account") || null;
  });

  // Forum topics
  const [topics, setTopics] = useState<{ id: number; name: string }[]>([]);
  const [activeTopic, setActiveTopic] = useState<number | null>(null);
  const [loadingTopic, setLoadingTopic] = useState(false);

  // Avatar cache
  const [avatarErrors, setAvatarErrors] = useState<Set<string>>(new Set());

  // Sending state (prevent freeze)
  const [sending, setSending] = useState(false);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const selectedRef = useRef<Contact | null>(null);
  const filterAccountRef = useRef<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => { api("/api/pinned").then((ids: string[]) => setPinned(new Set(ids))).catch(console.error); }, []);
  // Sync role from server (in case localStorage is stale)
  useEffect(() => {
    api("/api/staff/me").then((me: any) => { if (me?.role) setRole(me.role); }).catch(() => {});
  }, []);
  useEffect(() => { selectedRef.current = selected; }, [selected]);
  useEffect(() => { filterAccountRef.current = filterAccountId; }, [filterAccountId]);

  // Fetch TG accounts for switcher — default to first account if none selected
  useEffect(() => {
    fetchTgStatus().then((accs) => {
      setAccountsList(accs);
      if (accs.length > 1 && !filterAccountId) {
        const firstId = accs[0].id;
        setFilterAccountId(firstId);
        sessionStorage.setItem("crm_selected_account", firstId);
      }
    }).catch(console.error);
  }, []);

  // Account-aware data fetching
  useEffect(() => {
    const acctId = filterAccountId || undefined;
    fetchUnread(acctId).then((data) => {
      setUnread(new Map(Object.entries(data)));
    }).catch(console.error);
  }, [filterAccountId]);

  useEffect(() => {
    const acctId = filterAccountId || undefined;
    api(`/api/tags${acctId ? `?tg_account_id=${acctId}` : ""}`).then(setAllTags).catch(console.error);
  }, [filterAccountId]);

  useEffect(() => {
    const acctId = filterAccountId || undefined;
    fetchTemplates(acctId).then(setTemplates).catch(console.error);
  }, [filterAccountId]);

  // Re-fetch contacts when account filter changes
  useEffect(() => {
    const acctId = filterAccountId || undefined;
    fetchContacts("approved", acctId).then((data: Contact[]) =>
      setContacts(data.sort((a, b) => (b.last_message_at || "").localeCompare(a.last_message_at || "")))
    ).catch(console.error);
  }, [filterAccountId]);

  useEffect(() => {
    connectWS();

    const unsub = onWSEvent((event) => {
      if (event.type === "new_message") {
        const isCurrentChat = selectedRef.current?.id === event.contact_id;
        setContacts((prev) => {
          const exists = prev.some((c) => c.id === event.contact_id);
          if (!exists) {
            // New contact — fetch full contact list to get the new one
            fetchContacts("approved", filterAccountRef.current || undefined).then((data: Contact[]) =>
              setContacts(data.sort((a, b) => (b.last_message_at || "").localeCompare(a.last_message_at || "")))
            ).catch(console.error);
            return prev;
          }
          return prev
            .map((c) => c.id === event.contact_id ? { ...c, last_message_at: new Date().toISOString() } : c)
            .sort((a, b) => (b.last_message_at || "").localeCompare(a.last_message_at || ""));
        });
        if (isCurrentChat) {
          setMessages((prev) => {
            if (prev.some((m) => m.id === event.message.id || (m.tg_message_id && m.tg_message_id === event.message.tg_message_id))) return prev;
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
      if (event.type === "messages_read") {
        const readIds = new Set(event.message_ids as string[]);
        setMessages((prev) =>
          prev.map((m) => readIds.has(m.id) ? { ...m, is_read: true } : m)
        );
      }
      if (event.type === "contact_deleted") {
        setContacts((prev) => prev.filter((c) => c.id !== event.contact_id));
      }
    });

    return unsub;
  }, []);

  const [loadingMessages, setLoadingMessages] = useState(false);
  useEffect(() => {
    if (!selected) return;
    setActiveTopic(null);
    setTopics([]);
    setMessages([]);
    setLoadingMessages(true);
    api(`/api/messages/${selected.id}?limit=100`).then((msgs: Message[]) => {
      setMessages(msgs);
      setTimeout(() => messagesEndRef.current?.scrollIntoView(), 50);
    }).catch(console.error).finally(() => setLoadingMessages(false));
    // Load topics for forum supergroups
    if (selected.is_forum) {
      api(`/api/messages/${selected.id}/topics`).then(setTopics).catch(console.error);
    }
    setReplyTo(null);
    setForwardMode(false);
    setForwardSelected(new Set());
    setUnread((prev) => { const n = new Map(prev); n.delete(selected.id); return n; });
    api(`/api/messages/${selected.id}/read`, { method: "PATCH" }).catch(console.error);
  }, [selected]);

  // Reload messages when topic filter changes
  useEffect(() => {
    if (!selected) return;
    setLoadingTopic(true);
    const topicParam = activeTopic !== null ? `&topic_id=${activeTopic}` : "";
    api(`/api/messages/${selected.id}?limit=100${topicParam}`).then((msgs: Message[]) => {
      setMessages(msgs);
      setTimeout(() => messagesEndRef.current?.scrollIntoView(), 50);
    }).catch(console.error).finally(() => setLoadingTopic(false));
  }, [activeTopic]);

  useEffect(() => {
    if (!selected) return;
    const topicParam = activeTopic !== null ? `&topic_id=${activeTopic}` : "";
    const interval = setInterval(() => {
      api(`/api/messages/${selected.id}?limit=100${topicParam}`).then((msgs: Message[]) => {
        setMessages((prev) => {
          if (msgs.length !== prev.length || JSON.stringify(msgs.map(m => m.is_deleted)) !== JSON.stringify(prev.map(m => m.is_deleted))) return msgs;
          return prev;
        });
      }).catch(() => {});
    }, 3000);
    return () => clearInterval(interval);
  }, [selected, activeTopic]);

  const justOpenedChat = useRef(false);

  // When selecting a new chat, flag so first message load scrolls to bottom
  useEffect(() => {
    if (selected) justOpenedChat.current = true;
  }, [selected]);

  useEffect(() => {
    const container = messagesContainerRef.current;
    if (!container) return;
    // Always scroll to bottom when chat first opens
    if (justOpenedChat.current) {
      justOpenedChat.current = false;
      messagesEndRef.current?.scrollIntoView({ behavior: "instant" });
      return;
    }
    // Auto-scroll only if user is near the bottom (within 300px)
    const isNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 300;
    if (isNearBottom) {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  // Auto-hide bot toast
  useEffect(() => {
    if (!botToast) return;
    const t = setTimeout(() => setBotToast(null), 4000);
    return () => clearTimeout(t);
  }, [botToast]);

  const sendingRef = useRef(false);
  const sendMessage = async () => {
    const content = text.trim();
    if (!content || !selected || sendingRef.current) return;
    sendingRef.current = true;

    // Check for shortcut match
    const matchedTpl = checkShortcut(content);
    if (matchedTpl) {
      setText("");
      if (inputRef.current) inputRef.current.style.height = "auto";
      await applyTemplate(matchedTpl);
      sendingRef.current = false;
      return;
    }

    // Clear input immediately for snappy UX
    const savedText = content;
    const savedReply = replyTo;
    setText("");
    setReplyTo(null);
    setShowEmoji(false);
    setShowTemplates(false);
    if (inputRef.current) inputRef.current.style.height = "auto";
    setSending(true);
    try {
      const body: any = { content: savedText };
      if (savedReply) body.reply_to_msg_id = savedReply.id;

      const msg = await api(`/api/messages/${selected.id}/send`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      setMessages((prev) => {
        if (prev.some((m) => m.id === msg.id)) return prev;
        return [...prev, msg];
      });
    } catch (e: any) {
      // Restore text on failure
      setText(savedText);
      alert(e.message);
    } finally { sendingRef.current = false; setSending(false); }
  };

  // Apply template: set text and send media if template has it
  const applyTemplate = async (tpl: Template) => {
    setShowTemplates(false);
    if (tpl.media_path && tpl.media_type && selected) {
      // Send media from template via backend
      sendingRef.current = true;
      try {
        const msg = await api(`/api/messages/${selected.id}/send-template-media?template_id=${tpl.id}`, {
          method: "POST",
        });
        setMessages((prev) => {
          if (prev.some((m) => m.id === msg.id)) return prev;
          return [...prev, msg];
        });
      } catch (e: any) { alert(e.message); }
      sendingRef.current = false;
    } else {
      setText(tpl.content);
      inputRef.current?.focus();
    }
  };

  // Shortcut detection: when user types /shortcut and presses Enter
  const checkShortcut = (inputText: string): Template | undefined => {
    if (!inputText.startsWith("/")) return undefined;
    return templates.find((t) => t.shortcut === inputText.trim());
  };

  const handleArchive = async (contactId: string) => {
    try {
      const contact = contacts.find((c) => c.id === contactId);
      if (!contact) return;
      if (contact.is_archived) {
        await unarchiveContact(contactId);
      } else {
        await archiveContact(contactId);
      }
      setContacts((prev) => prev.map((c) => c.id === contactId ? { ...c, is_archived: !c.is_archived } : c));
      if (selected?.id === contactId) {
        setSelected(null);
        setMessages([]);
      }
    } catch (e: any) { alert(e.message); }
  };

  const handleTranslate = async (msgId: string, text: string, direction: string) => {
    setTranslating(msgId);
    try {
      const lang = direction === "incoming" ? translateLangIn : translateLangOut;
      const result = await translateText(text, lang);
      setTranslations((prev) => new Map(prev).set(msgId, result.translated));
    } catch (e: any) { alert(e.message); }
    setTranslating(null);
  };

  const handleEditMessage = async () => {
    if (!editingMsg || !selected || !editText.trim()) return;
    try {
      await editMessage(selected.id, editingMsg.id, editText.trim());
      setMessages((prev) => prev.map((m) => m.id === editingMsg.id ? { ...m, content: editText.trim(), is_edited: true } : m));
      setEditingMsg(null);
      setEditText("");
    } catch (e: any) { alert(e.message); }
  };

  const switchAccount = (accountId: string | null) => {
    setFilterAccountId(accountId);
    if (accountId) {
      sessionStorage.setItem("crm_selected_account", accountId);
    } else {
      sessionStorage.removeItem("crm_selected_account");
    }
    setSelected(null);
    setMessages([]);
  };

  const showEditHistory = async (contactId: string, messageId: string) => {
    setEditHistoryMsg({ contactId, messageId });
    setLoadingEditHistory(true);
    try {
      const history = await fetchEditHistory(contactId, messageId);
      setEditHistory(history);
    } catch (e: any) {
      setEditHistory([]);
    }
    setLoadingEditHistory(false);
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !selected || sending) return;
    setSending(true);
    try {
      const msg = await uploadMedia(selected.id, file, text.trim() || undefined);
      setMessages((prev) => [...prev, msg]);
      setText("");
    } catch (err: any) { alert(err.message); }
    setSending(false);
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
    .filter((c) => {
      // Archive filter
      if (showArchived ? !c.is_archived : c.is_archived) return false;
      // Search filter
      if (search && !c.alias.toLowerCase().includes(search.toLowerCase())) return false;
      // Tag filter
      if (filterTag && !c.tags.includes(filterTag)) return false;
      return true;
    })
    .sort((a, b) => {
      const ap = pinned.has(a.id) ? 1 : 0;
      const bp = pinned.has(b.id) ? 1 : 0;
      if (ap !== bp) return bp - ap;
      return (b.last_message_at || "").localeCompare(a.last_message_at || "");
    });

  const isGroup = selected?.chat_type === "group" || selected?.chat_type === "channel" || selected?.chat_type === "supergroup";

  return (
    <div className="flex h-full overflow-hidden">
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
                  api("/api/tg/status").then((res: any) => {
                    const accs: TgAccount[] = Array.isArray(res) ? res : (res.accounts || []);
                    setTgAccounts(accs.filter(a => a.is_active));
                    if (accs.length > 0) setSelectedAccount(accs[0].id);
                  }).catch(console.error);
                }}
                className="w-10 h-10 flex items-center justify-center bg-brand/10 border border-brand/20 text-brand rounded-xl hover:bg-brand/20 transition-all shrink-0"
                title="Создать группу"
              >
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
                </svg>
              </button>
            )}
          </div>
          {/* Account switcher */}
          {accountsList.length > 1 && (
            <div className="flex gap-1 px-4 pt-2 pb-1 overflow-x-auto flex-nowrap">
              {accountsList.map((acc) => (
                <button
                  key={acc.id}
                  onClick={() => switchAccount(acc.id)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium whitespace-nowrap transition-all ${
                    filterAccountId === acc.id
                      ? "bg-brand/15 text-brand border border-brand/30"
                      : "text-slate-400 hover:text-slate-300 border border-transparent"
                  }`}
                >
                  {acc.display_name || acc.phone}
                </button>
              ))}
            </div>
          )}

          {/* Filter bar: archive toggle + tag filter */}
          <div className="flex items-center gap-1.5 px-4 pt-1 pb-2 flex-wrap">
            <button
              onClick={() => setShowArchived(!showArchived)}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-all ${
                showArchived
                  ? "bg-brand/10 border-brand/30 text-brand"
                  : "border-surface-border text-slate-500 hover:text-brand"
              }`}
            >
              {showArchived ? "◀ Чаты" : "Архив"}
            </button>
            {allTags.map((tag) => (
              <button
                key={tag.id}
                onClick={() => setFilterTag(filterTag === tag.name ? null : tag.name)}
                className={`px-2 py-0.5 rounded-full text-[10px] font-medium border transition-all ${
                  filterTag === tag.name
                    ? "border-transparent shadow-sm"
                    : "border-surface-border opacity-50 hover:opacity-80"
                }`}
                style={{ backgroundColor: tag.color + "25", color: tag.color, borderColor: filterTag === tag.name ? tag.color + "40" : undefined }}
              >
                {tag.name}
              </button>
            ))}
            {filterTag && (
              <button onClick={() => setFilterTag(null)} className="text-[10px] text-slate-500 hover:text-white">✕</button>
            )}
          </div>
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
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-2 min-w-0">
                  {/* Avatar */}
                  {!avatarErrors.has(c.id) ? (
                    <img
                      src={avatarUrl(c.id)}
                      alt=""
                      className="w-8 h-8 rounded-full object-cover shrink-0 bg-surface-card"
                      onError={() => setAvatarErrors((prev) => new Set(prev).add(c.id))}
                    />
                  ) : (
                    <div className="w-8 h-8 rounded-full bg-surface-card border border-surface-border flex items-center justify-center shrink-0">
                      {(c.chat_type === "group" || c.chat_type === "channel" || c.chat_type === "supergroup") ? (
                        <svg className="w-4 h-4 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
                          <circle cx="9" cy="7" r="4" />
                          <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
                          <path d="M16 3.13a4 4 0 0 1 0 7.75" />
                        </svg>
                      ) : (
                        <span className="text-xs text-slate-400 font-medium">{c.alias.charAt(0).toUpperCase()}</span>
                      )}
                    </div>
                  )}
                  <span className={`font-medium text-sm truncate ${unread.has(c.id) ? "text-white" : ""}`}>{c.alias}</span>
                  {unread.has(c.id) && (
                    <span className="min-w-[20px] h-5 px-1.5 rounded-full bg-brand text-white text-[11px] font-bold flex items-center justify-center shrink-0">
                      {unread.get(c.id)! > 99 ? "99+" : unread.get(c.id)}
                    </span>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1 shrink-0">
                  {c.last_message_at && (
                    <span className={`text-xs ${unread.has(c.id) ? "text-brand font-medium" : "text-slate-500"}`}>
                      {(() => {
                        const d = new Date(c.last_message_at);
                        const now = new Date();
                        const isToday = d.toDateString() === now.toDateString();
                        return isToday
                          ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
                          : d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" });
                      })()}
                    </span>
                  )}
                  <div className="flex items-center gap-1">
                    <button
                      onClick={(e) => { e.stopPropagation(); togglePin(c.id); }}
                      className={`transition-colors p-0.5 ${pinned.has(c.id) ? "text-brand" : "text-slate-600 hover:text-brand"}`}
                      title={pinned.has(c.id) ? "Unpin" : "Pin"}
                    >
                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill={pinned.has(c.id) ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M12 17v5" /><path d="M9 2h6l-1 7h4l-7 8 1-5H8l1-10z" />
                      </svg>
                    </button>
                    <button
                      onClick={(e) => { e.stopPropagation(); handleArchive(c.id); }}
                      className="text-slate-600 hover:text-amber-400 transition-colors p-0.5"
                      title={c.is_archived ? "Разархивировать" : "Архивировать"}
                    >
                      <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill={c.is_archived ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="21 8 21 21 3 21 3 8" /><rect x="1" y="3" width="22" height="5" /><line x1="10" y1="12" x2="14" y2="12" />
                      </svg>
                    </button>
                    {isAdmin && (
                      <button
                        onClick={(e) => { e.stopPropagation(); deleteContact(c.id); }}
                        className="text-slate-600 hover:text-red-400 transition-colors p-0.5 -mr-1"
                        title="Удалить из CRM"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                        </svg>
                      </button>
                    )}
                  </div>
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
      <div className={`flex-1 flex flex-col min-w-0 ${!selected ? "hidden md:flex" : ""}`}>
        {selected ? (
          <>
            {/* Header */}
            <div className="px-2 py-1.5 md:px-3 md:py-2.5 border-b border-surface-border bg-surface-card/30 backdrop-blur-sm flex items-center gap-1.5 md:gap-2 shrink-0">
              <button onClick={() => setSelected(null)} className="md:hidden text-slate-400 hover:text-white transition-colors p-0.5">
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
                    <button onClick={renameContact} className="text-accent text-sm font-medium hover:text-accent/80 transition-colors">OK</button>
                    <button onClick={() => setEditingAlias(false)} className="text-slate-500 text-sm hover:text-slate-300 transition-colors">✕</button>
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5">
                    {isGroup && (
                      <svg className="w-3.5 h-3.5 text-slate-400 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
                        <path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
                      </svg>
                    )}
                    <div className="min-w-0">
                      <div
                        className="font-semibold text-sm cursor-pointer hover:text-brand transition-colors truncate"
                        onClick={() => { setAliasValue(selected.alias); setEditingAlias(true); }}
                        title="Click to rename"
                      >
                        {selected.alias}
                      </div>
                      <div className="text-[10px] text-slate-500 flex items-center gap-1.5 truncate">
                        <span title="Telegram ID">ID: {selected.real_tg_id || "—"}</span>
                        {selected.created_at && <span>с {new Date(selected.created_at).toLocaleDateString("ru-RU")}</span>}
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Tags toggle */}
              <button
                onClick={() => setShowTags(!showTags)}
                className={`p-1.5 rounded-lg border transition-all duration-200 shrink-0 ${
                  showTags || selected.tags.length > 0
                    ? "bg-brand/10 border-brand/30 text-brand"
                    : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                }`}
                title="Теги"
              >
                <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                  <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z" strokeLinecap="round" strokeLinejoin="round" />
                  <line x1="7" y1="7" x2="7.01" y2="7" />
                </svg>
              </button>

              {/* Add member (groups only) */}
              {isGroup && isAdmin && (
                <button
                  onClick={() => setShowAddMember(!showAddMember)}
                  className={`p-1.5 rounded-lg border transition-all duration-200 shrink-0 ${
                    showAddMember
                      ? "bg-brand/10 border-brand/30 text-brand"
                      : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                  }`}
                  title="Добавить участника"
                >
                  <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="8.5" cy="7" r="4" />
                    <line x1="20" y1="8" x2="20" y2="14" /><line x1="23" y1="11" x2="17" y2="11" />
                  </svg>
                </button>
              )}

              {/* Translation language selectors — hidden on mobile */}
              <select
                value={translateLangIn}
                onChange={(e) => setTranslateLangIn(e.target.value)}
                className="hidden md:block px-2 py-1.5 rounded-xl border border-surface-border bg-surface-card text-xs text-slate-400 focus:outline-none focus:border-brand/30 cursor-pointer shrink-0"
                title="Язык перевода входящих"
              >
                <option value="ru">↓ RU</option>
                <option value="en">↓ EN</option>
                <option value="es">↓ ES</option>
                <option value="de">↓ DE</option>
                <option value="fr">↓ FR</option>
                <option value="zh">↓ ZH</option>
                <option value="ar">↓ AR</option>
                <option value="pt">↓ PT</option>
                <option value="ja">↓ JA</option>
                <option value="ko">↓ KO</option>
                <option value="uk">↓ UK</option>
                <option value="tr">↓ TR</option>
              </select>
              <select
                value={translateLangOut}
                onChange={(e) => setTranslateLangOut(e.target.value)}
                className="hidden md:block px-2 py-1.5 rounded-xl border border-surface-border bg-surface-card text-xs text-slate-400 focus:outline-none focus:border-brand/30 cursor-pointer shrink-0"
                title="Язык перевода исходящих"
              >
                <option value="en">↑ EN</option>
                <option value="ru">↑ RU</option>
                <option value="es">↑ ES</option>
                <option value="de">↑ DE</option>
                <option value="fr">↑ FR</option>
                <option value="zh">↑ ZH</option>
                <option value="ar">↑ AR</option>
                <option value="pt">↑ PT</option>
                <option value="ja">↑ JA</option>
                <option value="ko">↑ KO</option>
                <option value="uk">↑ UK</option>
                <option value="tr">↑ TR</option>
              </select>

              {/* Forward mode toggle */}
              <button
                onClick={() => { setForwardMode(!forwardMode); setForwardSelected(new Set()); }}
                className={`p-1.5 rounded-lg border transition-all duration-200 shrink-0 ${
                  forwardMode
                    ? "bg-brand/10 border-brand/30 text-brand"
                    : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                }`}
                title={forwardMode ? "Отмена" : "Переслать"}
              >
                <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" />
                  <polyline points="14 8 18 12 14 16" />
                  <line x1="10" y1="12" x2="18" y2="12" />
                </svg>
              </button>
            </div>

            {/* Tags bar — expandable below header */}
            {(showTags || selected.tags.length > 0) && (
              <div className="px-3 py-1.5 border-b border-surface-border/50 bg-surface/50 shrink-0">
                <div className="flex gap-1 items-center flex-wrap">
                  {selected.tags.map((t) => {
                    const tagInfo = allTags.find((at) => at.name === t);
                    return <Badge key={t} text={t} color={tagInfo?.color} />;
                  })}
                  {selected.tags.length === 0 && !showTags && (
                    <span className="text-[10px] text-slate-600">нет тегов</span>
                  )}
                </div>
                {showTags && (
                  <div className="flex gap-1.5 mt-1.5 flex-wrap animate-slide-up">
                    {allTags.map((tag) => (
                      <button
                        key={tag.id}
                        onClick={() => toggleTag(tag.name)}
                        className={`px-2 py-0.5 rounded-full text-[11px] font-medium border transition-all duration-200 ${
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
                      <span className="text-xs text-slate-500">Тегов нет. Создайте в Настройках.</span>
                    )}
                  </div>
                )}
              </div>
            )}

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

            {/* Forum topic tabs */}
            {selected.is_forum && topics.length > 0 && (
              <div className="px-4 py-2 border-b border-surface-border/50 flex gap-1.5 overflow-x-auto flex-nowrap shrink-0">
                <button
                  onClick={() => setActiveTopic(null)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium border whitespace-nowrap transition-all ${
                    activeTopic === null
                      ? "bg-purple-500/20 border-purple-500/40 text-purple-400"
                      : "border-surface-border text-slate-400 hover:border-slate-500"
                  }`}
                >
                  Все топики
                </button>
                {topics.map((t) => (
                  <button
                    key={t.id}
                    onClick={() => setActiveTopic(t.id)}
                    className={`px-3 py-1.5 rounded-lg text-xs font-medium border whitespace-nowrap transition-all ${
                      activeTopic === t.id
                        ? "bg-purple-500/20 border-purple-500/40 text-purple-400"
                        : "border-surface-border text-slate-400 hover:border-slate-500"
                    }`}
                  >
                    {t.name}
                  </button>
                ))}
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
              {(loadingMessages || loadingTopic) && (
                <div className="flex items-center justify-center py-8">
                  <div className="w-6 h-6 border-2 border-brand/30 border-t-brand rounded-full animate-spin" />
                  <span className="ml-2 text-xs text-slate-400">Загрузка сообщений...</span>
                </div>
              )}
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
                    >
                      {/* Topic badge for forum supergroups */}
                      {m.topic_id && (
                        <div className="text-[10px] text-purple-400 font-medium mb-0.5 ml-1 flex items-center gap-1">
                          <svg className="w-2.5 h-2.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                            <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />
                          </svg>
                          {m.topic_name || (m.topic_id === 1 ? "General" : `Topic #${m.topic_id}`)}
                        </div>
                      )}
                      {/* Sender alias for group messages */}
                      {m.direction === "incoming" && isGroup && m.sender_alias && (
                        <div className="text-xs text-accent font-medium mb-0.5 ml-1">{m.sender_alias}</div>
                      )}

                      <div
                        id={`msg-${m.id}`}
                        className={`px-3.5 py-2.5 rounded-2xl text-sm overflow-hidden break-words ${
                          m.is_deleted
                            ? "bg-red-500/20 border border-red-500/40 text-red-200 rounded-br-md"
                            : m.is_edited
                            ? "bg-amber-500/15 border border-amber-400/40 text-amber-100 rounded-br-md"
                            : m.direction === "outgoing"
                            ? "bg-gradient-to-br from-brand to-brand-dark text-white rounded-br-md shadow-[0_2px_8px_rgba(14,165,233,0.2)]"
                            : "bg-surface-card border border-surface-border text-white rounded-bl-md"
                        }`}
                      >
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

                        {/* Forwarded label */}
                        {m.forwarded_from_alias && (
                          <div className={`flex items-center gap-1.5 mb-1 text-xs italic ${m.direction === "outgoing" ? "text-white/50" : "text-slate-400"}`}>
                            <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <polyline points="15 17 20 12 15 7" />
                              <path d="M4 18v-2a4 4 0 014-4h12" />
                            </svg>
                            Переслано от {m.forwarded_from_alias}
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
                                loading="lazy"
                                className="rounded-xl max-w-full max-h-64 object-cover cursor-pointer hover:opacity-90 transition-opacity"
                                onClick={(e) => { e.stopPropagation(); setLightboxSrc(mediaUrl(m.media_path!)); }}
                              />
                            )}
                            {m.media_type === "video" && (
                              <video src={mediaUrl(m.media_path)} controls preload="none" className="rounded-xl max-w-full max-h-64" />
                            )}
                            {m.media_type === "voice" && (
                              <audio src={mediaUrl(m.media_path)} controls preload="none" className="w-full" />
                            )}
                            {m.media_type === "document" && (() => {
                              const ext = m.media_path!.split('.').pop()?.toLowerCase() || '';
                              const isImage = ['jpg','jpeg','png','gif','webp','bmp','svg'].includes(ext);
                              return isImage ? (
                                <img
                                  src={mediaUrl(m.media_path!)}
                                  alt=""
                                  loading="lazy"
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

                        {/* Translation */}
                        {translations.has(m.id) && (
                          <div className={`mt-1.5 pt-1.5 border-t text-xs italic ${m.direction === "outgoing" ? "border-white/20 text-white/60" : "border-surface-border text-slate-400"}`}>
                            🌐 {translations.get(m.id)}
                          </div>
                        )}

                        {/* Timestamp + edited + read status + actions */}
                        <div className={`flex items-center justify-end gap-1 text-[10px] mt-1 ${m.direction === "outgoing" ? "text-white/40" : "text-slate-500"}`}>
                          {/* Translate button */}
                          {m.content && !translations.has(m.id) && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleTranslate(m.id, m.content!, m.direction); }}
                              className={`hover:text-brand transition-colors ${translating === m.id ? "animate-pulse" : ""}`}
                              title="Перевести"
                            >
                              🌐
                            </button>
                          )}
                          {translations.has(m.id) && (
                            <button
                              onClick={(e) => { e.stopPropagation(); setTranslations((prev) => { const n = new Map(prev); n.delete(m.id); return n; }); }}
                              className="hover:text-brand transition-colors"
                              title="Скрыть перевод"
                            >
                              ✕
                            </button>
                          )}
                          {/* Edit button (outgoing only) */}
                          {m.direction === "outgoing" && m.content && !m.is_deleted && (
                            <button
                              onClick={(e) => { e.stopPropagation(); setEditingMsg(m); setEditText(m.content || ""); }}
                              className="hover:text-brand transition-colors"
                              title="Редактировать"
                            >
                              ✏️
                            </button>
                          )}
                          {/* Delete button (outgoing only) */}
                          {m.direction === "outgoing" && !m.is_deleted && (
                            <button
                              onClick={async (e) => {
                                e.stopPropagation();
                                if (!confirm("Удалить сообщение?")) return;
                                try {
                                  await api(`/api/messages/${selected!.id}/delete/${m.id}`, { method: "DELETE" });
                                  setMessages((prev) => prev.map((msg) => msg.id === m.id ? { ...msg, is_deleted: true } : msg));
                                } catch {}
                              }}
                              className="hover:text-red-400 transition-colors"
                              title="Удалить"
                            >
                              🗑
                            </button>
                          )}
                          {m.is_edited && (
                            <button
                              onClick={(e) => { e.stopPropagation(); showEditHistory(selected!.id, m.id); }}
                              className="italic mr-1 text-amber-400/70 hover:text-amber-300 cursor-pointer transition-colors"
                              title="Показать историю изменений"
                            >
                              (ред.)
                            </button>
                          )}
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
                                      } else if (btn.send_text && selected) {
                                        const tempId = `temp-${Date.now()}`;
                                        setMessages((prev) => [...prev, {
                                          id: tempId, contact_id: selected.id, tg_message_id: null,
                                          direction: "outgoing", content: btn.send_text!, media_type: null,
                                          media_path: null, sent_by: null, is_read: false, is_edited: false,
                                          is_deleted: false, inline_buttons: null, reply_to_msg_id: null,
                                          reply_to_content_preview: null, forwarded_from_alias: null,
                                          sender_alias: null, topic_id: null, topic_name: null,
                                          created_at: new Date().toISOString(),
                                        } as any]);
                                        api(`/api/messages/${selected.id}/send`, {
                                          method: "POST",
                                          body: JSON.stringify({ content: btn.send_text }),
                                        }).then((msg) => {
                                          setMessages((prev) => prev.map((m) => m.id === tempId ? msg : m));
                                        }).catch((e: any) => {
                                          setMessages((prev) => prev.filter((m) => m.id !== tempId));
                                          alert(e.message);
                                        });
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

            {/* Edit message bar */}
            {editingMsg && (
              <div className="px-4 py-2 bg-amber-500/5 border-t border-amber-500/20 flex items-center gap-3 animate-slide-up">
                <div className="w-1 h-8 bg-amber-500 rounded-full shrink-0" />
                <div className="flex-1 min-w-0">
                  <input
                    value={editText}
                    onChange={(e) => setEditText(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") handleEditMessage(); if (e.key === "Escape") setEditingMsg(null); }}
                    className="w-full bg-transparent text-sm focus:outline-none"
                    autoFocus
                  />
                </div>
                <button onClick={handleEditMessage} className="text-amber-400 text-xs font-medium hover:text-amber-300">Сохранить</button>
                <button onClick={() => setEditingMsg(null)} className="text-slate-500 hover:text-white p-1">
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              </div>
            )}

            {/* Template picker popup */}
            {showTemplates && templates.length > 0 && (
              <div className="px-4 py-2 border-t border-surface-border bg-surface-card/50 max-h-40 overflow-auto animate-slide-up">
                <div className="text-[10px] text-slate-500 mb-1.5 font-medium">Шаблоны {text.startsWith("/") && <span className="text-brand">— введите шорткат и Enter</span>}</div>
                <div className="space-y-1">
                  {templates.filter((tpl) => {
                    // Filter by account: show templates matching selected account or global (null)
                    if (filterAccountId && tpl.tg_account_id && tpl.tg_account_id !== filterAccountId) return false;
                    if (!text.startsWith("/")) return true;
                    const q = text.toLowerCase();
                    return (tpl.shortcut && tpl.shortcut.toLowerCase().startsWith(q)) || tpl.title.toLowerCase().includes(q.slice(1));
                  }).map((tpl) => (
                    <button
                      key={tpl.id}
                      onClick={() => applyTemplate(tpl)}
                      className="w-full text-left px-2.5 py-1.5 rounded-lg text-xs hover:bg-surface-hover transition-colors border border-transparent hover:border-surface-border"
                    >
                      <span className="text-brand font-medium">{tpl.title}</span>
                      {tpl.media_type && <span className="ml-1">{tpl.media_type === "photo" ? "📷" : tpl.media_type === "video" ? "🎬" : tpl.media_type === "video_note" ? "🔵" : tpl.media_type === "voice" ? "🎤" : "📄"}</span>}
                      {tpl.shortcut && <span className="text-slate-600 ml-1 font-mono">{tpl.shortcut}</span>}
                      <span className="text-slate-500 ml-2 truncate">{tpl.content.slice(0, 50)}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Emoji picker popup */}
            {showEmoji && (
              <div className="px-4 py-2 border-t border-surface-border bg-surface-card/50 animate-slide-up">
                <div className="flex flex-wrap gap-1 max-h-32 overflow-auto">
                  {["😀","😂","🤣","😊","😍","🥰","😘","😎","🤔","😢","😭","😡","🔥","❤️","👍","👎","👏","🙏","💪","🎉","✅","❌","⭐","💯","🚀","💬","📌","📎","🔗","📸","🎵","💡","⚡","🌟","💎","🏆","🤝","👀","🙂","😅","🤩","😇","🤗","😋","🤭","🥺","😏","🙄","😴","🤑","🤠","👋","✌️","🤞","👌","💀","🫡","😈","💩"].map((e) => (
                    <button
                      key={e}
                      onClick={() => { setText((prev) => prev + e); inputRef.current?.focus(); }}
                      className="w-8 h-8 flex items-center justify-center text-lg hover:bg-surface-hover rounded-lg transition-colors"
                    >
                      {e}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Reply keyboard (persistent bot buttons) */}
            {(() => {
              // Find last reply keyboard, but check if it was hidden after
              let lastKb: any = null;
              for (let i = messages.length - 1; i >= 0; i--) {
                const m = messages[i];
                if (isKeyboardHidden(m.inline_buttons)) break; // keyboard was hidden
                const btns = parseInlineButtons(m.inline_buttons);
                if (btns.length > 0 && btns.some((row) => row.some((b) => b.send_text))) {
                  lastKb = m;
                  break;
                }
              }
              if (!lastKb) return null;
              const kbButtons = parseInlineButtons(lastKb.inline_buttons);
              return (
                <div className="px-2 py-1.5 border-t border-surface-border/50 bg-surface/80 shrink-0">
                  <div className="space-y-1">
                    {kbButtons.map((row, ri) => (
                      <div key={ri} className="flex gap-1">
                        {row.map((btn, bi) => (
                          <button
                            key={bi}
                            onClick={() => {
                              if (!selected) return;
                              const tempId = `temp-${Date.now()}`;
                              const sendText = btn.send_text || btn.text;
                              setMessages((prev) => [...prev, {
                                id: tempId, contact_id: selected.id, tg_message_id: null,
                                direction: "outgoing", content: sendText, media_type: null,
                                media_path: null, sent_by: null, is_read: false, is_edited: false,
                                is_deleted: false, inline_buttons: null, reply_to_msg_id: null,
                                reply_to_content_preview: null, forwarded_from_alias: null,
                                sender_alias: null, topic_id: null, topic_name: null,
                                created_at: new Date().toISOString(),
                              } as any]);
                              api(`/api/messages/${selected.id}/send`, {
                                method: "POST",
                                body: JSON.stringify({ content: sendText }),
                              }).then((msg) => {
                                setMessages((prev) => prev.map((m) => m.id === tempId ? msg : m));
                              }).catch((e: any) => {
                                setMessages((prev) => prev.filter((m) => m.id !== tempId));
                                alert(e.message);
                              });
                            }}
                            className="flex-1 px-2 py-2 text-xs font-medium rounded-xl bg-surface-card border border-surface-border text-slate-300 hover:border-brand/30 hover:text-brand transition-all active:scale-95"
                          >
                            {btn.text}
                          </button>
                        ))}
                      </div>
                    ))}
                  </div>
                </div>
              );
            })()}

            {/* Input */}
            <div className="p-2 md:p-3 border-t border-surface-border bg-surface-card/30 backdrop-blur-sm shrink-0" style={{ paddingBottom: 'max(0.5rem, env(safe-area-inset-bottom, 0.5rem))' }}>
              <div className="flex gap-1 items-center bg-surface-card border border-surface-border rounded-2xl px-1">
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.zip"
                  onChange={handleFileUpload}
                  className="hidden"
                />
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="text-slate-500 hover:text-brand transition-colors p-2 shrink-0"
                  title="Прикрепить файл"
                >
                  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48" />
                  </svg>
                </button>
                <button
                  onClick={() => { setShowEmoji(!showEmoji); setShowTemplates(false); }}
                  className={`p-2 transition-colors shrink-0 ${showEmoji ? "text-brand" : "text-slate-500 hover:text-brand"}`}
                  title="Эмоджи"
                >
                  <span className="text-lg leading-none">😊</span>
                </button>
                <button
                  onClick={() => { setShowTemplates(!showTemplates); setShowEmoji(false); }}
                  className={`p-2 transition-colors shrink-0 ${showTemplates ? "text-brand" : "text-slate-500 hover:text-brand"}`}
                  title="Шаблоны"
                >
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
                    <polyline points="14 2 14 8 20 8" />
                    <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
                  </svg>
                </button>
                <textarea
                  ref={inputRef}
                  value={text}
                  onChange={(e) => {
                    const val = e.target.value;
                    setText(val);
                    // Auto-show templates when typing /
                    if (val.startsWith("/") && val.length >= 1) {
                      setShowTemplates(true);
                      setShowEmoji(false);
                    } else if (showTemplates && !val.startsWith("/")) {
                      setShowTemplates(false);
                    }
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      sendMessage();
                    }
                  }}
                  placeholder="Введите сообщение..."
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
                  onClick={async () => {
                    if (!text.trim()) return;
                    setTranslatingInput(true);
                    try {
                      const result = await translateText(text, translateLangOut);
                      setText(result.translated);
                    } catch (e: any) { alert(e.message); }
                    setTranslatingInput(false);
                  }}
                  disabled={!text.trim() || translatingInput}
                  className={`text-slate-500 hover:text-brand disabled:text-slate-600 transition-colors p-2 shrink-0 ${translatingInput ? "animate-pulse" : ""}`}
                  title={`Перевести на ${translateLangOut.toUpperCase()}`}
                >
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M5 8l6 6" /><path d="M4 14l6-6 2-3" /><path d="M2 5h12" /><path d="M7 2h1" />
                    <path d="M22 22l-5-10-5 10" /><path d="M14 18h6" />
                  </svg>
                </button>
                <button
                  onClick={sendMessage}
                  disabled={!text.trim() || sending}
                  className={`text-brand hover:text-brand-light disabled:text-slate-600 transition-colors p-2 shrink-0 ${sending ? "animate-pulse" : ""}`}
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
      {showForwardPicker && (() => {
        const ForwardPicker = () => {
          const [fwdSearch, setFwdSearch] = useState("");
          const filtered = contacts
            .filter((c) => c.id !== selected?.id && c.status === "approved")
            .filter((c) => !fwdSearch || c.alias.toLowerCase().includes(fwdSearch.toLowerCase()));
          return (
            <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => setShowForwardPicker(false)}>
              <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-sm mx-4 max-h-[60vh] flex flex-col animate-slide-up" onClick={(e) => e.stopPropagation()}>
                <div className="p-4 border-b border-surface-border space-y-2">
                  <h3 className="font-semibold">Переслать</h3>
                  <input
                    type="text"
                    placeholder="Поиск контакта..."
                    value={fwdSearch}
                    onChange={(e) => setFwdSearch(e.target.value)}
                    className="w-full px-3 py-2 rounded-xl bg-surface border border-surface-border text-sm text-white placeholder-slate-500 focus:outline-none focus:border-brand/40"
                    autoFocus
                  />
                </div>
                <div className="flex-1 overflow-auto">
                  {filtered.length === 0 && (
                    <div className="p-4 text-center text-sm text-slate-500">Ничего не найдено</div>
                  )}
                  {filtered.map((c) => (
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
                    Отмена
                  </Button>
                </div>
              </div>
            </div>
          );
        };
        return <ForwardPicker />;
      })()}

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

      {/* Edit history popup */}
      {editHistoryMsg && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => { setEditHistoryMsg(null); setEditHistory([]); }}>
          <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-sm mx-4 max-h-[60vh] flex flex-col animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <div className="p-4 border-b border-surface-border flex items-center justify-between">
              <h3 className="font-semibold text-sm">История изменений</h3>
              <button onClick={() => { setEditHistoryMsg(null); setEditHistory([]); }} className="text-slate-500 hover:text-white p-1">
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4 space-y-3">
              {loadingEditHistory && (
                <div className="flex items-center justify-center py-4">
                  <div className="w-5 h-5 border-2 border-brand/30 border-t-brand rounded-full animate-spin" />
                </div>
              )}
              {!loadingEditHistory && editHistory.length === 0 && (
                <p className="text-sm text-slate-500 text-center">Нет истории изменений</p>
              )}
              {editHistory.map((entry, idx) => (
                <div key={idx} className="border border-surface-border rounded-xl p-3 space-y-1.5">
                  <div className="text-[10px] text-slate-500">
                    {new Date(entry.edited_at).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" })}
                  </div>
                  {entry.old_content && (
                    <div className="text-xs text-red-400/80 line-through break-words">{entry.old_content}</div>
                  )}
                  <div className="text-[10px] text-slate-600 flex items-center gap-1">
                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <polyline points="6 9 12 15 18 9" />
                    </svg>
                  </div>
                  {entry.new_content && (
                    <div className="text-xs text-emerald-400/80 break-words">{entry.new_content}</div>
                  )}
                </div>
              ))}
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

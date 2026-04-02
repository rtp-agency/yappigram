"use client";

import { useEffect, useRef, useState, useCallback, memo, useMemo } from "react";
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

// Lazy avatar: shows initials immediately, loads real avatar when visible in viewport
function LazyAvatar({ contactId, alias, chatType, hasError, onError }: {
  contactId: string; alias: string; chatType: string; hasError: boolean; onError: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el || hasError) return;
    const obs = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) { setVisible(true); obs.disconnect(); }
    }, { rootMargin: "100px" });
    obs.observe(el);
    return () => obs.disconnect();
  }, [hasError]);

  const isGroup = chatType === "group" || chatType === "channel" || chatType === "supergroup";
  const initial = alias.charAt(0).toUpperCase();

  return (
    <div ref={ref} className="w-8 h-8 rounded-full shrink-0 relative">
      {visible && !hasError && (
        <img
          src={avatarUrl(contactId)}
          alt=""
          className={`w-8 h-8 rounded-full object-cover absolute inset-0 transition-opacity duration-300 ${loaded ? "opacity-100" : "opacity-0"}`}
          onLoad={() => setLoaded(true)}
          onError={onError}
        />
      )}
      <div className={`w-8 h-8 rounded-full bg-surface-card border border-surface-border flex items-center justify-center transition-opacity duration-300 ${loaded ? "opacity-0" : "opacity-100"}`}>
        {isGroup ? (
          <svg className="w-4 h-4 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
            <path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
          </svg>
        ) : (
          <span className="text-xs text-slate-400 font-medium">{initial}</span>
        )}
      </div>
    </div>
  );
}

// Custom voice message player with waveform visualization
function VoicePlayer({ src, direction }: { src: string; direction: string }) {
  const audioRef = useRef<HTMLAudioElement>(null);
  const [playing, setPlaying] = useState(false);
  const [progress, setProgress] = useState(0);
  const [duration, setDuration] = useState(0);
  const isOut = direction === "outgoing";

  // Generate pseudo-random waveform bars from src hash
  const bars = useRef(
    Array.from({ length: 32 }, (_, i) => 0.15 + Math.abs(Math.sin(i * 2.7 + src.length)) * 0.85)
  ).current;

  const toggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    const a = audioRef.current;
    if (!a) return;
    if (playing) { a.pause(); } else { a.play(); }
    setPlaying(!playing);
  };

  const seek = (e: React.MouseEvent<HTMLDivElement>) => {
    const a = audioRef.current;
    if (!a || !a.duration) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const pct = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    a.currentTime = pct * a.duration;
    setProgress(pct);
  };

  const fmt = (s: number) => {
    if (!s || !isFinite(s)) return "0:00";
    const m = Math.floor(s / 60);
    const sec = Math.floor(s % 60);
    return `${m}:${sec.toString().padStart(2, "0")}`;
  };

  return (
    <div className="mb-2 flex items-center gap-2 min-w-[200px] max-w-[280px]">
      <audio
        ref={audioRef}
        src={src}
        preload="metadata"
        onLoadedMetadata={(e) => setDuration((e.target as HTMLAudioElement).duration)}
        onTimeUpdate={(e) => {
          const a = e.target as HTMLAudioElement;
          if (a.duration) setProgress(a.currentTime / a.duration);
        }}
        onEnded={() => { setPlaying(false); setProgress(0); }}
      />
      <button
        onClick={toggle}
        className={`w-9 h-9 rounded-full flex items-center justify-center shrink-0 transition-colors ${
          isOut ? "bg-white/20 hover:bg-white/30" : "bg-brand/20 hover:bg-brand/30"
        }`}
      >
        {playing ? (
          <svg className={`w-4 h-4 ${isOut ? "text-white" : "text-brand"}`} viewBox="0 0 24 24" fill="currentColor">
            <rect x="6" y="4" width="4" height="16" rx="1" /><rect x="14" y="4" width="4" height="16" rx="1" />
          </svg>
        ) : (
          <svg className={`w-4 h-4 ml-0.5 ${isOut ? "text-white" : "text-brand"}`} viewBox="0 0 24 24" fill="currentColor">
            <polygon points="5 3 19 12 5 21 5 3" />
          </svg>
        )}
      </button>
      <div className="flex-1 min-w-0">
        <div className="flex items-end gap-[2px] h-7 cursor-pointer" onClick={seek}>
          {bars.map((h, i) => {
            const filled = i / bars.length <= progress;
            return (
              <div
                key={i}
                className={`flex-1 rounded-full transition-colors duration-100 ${
                  filled
                    ? isOut ? "bg-white" : "bg-brand"
                    : isOut ? "bg-white/25" : "bg-brand/25"
                }`}
                style={{ height: `${h * 100}%`, minWidth: 2 }}
              />
            );
          })}
        </div>
        <div className={`text-[10px] mt-0.5 ${isOut ? "text-white/50" : "text-slate-500"}`}>
          {playing ? fmt(audioRef.current?.currentTime || 0) : fmt(duration)}
        </div>
      </div>
    </div>
  );
}

// Sort messages by time, fallback to tg_message_id
const sortMsgs = (msgs: Message[]) => [...msgs].sort((a, b) => {
  const dt = new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
  if (dt !== 0) return dt;
  return (a.tg_message_id || 0) - (b.tg_message_id || 0);
});

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
  const [text, _setText] = useState("");
  const textRef = useRef("");
  const setText = (val: string) => {
    textRef.current = val;
    _setText(val);
    // Sync uncontrolled textarea
    if (inputRef.current && inputRef.current.value !== val) {
      inputRef.current.value = val;
      inputRef.current.style.height = "auto";
      inputRef.current.style.height = Math.min(inputRef.current.scrollHeight, 128) + "px";
    }
  };
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
  const [forwardMediaOnly, setForwardMediaOnly] = useState(false);

  // Bot callback toast
  const [botToast, setBotToast] = useState<string | null>(null);
  // User timezone for formatting times
  const [userTimezone, setUserTimezone] = useState(() => {
    if (typeof window === "undefined") return "UTC";
    return localStorage.getItem("crm_timezone") || "UTC";
  });

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
  const [tplCategory, setTplCategory] = useState<string | null>(null);

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

  // Context menu (right-click / long-press)
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; message: Message } | null>(null);
  const longPressTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const longPressTriggered = useRef(false);

  // Pending media files (attach before sending)
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);

  // Input dropdown menu (emoji/translate/schedule)
  const [showInputMenu, setShowInputMenu] = useState(false);

  // Scheduled message
  const [scheduleMode, setScheduleMode] = useState(false);
  const [scheduleDate, setScheduleDate] = useState("");
  const [scheduleTime, setScheduleTime] = useState("");
  const [scheduledList, setScheduledList] = useState<any[]>([]);
  const [showScheduledList, setShowScheduledList] = useState(false);

  // User info panel
  const [showUserInfo, setShowUserInfo] = useState(false);
  const [userInfoTab, setUserInfoTab] = useState<"media" | "notes" | "postbacks">("media");
  const [mediaSubTab, setMediaSubTab] = useState<"photos" | "videos" | "files" | "voice">("photos");
  const [contactNotes, setContactNotes] = useState("");
  const [savingNotes, setSavingNotes] = useState(false);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const selectedRef = useRef<Contact | null>(null);
  const filterAccountRef = useRef<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => { api("/api/pinned").then((ids: string[]) => setPinned(new Set(ids))).catch(console.error); }, []);
  // Sync role from server (in case localStorage is stale)
  useEffect(() => {
    api("/api/staff/me").then((me: any) => {
      if (me?.role) setRole(me.role);
      if (me?.timezone) {
        setUserTimezone(me.timezone);
        try { localStorage.setItem("crm_timezone", me.timezone); } catch {}
      }
    }).catch(() => {});
  }, []);
  useEffect(() => { selectedRef.current = selected; }, [selected]);
  useEffect(() => { filterAccountRef.current = filterAccountId; }, [filterAccountId]);

  // Fetch TG accounts for switcher — default to first account if none selected
  useEffect(() => {
    fetchTgStatus().then((rawAccs) => {
      const accs = rawAccs.filter((a) => a.is_active);
      setAccountsList(accs);
      if (accs.length >= 1) {
        // If no account selected or selected account is not in active list — auto-select first
        const currentValid = filterAccountId && accs.some((a) => a.id === filterAccountId);
        if (!currentValid) {
          const firstId = accs[0].id;
          setFilterAccountId(firstId);
          sessionStorage.setItem("crm_selected_account", firstId);
        }
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

  // Re-fetch contacts when account filter changes (both normal + archived)
  useEffect(() => {
    const acctId = filterAccountId || undefined;
    Promise.all([
      fetchContacts(undefined, acctId, false),
      fetchContacts(undefined, acctId, true),
    ]).then(([normal, archived]) => {
      setContacts([...normal, ...archived]);
    }).catch(console.error);
  }, [filterAccountId]);

  useEffect(() => {
    connectWS();

    const unsub = onWSEvent((event) => {
      if (event.type === "new_message") {
        const isCurrentChat = selectedRef.current?.id === event.contact_id;
        setContacts((prev) => {
          const exists = prev.some((c) => c.id === event.contact_id);
          if (!exists) {
            // New contact — fetch full contact list
            const acctId = filterAccountRef.current || undefined;
            Promise.all([fetchContacts(undefined, acctId, false), fetchContacts(undefined, acctId, true)])
              .then(([n, a]) => setContacts([...n, ...a])).catch(console.error);
            return prev;
          }
          const msgPreview = event.message?.content || (event.message?.media_type ? `[${event.message.media_type}]` : "") || "";
          const msgDir = event.message?.direction || "incoming";
          return prev
            .map((c) => c.id === event.contact_id ? { ...c, last_message_at: new Date().toISOString(), last_message_content: msgPreview.slice(0, 100), last_message_direction: msgDir, last_message_is_read: false } : c);
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
        // Update contact preview checkmarks
        if (event.contact_id) {
          setContacts((prev) => prev.map((c) => c.id === event.contact_id ? { ...c, last_message_is_read: true } : c));
        }
      }
      if (event.type === "contact_deleted") {
        setContacts((prev) => prev.filter((c) => c.id !== event.contact_id));
      }
    });

    return unsub;
  }, []);

  // Polling fallback: refresh contacts every 10s (handles WS being down)
  useEffect(() => {
    const interval = setInterval(() => {
      const acctId = filterAccountRef.current || undefined;
      Promise.all([
        fetchContacts(undefined, acctId, false),
        fetchContacts(undefined, acctId, true),
      ]).then(([normal, archived]) => {
        const all = [...normal, ...archived];
        setContacts((prev) => {
          if (JSON.stringify(all.map((c: Contact) => c.last_message_at)) !== JSON.stringify(prev.map((c: Contact) => c.last_message_at))) {
            return all;
          }
          return prev;
        });
      }).catch(() => {});
    }, 30000);  // 30s fallback — WS handles real-time updates
    return () => clearInterval(interval);
  }, []);

  const [loadingMessages, setLoadingMessages] = useState(false);
  useEffect(() => {
    if (!selected) return;
    setActiveTopic(null);
    setTopics([]);
    setMessages([]);
    setLoadingMessages(true);
    const sortMsgs = (msgs: Message[]) => [...msgs].sort((a, b) => {
      const aId = a.tg_message_id || 0;
      const bId = b.tg_message_id || 0;
      if (aId && bId) return aId - bId;
      return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
    });
    api(`/api/messages/${selected.id}?limit=200`).then((msgs: Message[]) => {
      setMessages(sortMsgs(msgs));
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: "instant" }), 50);
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: "instant" }), 200);
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: "instant" }), 500);
    }).catch(console.error).finally(() => setLoadingMessages(false));
    if (selected.is_forum) {
      api(`/api/messages/${selected.id}/topics`).then(setTopics).catch(console.error);
    }
    setReplyTo(null);
    setForwardMode(false);
    setForwardSelected(new Set());
    setShowUserInfo(false);
    setContactNotes(selected?.notes || "");
    setUnread((prev) => { const n = new Map(prev); n.delete(selected.id); return n; });
    api(`/api/messages/${selected.id}/read`, { method: "PATCH" }).catch(console.error);
  }, [selected]);


  // Reload messages when topic filter changes
  useEffect(() => {
    if (!selected) return;
    setLoadingTopic(true);
    const topicParam = activeTopic !== null ? `&topic_id=${activeTopic}` : "";
    api(`/api/messages/${selected.id}?limit=200${topicParam}`).then((msgs: Message[]) => {
      setMessages(sortMsgs(msgs));
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: "instant" }), 50);
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: "instant" }), 300);
    }).catch(console.error).finally(() => setLoadingTopic(false));
  }, [activeTopic]);

  useEffect(() => {
    if (!selected) return;
    const topicParam = activeTopic !== null ? `&topic_id=${activeTopic}` : "";
    const interval = setInterval(() => {
      api(`/api/messages/${selected.id}?limit=200${topicParam}`).then((msgs: Message[]) => {
        const sorted = sortMsgs(msgs);
        setMessages((prev) => {
          if (sorted.length !== prev.length || JSON.stringify(sorted.map(m => m.is_deleted)) !== JSON.stringify(prev.map(m => m.is_deleted))) return sorted;
          return prev;
        });
      }).catch(() => {});
    }, 5000);  // 5s fallback — WS handles real-time updates
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

  // Close context menu on outside click/scroll/resize
  useEffect(() => {
    if (!contextMenu) return;
    const close = () => setContextMenu(null);
    window.addEventListener("click", close);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      window.removeEventListener("click", close);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [contextMenu]);

  // Hide bottom nav when chat is open on mobile
  useEffect(() => {
    const nav = document.getElementById("bottom-nav");
    if (!nav) return;
    const isMobile = window.innerWidth < 768;
    if (isMobile && selected) {
      nav.style.display = "none";
    } else {
      nav.style.display = "";
    }
    return () => { nav.style.display = ""; };
  }, [selected]);

  // Load scheduled messages
  useEffect(() => {
    api("/api/scheduled").then(setScheduledList).catch(() => {});
    const iv = setInterval(() => api("/api/scheduled").then(setScheduledList).catch(() => {}), 15000);
    return () => clearInterval(iv);
  }, []);

  const cancelScheduled = async (id: string) => {
    try {
      await api(`/api/scheduled/${id}`, { method: "DELETE" });
      setScheduledList((prev) => prev.filter((s) => s.id !== id));
    } catch (e: any) { alert(e.message); }
  };

  // Close input menu on outside click
  useEffect(() => {
    if (!showInputMenu) return;
    const close = (e: any) => {
      if (!(e.target as HTMLElement).closest?.(".input-menu-container")) setShowInputMenu(false);
    };
    setTimeout(() => window.addEventListener("click", close), 0);
    return () => window.removeEventListener("click", close);
  }, [showInputMenu]);

  const sendingRef = useRef(false);
  const sendMessage = async () => {
    const content = textRef.current.trim();
    const hasFiles = pendingFiles.length > 0;
    if ((!content && !hasFiles) || !selected || sendingRef.current) return;
    sendingRef.current = true;

    // Check for shortcut match (text-only)
    if (content && !hasFiles) {
      const matchedTpl = checkShortcut(content);
      if (matchedTpl) {
        setText("");
        if (inputRef.current) inputRef.current.style.height = "auto";
        await applyTemplate(matchedTpl);
        sendingRef.current = false;
        return;
      }
    }

    // Clear input immediately for snappy UX
    const savedText = content;
    const savedReply = replyTo;
    const savedFiles = [...pendingFiles];
    setText("");
    setReplyTo(null);
    setPendingFiles([]);
    setShowEmoji(false);
    setShowTemplates(false);
    setShowInputMenu(false);
    if (inputRef.current) inputRef.current.style.height = "auto";
    setSending(true);
    try {
      if (savedFiles.length > 0) {
        // Send files — last one gets caption
        for (let i = 0; i < savedFiles.length; i++) {
          const isLast = i === savedFiles.length - 1;
          const caption = isLast ? savedText : undefined;
          const msg = await uploadMedia(selected.id, savedFiles[i], caption || undefined);
          setMessages((prev) => {
            if (prev.some((m) => m.id === msg.id)) return prev;
            return [...prev, msg];
          });
        }
      } else {
        // Text-only message
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
      }
      // Move this chat to top + update last message preview
      setContacts((prev) => prev
        .map((c: Contact) => c.id === selected.id ? { ...c, last_message_at: new Date().toISOString(), last_message_content: (savedText || "[media]").slice(0, 100), last_message_direction: "outgoing", last_message_is_read: false } : c)
      );
    } catch (e: any) {
      // Restore text on failure
      setText(savedText);
      if (savedFiles.length > 0) setPendingFiles(savedFiles);
      alert(e.message);
    } finally { sendingRef.current = false; setSending(false); }
  };

  // Apply template: supports scripts (multi-message, split by \n---\n)
  const applyTemplate = async (tpl: Template) => {
    setShowTemplates(false);
    if (!selected) return;

    // Check if it's a script (multi-message template)
    const parts = tpl.content.split("\n---\n").map((s) => s.trim()).filter(Boolean);
    const isScript = parts.length > 1;

    if (tpl.media_path && tpl.media_type) {
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
    } else if (isScript) {
      // Script mode: send each part as a separate message with small delay
      sendingRef.current = true;
      setSending(true);
      try {
        for (const part of parts) {
          const msg = await api(`/api/messages/${selected.id}/send`, {
            method: "POST",
            body: JSON.stringify({ content: part }),
          });
          setMessages((prev) => {
            if (prev.some((m) => m.id === msg.id)) return prev;
            return [...prev, msg];
          });
          // Small delay between messages for natural feel
          if (part !== parts[parts.length - 1]) {
            await new Promise((r) => setTimeout(r, 10));
          }
        }
      } catch (e: any) { alert(e.message); }
      sendingRef.current = false;
      setSending(false);
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

  const MAX_FILE_SIZE = 20 * 1024 * 1024; // 20MB per file
  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files || !selected) return;
    const valid = Array.from(files).filter((f) => {
      if (f.size > MAX_FILE_SIZE) {
        alert(`Файл "${f.name}" слишком большой (${(f.size / 1024 / 1024).toFixed(1)}MB). Максимум 20MB.`);
        return false;
      }
      return true;
    });
    setPendingFiles((prev) => [...prev, ...valid].slice(0, 5));
    e.target.value = "";
  };

  const removePendingFile = (idx: number) => {
    setPendingFiles((prev) => prev.filter((_, i) => i !== idx));
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
      await forwardMessages(selected.id, Array.from(forwardSelected), toContactId, forwardMediaOnly);
      setForwardMode(false);
      setForwardSelected(new Set());
      setShowForwardPicker(false);
      setForwardMediaOnly(false);
      // Move target contact to top of list (update last_message_at)
      setContacts((prev) => prev.map((c) =>
        c.id === toContactId ? { ...c, last_message_at: new Date().toISOString() } : c
      ));
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

  const filteredContacts = useMemo(() => contacts
    .filter((c) => {
      if (showArchived ? !c.is_archived : c.is_archived) return false;
      if (search && !c.alias.toLowerCase().includes(search.toLowerCase())) return false;
      if (filterTag && !c.tags.includes(filterTag)) return false;
      return true;
    })
    .sort((a, b) => {
      const ap = pinned.has(a.id) ? 1 : 0;
      const bp = pinned.has(b.id) ? 1 : 0;
      if (ap !== bp) return bp - ap;
      const aDate = a.last_message_at || a.created_at || "";
      const bDate = b.last_message_at || b.created_at || "";
      return bDate.localeCompare(aDate);
    }), [contacts, showArchived, search, filterTag, pinned]);

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
          <div className="flex items-center gap-1.5 px-4 pt-1 pb-2 overflow-x-auto flex-nowrap" style={{ scrollbarWidth: "thin" }}>
            <button
              onClick={() => setShowArchived(!showArchived)}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-all shrink-0 ${
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
                className={`px-2 py-0.5 rounded-full text-[10px] font-medium border transition-all shrink-0 whitespace-nowrap ${
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
                <div className="flex items-center gap-2 min-w-0 flex-1">
                  {/* Avatar — lazy loaded with IntersectionObserver */}
                  <LazyAvatar contactId={c.id} alias={c.alias} chatType={c.chat_type} hasError={avatarErrors.has(c.id)} onError={() => setAvatarErrors((prev) => new Set(prev).add(c.id))} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5">
                      <span className={`font-medium text-sm truncate ${unread.has(c.id) ? "text-white" : ""}`}>{c.alias}</span>
                      {unread.has(c.id) && (
                        <span className="min-w-[20px] h-5 px-1.5 rounded-full bg-brand text-white text-[11px] font-bold flex items-center justify-center shrink-0">
                          {unread.get(c.id)! > 99 ? "99+" : unread.get(c.id)}
                        </span>
                      )}
                    </div>
                    {c.last_message_content && (
                      <p className={`text-xs truncate mt-0.5 flex items-center gap-1 ${
                        !unread.has(c.id) && c.last_message_direction === "incoming"
                          ? "text-white font-medium"
                          : "text-slate-500"
                      }`}>
                        {c.last_message_direction === "outgoing" && (
                          <svg className={`w-3.5 h-3.5 shrink-0 ${c.last_message_is_read ? "text-sky-400" : ""}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                            {c.last_message_is_read ? (
                              <><polyline points="1 12 5 16 12 6" /><polyline points="8 12 12 16 20 6" /></>
                            ) : (
                              <polyline points="4 12 9 17 20 6" />
                            )}
                          </svg>
                        )}
                        <span className="truncate">{c.last_message_content}</span>
                      </p>
                    )}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1 shrink-0">
                  {c.last_message_at && (
                    <span className={`text-xs ${unread.has(c.id) ? "text-brand font-medium" : "text-slate-500"}`}>
                      {(() => {
                        const d = new Date(c.last_message_at);
                        const now = new Date();
                        const tzNow = new Date(now.toLocaleString("en-US", { timeZone: userTimezone }));
                        const tzD = new Date(d.toLocaleString("en-US", { timeZone: userTimezone }));
                        const isToday = tzD.toDateString() === tzNow.toDateString();
                        return isToday
                          ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZone: userTimezone })
                          : d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", timeZone: userTimezone });
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
                      <div className="text-[10px] text-slate-500 truncate">
                        {selected.chat_type !== "private" ? selected.chat_type : ""}
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

              {/* Translation language selectors */}
              <select
                value={translateLangIn}
                onChange={(e) => setTranslateLangIn(e.target.value)}
                className="px-2.5 py-1.5 rounded-lg border border-surface-border bg-surface-card text-xs text-slate-400 focus:outline-none focus:border-brand/30 cursor-pointer shrink-0"
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
                className="px-2.5 py-1.5 rounded-lg border border-surface-border bg-surface-card text-xs text-slate-400 focus:outline-none focus:border-brand/30 cursor-pointer shrink-0"
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

              {/* User info toggle */}
              <button
                onClick={() => setShowUserInfo(!showUserInfo)}
                className={`p-1.5 rounded-lg border transition-all duration-200 shrink-0 ${
                  showUserInfo
                    ? "bg-brand/10 border-brand/30 text-brand"
                    : "border-surface-border text-slate-500 hover:text-brand hover:border-brand/30"
                }`}
                title="Информация о контакте"
              >
                <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" />
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
                      className={`max-w-[75%] min-w-0 select-none ${m.direction === "outgoing" ? "ml-auto" : ""}`}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        const menuH = 280;
                        const menuW = 200;
                        const y = e.clientY + menuH > window.innerHeight ? e.clientY - menuH : e.clientY;
                        const x = Math.min(e.clientX, window.innerWidth - menuW);
                        setContextMenu({ x, y, message: m });
                      }}
                      onTouchStart={(e) => {
                        longPressTriggered.current = false;
                        longPressTimer.current = setTimeout(() => {
                          longPressTriggered.current = true;
                          const touch = e.touches[0];
                          const menuH = 280;
                          const menuW = 200;
                          const y = touch.clientY + menuH > window.innerHeight ? touch.clientY - menuH : touch.clientY;
                          const x = Math.min(touch.clientX, window.innerWidth - menuW);
                          setContextMenu({ x, y, message: m });
                        }, 500);
                      }}
                      onTouchEnd={() => { if (longPressTimer.current) { clearTimeout(longPressTimer.current); longPressTimer.current = null; } }}
                      onTouchMove={() => { if (longPressTimer.current) { clearTimeout(longPressTimer.current); longPressTimer.current = null; } }}
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
                        className={`rounded-2xl text-sm overflow-hidden break-words ${
                          m.media_type === "sticker"
                            ? "bg-transparent p-1"
                            : m.is_deleted
                            ? "px-3.5 py-2.5 bg-red-500/20 border border-red-500/40 text-red-200 rounded-br-md"
                            : m.is_edited
                            ? "px-3.5 py-2.5 bg-amber-500/15 border border-amber-400/40 text-amber-100 rounded-br-md"
                            : m.direction === "outgoing"
                            ? "px-3.5 py-2.5 bg-gradient-to-br from-brand to-brand-dark text-white rounded-br-md shadow-[0_2px_8px_rgba(14,165,233,0.2)]"
                            : "px-3.5 py-2.5 bg-surface-card border border-surface-border text-white rounded-bl-md"
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

                        {/* Sticker — just label */}
                        {m.media_type === "sticker" && (
                          <div className={`text-xs italic ${m.direction === "outgoing" ? "text-white/50" : "text-slate-400"}`}>Стикер {m.content || ""}</div>
                        )}
                        {m.media_type && m.media_type !== "sticker" && m.media_path && (
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
                              <VoicePlayer src={mediaUrl(m.media_path)} direction={m.direction} />
                            )}
                            {m.media_type === "document" && (() => {
                              const fname = m.media_path!.split('/').pop() || '';
                              const ext = fname.split('.').pop()?.toLowerCase() || '';
                              const isImage = ['jpg','jpeg','png','gif','webp','bmp','svg'].includes(ext) || fname.startsWith('photo_');
                              return isImage ? (
                                <img
                                  src={mediaUrl(m.media_path!)}
                                  alt=""
                                  loading="lazy"
                                  className="rounded-xl max-w-full max-h-64 object-cover cursor-pointer hover:opacity-90 transition-opacity"
                                  onClick={(e) => { e.stopPropagation(); setLightboxSrc(mediaUrl(m.media_path!)); }}
                                />
                              ) : (
                                <a href={mediaUrl(m.media_path)} target="_blank" rel="noreferrer" download className="flex items-center gap-2 text-brand-light hover:underline">
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

                        {/* Content (skip for stickers — already shown above) */}
                        {m.content && m.media_type !== "sticker" && <span className={`break-words whitespace-pre-wrap [overflow-wrap:anywhere] ${m.is_deleted ? "line-through" : ""}`}>{m.content}</span>}

                        {/* Translation */}
                        {translations.has(m.id) && (
                          <div className={`mt-1.5 pt-1.5 border-t text-xs italic ${m.direction === "outgoing" ? "border-white/20 text-white/60" : "border-surface-border text-slate-400"}`}>
                            🌐 {translations.get(m.id)}
                          </div>
                        )}

                        {/* Timestamp + translate + edited + read status */}
                        <div className={`flex items-center justify-end gap-1 text-[10px] mt-1 ${m.direction === "outgoing" ? "text-white/40" : "text-slate-500"}`}>
                          {/* Translate button — text-only messages */}
                          {m.content && !m.media_type && !translations.has(m.id) && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleTranslate(m.id, m.content!, m.direction); }}
                              className={`hover:text-brand transition-colors ${translating === m.id ? "animate-pulse" : ""}`}
                              title="Перевести"
                            >
                              <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <path d="M5 8l6 6" /><path d="M4 14l6-6 2-3" /><path d="M2 5h12" /><path d="M7 2h1" />
                                <path d="M22 22l-5-10-5 10" /><path d="M14 18h6" />
                              </svg>
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
                          {m.is_edited && (
                            <button
                              onClick={(e) => { e.stopPropagation(); showEditHistory(selected!.id, m.id); }}
                              className="italic mr-1 text-amber-400/70 hover:text-amber-300 cursor-pointer transition-colors"
                              title="Показать историю изменений"
                            >
                              (ред.)
                            </button>
                          )}
                          {new Date(m.created_at).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", timeZone: userTimezone })}
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
                                        try {
                                          const u = new URL(btn.url, window.location.href);
                                          if (u.protocol === "http:" || u.protocol === "https:") {
                                            window.open(btn.url, "_blank", "noopener,noreferrer");
                                          }
                                        } catch {}
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
            {showTemplates && templates.length > 0 && (() => {
              const acctFiltered = templates.filter((tpl) => {
                if (filterAccountId && tpl.tg_account_id && tpl.tg_account_id !== filterAccountId) return false;
                if (!text.startsWith("/")) return true;
                const q = text.toLowerCase();
                return (tpl.shortcut && tpl.shortcut.toLowerCase().startsWith(q)) || tpl.title.toLowerCase().includes(q.slice(1));
              });
              const categories = [...new Set(acctFiltered.map((t) => t.category).filter(Boolean))] as string[];
              return (
                <div className="px-4 py-2 border-t border-surface-border bg-surface-card/50 max-h-48 overflow-auto animate-slide-up">
                  <div className="text-[10px] text-slate-500 mb-1.5 font-medium flex items-center gap-2 flex-wrap">
                    <span>Шаблоны</span>
                    {text.startsWith("/") && <span className="text-brand">— введите шорткат и Enter</span>}
                  </div>
                  {categories.length > 0 && (
                    <div className="flex gap-1 mb-1.5 flex-wrap">
                      <button
                        onClick={() => setTplCategory(null)}
                        className={`px-2 py-0.5 rounded-full text-[10px] font-medium border transition-colors ${!tplCategory ? "bg-brand/20 text-brand border-brand/30" : "text-slate-500 border-surface-border hover:border-slate-600"}`}
                      >Все</button>
                      {categories.map((cat) => (
                        <button
                          key={cat}
                          onClick={() => setTplCategory(tplCategory === cat ? null : cat)}
                          className={`px-2 py-0.5 rounded-full text-[10px] font-medium border transition-colors ${tplCategory === cat ? "bg-brand/20 text-brand border-brand/30" : "text-slate-500 border-surface-border hover:border-slate-600"}`}
                        >{cat}</button>
                      ))}
                    </div>
                  )}
                  <div className="space-y-1">
                    {acctFiltered.filter((tpl) => !tplCategory || tpl.category === tplCategory).map((tpl) => {
                      const isScript = tpl.content.includes("\n---\n");
                      return (
                        <button
                          key={tpl.id}
                          onClick={() => applyTemplate(tpl)}
                          className="w-full text-left px-2.5 py-1.5 rounded-lg text-xs hover:bg-surface-hover transition-colors border border-transparent hover:border-surface-border"
                        >
                          <span className="text-brand font-medium">{tpl.title}</span>
                          {isScript && <span className="ml-1 text-amber-400" title="Скрипт (несколько сообщений)">📜</span>}
                          {tpl.media_type && <span className="ml-1">{tpl.media_type === "photo" ? "📷" : tpl.media_type === "video" ? "🎬" : tpl.media_type === "video_note" ? "🔵" : tpl.media_type === "voice" ? "🎤" : "📄"}</span>}
                          {tpl.shortcut && <span className="text-slate-600 ml-1 font-mono">{tpl.shortcut}</span>}
                          {tpl.category && <span className="text-purple-400/60 ml-1 text-[10px]">{tpl.category}</span>}
                          <span className="text-slate-500 ml-2 truncate">{tpl.content.slice(0, 50)}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })()}

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

            {/* Pending files preview */}
            {pendingFiles.length > 0 && (
              <div className="px-3 py-2 border-t border-surface-border/50 bg-surface/50 shrink-0 animate-slide-up">
                <div className="flex items-center gap-2 overflow-x-auto">
                  {pendingFiles.map((file, idx) => (
                    <div key={idx} className="relative shrink-0 group">
                      {file.type.startsWith("image/") ? (
                        <img src={URL.createObjectURL(file)} alt="" className="w-16 h-16 rounded-lg object-cover border border-surface-border" />
                      ) : file.type.startsWith("video/") ? (
                        <div className="w-16 h-16 rounded-lg border border-surface-border bg-surface-card flex items-center justify-center">
                          <svg className="w-6 h-6 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polygon points="5 3 19 12 5 21 5 3" /></svg>
                        </div>
                      ) : (
                        <div className="w-16 h-16 rounded-lg border border-surface-border bg-surface-card flex flex-col items-center justify-center p-1">
                          <svg className="w-5 h-5 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" /><polyline points="14 2 14 8 20 8" /></svg>
                          <span className="text-[8px] text-slate-500 truncate w-full text-center mt-0.5">{file.name.split('.').pop()}</span>
                        </div>
                      )}
                      <button
                        onClick={() => removePendingFile(idx)}
                        className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-red-500 text-white rounded-full flex items-center justify-center text-xs opacity-0 group-hover:opacity-100 transition-opacity"
                      >
                        ✕
                      </button>
                    </div>
                  ))}
                  {pendingFiles.length < 5 && (
                    <button
                      onClick={() => fileInputRef.current?.click()}
                      className="w-16 h-16 rounded-lg border border-dashed border-surface-border flex items-center justify-center text-slate-500 hover:text-brand hover:border-brand/30 transition-colors shrink-0"
                    >
                      <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></svg>
                    </button>
                  )}
                </div>
                <div className="text-[10px] text-slate-500 mt-1">{pendingFiles.length}/5 файлов</div>
              </div>
            )}

            {/* Input */}
            <div className="px-2 py-1.5 md:p-3 border-t border-surface-border bg-surface-card shrink-0">
              <div className="flex gap-1 items-center bg-surface-card border border-surface-border rounded-2xl px-1">
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.zip"
                  onChange={handleFileUpload}
                  className="hidden"
                  multiple
                />
                <div className="relative shrink-0">
                  {/* Translate button — floats above attach when typing real text */}
                  {(() => { const s = text.replace(/[\s\d]/g, ""); return s.length > 0; })() && (
                    <button
                      onClick={async (e) => {
                        e.stopPropagation();
                        if (!text.trim()) return;
                        setTranslatingInput(true);
                        try {
                          const result = await translateText(text, translateLangOut);
                          setText(result.translated);
                        } catch (err: any) { alert(err.message); }
                        setTranslatingInput(false);
                      }}
                      disabled={translatingInput}
                      className={`absolute -top-10 left-1/2 -translate-x-1/2 w-8 h-8 rounded-full bg-surface-card border border-surface-border flex items-center justify-center shadow-lg hover:border-brand/40 transition-all animate-scale-in ${translatingInput ? "animate-pulse" : ""}`}
                      title={`Перевести на ${translateLangOut.toUpperCase()}`}
                    >
                      <svg className="w-3.5 h-3.5 text-brand" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M5 8l6 6" /><path d="M4 14l6-6 2-3" /><path d="M2 5h12" /><path d="M7 2h1" />
                        <path d="M22 22l-5-10-5 10" /><path d="M14 18h6" />
                      </svg>
                    </button>
                  )}
                  <button
                    onClick={() => fileInputRef.current?.click()}
                    className="text-slate-500 hover:text-brand transition-colors p-2"
                    title="Прикрепить файл"
                  >
                    <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48" />
                    </svg>
                  </button>
                </div>

                {/* Dropdown menu: emoji, scheduled */}
                <div className="relative input-menu-container shrink-0">
                  <button
                    onClick={() => { setShowInputMenu(!showInputMenu); setShowEmoji(false); setShowTemplates(false); }}
                    className={`p-2 transition-colors ${showInputMenu ? "text-brand" : "text-slate-500 hover:text-brand"}`}
                    title="Ещё"
                  >
                    <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="1" /><circle cx="12" cy="5" r="1" /><circle cx="12" cy="19" r="1" />
                    </svg>
                  </button>
                  {showInputMenu && (
                    <div className="absolute bottom-full left-0 mb-2 bg-surface-card border border-surface-border rounded-xl shadow-2xl py-1 min-w-[200px] z-50 animate-slide-up">
                      <button
                        onClick={() => { setShowEmoji(!showEmoji); setShowTemplates(false); setShowInputMenu(false); }}
                        className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
                      >
                        <span className="text-base">😊</span> Эмодзи
                      </button>
                      <button
                        onClick={() => {
                          setShowInputMenu(false);
                          setScheduleMode(true);
                          // Default to now + 1 hour
                          const d = new Date(Date.now() + 3600000);
                          setScheduleDate(d.toISOString().split("T")[0]);
                          setScheduleTime(d.toTimeString().slice(0, 5));
                        }}
                        className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
                      >
                        <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
                        </svg>
                        Отложенное сообщение
                      </button>
                      {scheduledList.length > 0 && (
                        <button
                          onClick={() => { setShowScheduledList(true); setShowInputMenu(false); }}
                          className="w-full px-4 py-2.5 text-left text-sm text-amber-400 hover:bg-surface-hover flex items-center gap-3 transition-colors"
                        >
                          <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <rect x="3" y="4" width="18" height="18" rx="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" />
                          </svg>
                          Запланировано ({scheduledList.length})
                        </button>
                      )}
                      <button
                        onClick={() => { setShowTemplates(!showTemplates); setShowEmoji(false); setShowInputMenu(false); }}
                        className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
                      >
                        <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
                          <polyline points="14 2 14 8 20 8" />
                          <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
                        </svg>
                        Шаблоны
                      </button>
                    </div>
                  )}
                </div>

                <textarea
                  ref={inputRef}
                  defaultValue={text}
                  onChange={(e) => {
                    const val = e.target.value;
                    textRef.current = val;
                    // Debounced state update — only update when needed for UI elements
                    const hasRealText = val.replace(/[\s\d]/g, "").length > 0;
                    const hadRealText = text.replace(/[\s\d]/g, "").length > 0;
                    if (hasRealText !== hadRealText) _setText(val);
                    // Template toggle
                    const shouldShow = val.startsWith("/") && val.length >= 1;
                    if (shouldShow && !showTemplates) {
                      setShowTemplates(true);
                      setShowEmoji(false);
                    } else if (!shouldShow && showTemplates) {
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
                  onFocus={() => {
                    // On mobile, scroll to bottom after keyboard opens
                    // Multiple delays to catch different keyboard animation speeds
                    for (const delay of [100, 300, 500, 800]) {
                      setTimeout(() => {
                        messagesEndRef.current?.scrollIntoView({ behavior: "instant" });
                        // Also ensure input is visible
                        if (window.visualViewport) {
                          const vv = window.visualViewport;
                          document.documentElement.style.height = `${vv.height}px`;
                        }
                      }, delay);
                    }
                  }}
                  onBlur={() => {
                    // Reset height when keyboard closes
                    document.documentElement.style.height = "";
                  }}
                  onInput={(e) => {
                    const target = e.target as HTMLTextAreaElement;
                    target.style.height = "auto";
                    target.style.height = Math.min(target.scrollHeight, 128) + "px";
                  }}
                />
                <button
                  onClick={sendMessage}
                  disabled={(!text.trim() && pendingFiles.length === 0) || sending}
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

      {/* User info sidebar */}
      {showUserInfo && selected && (
        <div className="w-full md:w-80 border-l border-surface-border flex flex-col shrink-0 overflow-hidden animate-slide-right fixed md:relative inset-0 md:inset-y-0 md:right-0 md:left-auto z-40 bg-surface-card">
          {/* Header */}
          <div className="p-4 border-b border-surface-border flex items-center justify-between shrink-0">
            <span className="text-sm font-semibold">Контакт</span>
            <button onClick={() => setShowUserInfo(false)} className="text-slate-500 hover:text-white p-1">
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
              </svg>
            </button>
          </div>

          {/* Avatar + name + info */}
          <div className="p-4 flex flex-col items-center text-center border-b border-surface-border shrink-0">
            <div className="w-16 h-16 rounded-full bg-surface border border-surface-border overflow-hidden mb-3">
              <img
                src={avatarUrl(selected.id)}
                alt=""
                className="w-full h-full object-cover"
                onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
              />
            </div>
            <div className="text-sm font-semibold text-white">{selected.alias}</div>
            {selected.real_tg_id && (
              <div className="text-[11px] text-slate-500 mt-1">ID: {selected.real_tg_id}</div>
            )}
            <div className="text-[11px] text-slate-500 mt-0.5">
              Первое сообщение: {selected.created_at ? new Date(selected.created_at).toLocaleDateString("ru-RU", { timeZone: userTimezone }) : "—"}
            </div>
            {selected.tags.length > 0 && (
              <div className="flex gap-1 mt-2 flex-wrap justify-center">
                {selected.tags.map((t) => (
                  <span key={t} className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-brand/10 text-brand border border-brand/20">{t}</span>
                ))}
              </div>
            )}
          </div>

          {/* Tabs */}
          <div className="flex border-b border-surface-border shrink-0">
            {(["media", "notes", "postbacks"] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setUserInfoTab(tab)}
                className={`flex-1 py-2 text-xs font-medium transition-colors ${
                  userInfoTab === tab ? "text-brand border-b-2 border-brand" : "text-slate-500 hover:text-slate-300"
                }`}
              >
                {tab === "media" ? "Медиа" : tab === "notes" ? "Заметки" : "Постбеки"}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-auto p-3">
            {userInfoTab === "media" && (() => {
              const mediaMessages = messages.filter((m) => m.media_path && m.media_type && m.media_type !== "sticker");
              const photos = mediaMessages.filter((m) => m.media_type === "photo" || (m.media_type === "document" && (() => { const ext = (m.media_path || "").split(".").pop()?.toLowerCase() || ""; return ["jpg","jpeg","png","gif","webp"].includes(ext); })()));
              const videos = mediaMessages.filter((m) => m.media_type === "video" && !(m.media_path || "").endsWith(".webm"));
              const files = mediaMessages.filter((m) => m.media_type === "document" && !photos.includes(m));
              const voices = mediaMessages.filter((m) => m.media_type === "voice");
              const subTabs = [
                { key: "photos" as const, label: "Фото", count: photos.length },
                { key: "videos" as const, label: "Видео", count: videos.length },
                { key: "files" as const, label: "Файлы", count: files.length },
                { key: "voice" as const, label: "Голос", count: voices.length },
              ];
              return (
                <div>
                  <div className="flex gap-1 mb-3 bg-surface border border-surface-border rounded-lg p-0.5">
                    {subTabs.map((st) => (
                      <button
                        key={st.key}
                        onClick={() => setMediaSubTab(st.key)}
                        className={`flex-1 py-1.5 rounded-md text-[10px] font-medium transition-all ${
                          mediaSubTab === st.key ? "bg-brand/15 text-brand" : "text-slate-500 hover:text-slate-300"
                        }`}
                      >
                        {st.label} {st.count > 0 && <span className="opacity-60">({st.count})</span>}
                      </button>
                    ))}
                  </div>

                  {mediaSubTab === "photos" && (
                    photos.length > 0 ? (
                      <div className="grid grid-cols-3 gap-1">
                        {photos.map((m) => (
                          <img key={m.id} src={mediaUrl(m.media_path!)} alt="" className="w-full aspect-square object-cover rounded-lg cursor-pointer hover:opacity-80 transition-opacity" onClick={() => setLightboxSrc(mediaUrl(m.media_path!))} />
                        ))}
                      </div>
                    ) : <p className="text-xs text-slate-500 text-center py-6">Нет фотографий</p>
                  )}

                  {mediaSubTab === "videos" && (
                    videos.length > 0 ? (
                      <div className="space-y-2">
                        {videos.map((m) => (
                          <video key={m.id} src={mediaUrl(m.media_path!)} controls preload="none" className="w-full rounded-lg" />
                        ))}
                      </div>
                    ) : <p className="text-xs text-slate-500 text-center py-6">Нет видео</p>
                  )}

                  {mediaSubTab === "files" && (
                    files.length > 0 ? (
                      <div className="space-y-1">
                        {files.map((m) => (
                          <a key={m.id} href={mediaUrl(m.media_path!)} target="_blank" rel="noreferrer" download
                            className="flex items-center gap-2 px-2.5 py-2 rounded-lg border border-surface-border hover:border-brand/30 text-xs text-slate-300 hover:text-brand transition-colors">
                            <svg className="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" /><polyline points="14 2 14 8 20 8" /></svg>
                            <span className="truncate">{m.media_path!.split("/").pop()}</span>
                          </a>
                        ))}
                      </div>
                    ) : <p className="text-xs text-slate-500 text-center py-6">Нет файлов</p>
                  )}

                  {mediaSubTab === "voice" && (
                    voices.length > 0 ? (
                      <div className="space-y-2">
                        {voices.map((m) => (
                          <VoicePlayer key={m.id} src={mediaUrl(m.media_path!)} direction={m.direction} />
                        ))}
                      </div>
                    ) : <p className="text-xs text-slate-500 text-center py-6">Нет голосовых</p>
                  )}
                </div>
              );
            })()}

            {userInfoTab === "notes" && (
              <div className="space-y-3">
                <textarea
                  value={contactNotes}
                  onChange={(e) => setContactNotes(e.target.value)}
                  placeholder="Заметки об этом контакте..."
                  rows={6}
                  className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50 resize-none"
                />
                <button
                  onClick={async () => {
                    if (!selected) return;
                    setSavingNotes(true);
                    try {
                      await api(`/api/contacts/${selected.id}`, {
                        method: "PATCH",
                        body: JSON.stringify({ notes: contactNotes }),
                      });
                    } catch (e: any) { alert(e.message); }
                    setSavingNotes(false);
                  }}
                  disabled={savingNotes}
                  className="w-full py-2 rounded-xl text-xs font-medium bg-brand/10 text-brand border border-brand/20 hover:bg-brand/20 transition-colors disabled:opacity-50"
                >
                  {savingNotes ? "Сохранение..." : "Сохранить заметки"}
                </button>
              </div>
            )}

            {userInfoTab === "postbacks" && (
              <div className="flex flex-col items-center justify-center py-8 text-slate-500">
                <svg className="w-8 h-8 mb-2 text-slate-600" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z" /><polyline points="13 2 13 9 20 9" />
                </svg>
                <p className="text-xs">В разработке</p>
              </div>
            )}
          </div>
        </div>
      )}

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
                  {messages.some((m) => forwardSelected.has(m.id) && m.media_path) && (
                    <label className="flex items-center gap-2 cursor-pointer text-sm text-slate-300">
                      <input
                        type="checkbox"
                        checked={forwardMediaOnly}
                        onChange={(e) => setForwardMediaOnly(e.target.checked)}
                        className="w-4 h-4 rounded border-surface-border accent-brand"
                      />
                      Только медиа (без текста)
                    </label>
                  )}
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
                  <option key={acc.id} value={acc.id}>{(acc as any).display_name || acc.phone}</option>
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

      {/* Context menu (right-click / long-press) */}
      {contextMenu && (
        <div
          className="fixed z-[70] bg-surface-card border border-surface-border rounded-xl shadow-2xl py-1 min-w-[190px] max-w-[calc(100vw-16px)] animate-scale-in"
          style={{
            left: Math.min(contextMenu.x, window.innerWidth - 200),
            top: Math.max(8, Math.min(contextMenu.y, window.innerHeight - 300)),
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Reply */}
          <button
            onClick={() => { setReplyTo(contextMenu.message); setContextMenu(null); inputRef.current?.focus(); }}
            className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
          >
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="9 17 4 12 9 7" /><path d="M20 18v-2a4 4 0 00-4-4H4" />
            </svg>
            Ответить
          </button>
          {/* Copy */}
          {contextMenu.message.content && (
            <button
              onClick={() => { navigator.clipboard.writeText(contextMenu.message.content!); setContextMenu(null); }}
              className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
              </svg>
              Копировать
            </button>
          )}
          {/* Translate */}
          {contextMenu.message.content && !translations.has(contextMenu.message.id) && (
            <button
              onClick={() => { handleTranslate(contextMenu.message.id, contextMenu.message.content!, contextMenu.message.direction); setContextMenu(null); }}
              className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M5 8l6 6" /><path d="M4 14l6-6 2-3" /><path d="M2 5h12" /><path d="M7 2h1" />
                <path d="M22 22l-5-10-5 10" /><path d="M14 18h6" />
              </svg>
              Перевести
            </button>
          )}
          {/* Edit (outgoing only) */}
          {contextMenu.message.direction === "outgoing" && contextMenu.message.content && !contextMenu.message.is_deleted && (
            <button
              onClick={() => { setEditingMsg(contextMenu.message); setEditText(contextMenu.message.content || ""); setContextMenu(null); }}
              className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
              </svg>
              Редактировать
            </button>
          )}
          {/* Forward */}
          <button
            onClick={() => {
              setForwardMode(true);
              setForwardSelected(new Set([contextMenu.message.id]));
              setContextMenu(null);
            }}
            className="w-full px-4 py-2.5 text-left text-sm text-slate-300 hover:bg-surface-hover flex items-center gap-3 transition-colors"
          >
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="15 17 20 12 15 7" /><path d="M4 18v-2a4 4 0 014-4h12" />
            </svg>
            Переслать
          </button>
          {/* Delete (outgoing only) */}
          {contextMenu.message.direction === "outgoing" && !contextMenu.message.is_deleted && (
            <button
              onClick={async () => {
                if (!confirm("Удалить сообщение?")) return;
                try {
                  await api(`/api/messages/${selected!.id}/delete/${contextMenu.message.id}`, { method: "DELETE" });
                  setMessages((prev) => prev.map((msg) => msg.id === contextMenu.message.id ? { ...msg, is_deleted: true } : msg));
                } catch {}
                setContextMenu(null);
              }}
              className="w-full px-4 py-2.5 text-left text-sm text-red-400 hover:bg-red-500/10 flex items-center gap-3 transition-colors"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
              </svg>
              Удалить
            </button>
          )}
        </div>
      )}

      {/* Scheduled message modal */}
      {scheduleMode && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => setScheduleMode(false)}>
          <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-sm mx-4 p-5 animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <h3 className="text-lg font-semibold mb-1">Отложенное сообщение</h3>
            <p className="text-xs text-slate-500 mb-4">Часовой пояс: {userTimezone}</p>
            <div className="space-y-3">
              <div>
                <label className="text-xs text-slate-400 mb-1 block">Дата</label>
                <input
                  type="date"
                  value={scheduleDate}
                  onChange={(e) => setScheduleDate(e.target.value)}
                  className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50"
                />
              </div>
              <div>
                <label className="text-xs text-slate-400 mb-1 block">Время</label>
                <input
                  type="time"
                  value={scheduleTime}
                  onChange={(e) => setScheduleTime(e.target.value)}
                  className="w-full bg-surface border border-surface-border rounded-xl px-3 py-2.5 text-sm focus:outline-none focus:border-brand/50"
                />
              </div>
            </div>
            {!text.trim() && <p className="text-xs text-amber-400/70 mt-3">Сначала введите сообщение в поле ввода внизу</p>}
            <div className="flex gap-2 justify-end mt-4">
              <Button variant="ghost" onClick={() => setScheduleMode(false)}>Отмена</Button>
              <Button
                disabled={!text.trim() || !scheduleDate || !scheduleTime}
                onClick={async () => {
                  if (!selected || !text.trim() || !scheduleDate || !scheduleTime) return;
                  try {
                    const sm = await api(`/api/messages/${selected.id}/schedule`, {
                      method: "POST",
                      body: JSON.stringify({
                        content: text.trim(),
                        scheduled_at: `${scheduleDate}T${scheduleTime}:00`,
                        timezone: userTimezone,
                      }),
                    });
                    setScheduledList((prev) => [...prev, sm]);
                    setText("");
                    setScheduleMode(false);
                    setScheduleDate("");
                    setScheduleTime("");
                  } catch (e: any) { alert(e.message); }
                }}
              >
                Запланировать
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Scheduled messages list modal */}
      {showScheduledList && (
        <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 animate-fade-in" onClick={() => setShowScheduledList(false)}>
          <div className="bg-surface-card border border-surface-border rounded-2xl w-full max-w-md mx-4 max-h-[70vh] flex flex-col animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <div className="p-4 border-b border-surface-border flex items-center justify-between">
              <h3 className="font-semibold">Запланированные сообщения</h3>
              <button onClick={() => setShowScheduledList(false)} className="text-slate-500 hover:text-white p-1">
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4 space-y-3">
              {scheduledList.length === 0 && (
                <p className="text-sm text-slate-500 text-center py-4">Нет запланированных сообщений</p>
              )}
              {scheduledList.map((sm) => (
                <div key={sm.id} className="border border-surface-border rounded-xl p-3 space-y-2 animate-fade-in">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0 flex-1">
                      <div className="text-xs text-brand font-medium">{sm.contact_alias || "—"}</div>
                      <div className="text-sm text-white mt-1 break-words">{sm.content || "[медиа]"}</div>
                    </div>
                    <button
                      onClick={() => cancelScheduled(sm.id)}
                      className="text-red-400 hover:text-red-300 p-1 shrink-0"
                      title="Отменить"
                    >
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <circle cx="12" cy="12" r="10" /><line x1="15" y1="9" x2="9" y2="15" /><line x1="9" y1="9" x2="15" y2="15" />
                      </svg>
                    </button>
                  </div>
                  <div className="flex items-center gap-2 text-[11px] text-slate-500">
                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>
                    {new Date(sm.scheduled_at).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit", timeZone: sm.timezone || userTimezone })}
                    <span className="text-slate-600">({sm.timezone})</span>
                  </div>
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

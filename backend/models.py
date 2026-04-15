import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy import UniqueConstraint

from config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    # 120 total (60+60). Previous 50 was exhausted during Telethon message
    # bursts — many concurrent listener coroutines holding a session each
    # while awaiting an UPDATE that's blocked on a row-level lock (every
    # incoming TG message triggers UPDATE contacts SET last_message_*).
    # Pool exhaustion then blocked all HTTP requests (auth etc) → 500s.
    pool_size=60,
    max_overflow=60,
    pool_recycle=1800,
    pool_pre_ping=True,
)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Staff(Base):
    __tablename__ = "staff"
    __table_args__ = (
        UniqueConstraint('postforge_user_id', 'postforge_org_id', name='uq_staff_pf_user_org'),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tg_user_id = Column(BigInteger, unique=True, nullable=False)
    tg_username = Column(String, nullable=True)
    role = Column(String, nullable=False)  # super_admin | admin | operator
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())

    # PostForge SSO link
    postforge_user_id = Column(String, nullable=True)
    postforge_org_id = Column(String, nullable=True, index=True)

    # Real Telegram user ID (for Mini App auth across org contexts)
    real_tg_id = Column(BigInteger, nullable=True, index=True)

    # Signature: "named" (shows staff name) or "anonymous" (hides)
    signature_mode = Column(String, default="named")

    # Per-staff timezone (IANA, e.g. "Europe/Moscow")
    timezone = Column(String, default="UTC")

    # Admin setting: show real contact names to operators (default: aliases only)
    show_real_names = Column(Boolean, default=True)

    # CRM super-admin flag. Separate from the `role` field (which is per-org,
    # auto-assigned by PostForge SSO). This flag is ONLY granted by an explicit
    # toggle in the PostForge admin panel (beta_features.crm_admin on the
    # PostForge User). Synced on every SSO login. A CRM super-admin can see
    # the audit log, cross-org stats, and account debugging view.
    is_crm_admin = Column(Boolean, default=False, nullable=False)

    assigned_contacts = relationship("Contact", back_populates="assigned_operator")


class TgAccount(Base):
    __tablename__ = "tg_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    phone = Column(String, unique=True, nullable=False)
    session_file = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    connected_at = Column(DateTime, default=func.now())
    org_id = Column(String, nullable=True, index=True)

    session_string = Column(Text, nullable=True)  # Telethon StringSession (replaces SQLite file)
    display_name = Column(String, nullable=True)
    show_real_names = Column(Boolean, default=True)
    disconnected_at = Column(DateTime, nullable=True)

    # Auto-tag: tag names applied to every NEW private contact on first message.
    # JSON array of tag name strings, e.g. ["RD", "new_lead"].
    auto_tags = Column(ARRAY(String), default=lambda: [])

    # Auto-greeting: template ID sent automatically when a new contact writes
    # their first message. NULL = no greeting. References message_templates.id.
    auto_greeting_template_id = Column(UUID(as_uuid=True), nullable=True)

    contacts = relationship("Contact", back_populates="tg_account")


class Contact(Base):
    __tablename__ = "contacts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id"))

    # Real data (encrypted)
    real_tg_id = Column(BigInteger, nullable=False)
    real_name_encrypted = Column(Text)
    real_username_encrypted = Column(Text)

    group_title_encrypted = Column(Text, nullable=True)
    chat_type = Column(String, nullable=False, default="private")  # private | group | channel | supergroup
    is_forum = Column(Boolean, default=False)  # Supergroup with topics

    # Public data
    alias = Column(String, nullable=False, unique=True)
    status = Column(String, default="approved")  # approved | blocked

    # CRM data
    assigned_to = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    tags = Column(ARRAY(String), default=lambda: [])
    notes = Column(Text)
    is_archived = Column(Boolean, default=False)
    # Pin status synced FROM Telegram. Represents Telegram's native pin state,
    # not the CRM's manual PinnedChat table (which is per-staff). Refreshed on
    # every _do_sync_dialogs run.
    is_pinned = Column(Boolean, default=False, nullable=False)

    # Mute state synced FROM Telegram. If the user muted this chat in the
    # native Telegram client, we skip the in-CRM notification toast so the
    # CRM doesn't alert on chats the user has already silenced elsewhere.
    # Refreshed on every _do_sync_dialogs run from
    # `dialog.notify_settings.mute_until > now`.
    is_muted = Column(Boolean, default=False, nullable=False)

    # CRM-local mute toggle. Independent of `is_muted` (which is TG-synced).
    # Lets operators silence a chat inside the CRM without touching the
    # Telegram client. The effective mute state for notification/icon
    # purposes is `is_muted OR crm_muted`.
    crm_muted = Column(Boolean, default=False, nullable=False)

    # Telegram stripped profile thumbnail as a data URL (`data:image/jpeg;base64,...`).
    # ~700-1200 bytes, blurry 60x60 — shown instantly in the chat list while the
    # full avatar loads in the background. Refreshed on every _do_sync_dialogs
    # run from `entity.photo.stripped_thumb`.
    avatar_thumb = Column(Text, nullable=True)

    # Denormalized last-message preview fields. Populated on every new
    # message (listener + CRM send API) + on read events. `list_contacts`
    # used to recompute these on every call via a per-contact subquery
    # over `messages` that scanned O(total_messages) rows; the denorm
    # reduces the hot path to a single indexed scan of `contacts`.
    last_message_content = Column(String(200), nullable=True)
    last_message_direction = Column(String, nullable=True)  # "incoming" | "outgoing"
    last_message_is_read = Column(Boolean, nullable=True)

    created_at = Column(DateTime, default=func.now())
    approved_at = Column(DateTime, nullable=True)
    last_message_at = Column(DateTime, nullable=True)

    tg_account = relationship("TgAccount", back_populates="contacts")
    assigned_operator = relationship("Staff", back_populates="assigned_contacts")
    messages = relationship("Message", back_populates="contact", order_by="Message.created_at")


class Message(Base):
    __tablename__ = "messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    tg_message_id = Column(Integer, nullable=True)
    direction = Column(String, nullable=False)  # incoming | outgoing
    content = Column(Text)
    media_type = Column(String, nullable=True)  # photo | video | document | voice | None
    media_path = Column(String, nullable=True)  # relative path in /media/
    sent_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    is_read = Column(Boolean, default=False)
    is_deleted = Column(Boolean, default=False)
    is_edited = Column(Boolean, default=False)

    # Reply-to
    reply_to_tg_msg_id = Column(Integer, nullable=True)
    reply_to_msg_id = Column(UUID(as_uuid=True), ForeignKey("messages.id", ondelete="SET NULL"), nullable=True)
    reply_to_content_preview = Column(String(200), nullable=True)

    # Forward
    forwarded_from_alias = Column(String, nullable=True)

    # Group sender
    sender_tg_id = Column(BigInteger, nullable=True)
    sender_alias = Column(String, nullable=True)

    # Forum topic
    topic_id = Column(Integer, nullable=True)
    topic_name = Column(String, nullable=True)

    # Bot inline buttons (JSON string)
    inline_buttons = Column(Text, nullable=True)

    # Media group (album) ID — messages with same grouped_id are shown as album
    grouped_id = Column(BigInteger, nullable=True, index=True)

    created_at = Column(DateTime, default=func.now())

    contact = relationship("Contact", back_populates="messages")
    reply_to = relationship("Message", foreign_keys=[reply_to_msg_id], remote_side="Message.id")


class ScheduledMessage(Base):
    __tablename__ = "scheduled_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    content = Column(Text, nullable=True)
    media_path = Column(String, nullable=True)
    media_type = Column(String, nullable=True)
    scheduled_at = Column(DateTime, nullable=False)  # UTC
    timezone = Column(String, default="UTC")
    status = Column(String, default="pending")  # pending, sent, cancelled
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=func.now())
    sent_at = Column(DateTime, nullable=True)


class Tag(Base):
    __tablename__ = "tags"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    color = Column(String, default="#6366f1")
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id"), nullable=True)
    created_at = Column(DateTime, default=func.now())


class BotInvite(Base):
    __tablename__ = "bot_invites"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String, unique=True, nullable=False)
    role = Column(String, default="operator")
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"))
    used_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)


class StaffTgAccount(Base):
    """Many-to-many: which staff can see which TG accounts' chats."""
    __tablename__ = "staff_tg_accounts"
    __table_args__ = (UniqueConstraint("staff_id", "tg_account_id", name="uq_staff_tg_account"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id", ondelete="CASCADE"), nullable=False)
    assigned_at = Column(DateTime, default=func.now())


class PinnedChat(Base):
    """Org-wide pinned chats (shared across all staff in same workspace)."""
    __tablename__ = "pinned_chats"
    __table_args__ = (UniqueConstraint("staff_id", "contact_id", "org_id", name="uq_pinned_chat"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id", ondelete="CASCADE"), nullable=False)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    org_id = Column(String, nullable=True, index=True)
    pinned_at = Column(DateTime, default=func.now())


class AuditLog(Base):
    """Append-only audit trail for sensitive operations.

    SOC2 requirement: admins MUST NOT be able to delete audit logs.
    Application-level enforcement: no DELETE endpoint exists for this table.
    """
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=False)
    action = Column(String, nullable=False)
    # Generic target — use contact_id for backward compat, target_id for new ops
    target_contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=True)
    target_id = Column(String, nullable=True)     # Generic target (account UUID, staff UUID, etc)
    target_type = Column(String, nullable=True)    # "tg_account", "staff", "contact", etc
    metadata_json = Column(JSON, nullable=True)    # Old/new values, extra context
    ip_address = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())


class MessageTemplate(Base):
    """Quick reply templates / scripts."""
    __tablename__ = "message_templates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String, nullable=True)  # grouping label
    shortcut = Column(String, nullable=True)  # e.g. "/hello" trigger
    media_path = Column(String, nullable=True)
    media_type = Column(String, nullable=True)  # photo | video | video_note | voice | document
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id"), nullable=True, index=True)
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
    blocks_json = Column(JSON, nullable=True)  # [{type, content, media_path, media_type, delay_after}]
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Broadcast(Base):
    """Mass message campaign."""
    __tablename__ = "broadcasts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=True)
    media_path = Column(String, nullable=True)
    media_type = Column(String, nullable=True)  # photo | video | document

    # Targeting
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id"), nullable=False)
    tag_filter = Column(ARRAY(String), default=lambda: [])  # only contacts with these tags (empty = all)
    max_recipients = Column(Integer, nullable=True)  # Random N from filtered set
    contact_ids = Column(ARRAY(UUID(as_uuid=True)), default=lambda: [])  # Manual selection

    # Delay between sends (seconds)
    delay_seconds = Column(Integer, default=1)  # 1s to 3600s

    # Status
    status = Column(String, default="draft")  # draft | running | paused | completed | cancelled | failed
    total_recipients = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    # Last error message — set on catastrophic failure (whole task crashed)
    # OR updated to the most recent per-recipient failure so the user can see
    # what's happening without opening every recipient row.
    last_error = Column(Text, nullable=True)

    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=func.now())
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)


class BroadcastRecipient(Base):
    """Individual recipient status for a broadcast."""
    __tablename__ = "broadcast_recipients"
    __table_args__ = (UniqueConstraint("broadcast_id", "contact_id", name="uq_broadcast_recipient"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broadcast_id = Column(UUID(as_uuid=True), ForeignKey("broadcasts.id", ondelete="CASCADE"), nullable=False)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    status = Column(String, default="pending")  # pending | sent | failed
    sent_at = Column(DateTime, nullable=True)
    error = Column(Text, nullable=True)


class MessageEditHistory(Base):
    """Tracks edit history for messages."""
    __tablename__ = "message_edit_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    message_id = Column(UUID(as_uuid=True), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False)
    old_content = Column(Text, nullable=True)
    new_content = Column(Text, nullable=True)
    edited_at = Column(DateTime, default=func.now())

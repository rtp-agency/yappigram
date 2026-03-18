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
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy import UniqueConstraint

from config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)
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

    # Admin setting: show real contact names to operators (default: aliases only)
    show_real_names = Column(Boolean, default=False)

    assigned_contacts = relationship("Contact", back_populates="assigned_operator")


class TgAccount(Base):
    __tablename__ = "tg_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    phone = Column(String, unique=True, nullable=False)
    session_file = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    connected_at = Column(DateTime, default=func.now())
    org_id = Column(String, nullable=True, index=True)

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
    status = Column(String, default="pending")  # pending | approved | blocked

    # CRM data
    assigned_to = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    tags = Column(ARRAY(String), default=list)
    notes = Column(Text)
    is_archived = Column(Boolean, default=False)

    created_at = Column(DateTime, default=func.now())
    approved_at = Column(DateTime, nullable=True)
    last_message_at = Column(DateTime, nullable=True)

    tg_account = relationship("TgAccount", back_populates="contacts")
    assigned_operator = relationship("Staff", back_populates="assigned_contacts")
    messages = relationship("Message", back_populates="contact", order_by="Message.created_at")


class Message(Base):
    __tablename__ = "messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=False)
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
    reply_to_msg_id = Column(UUID(as_uuid=True), ForeignKey("messages.id"), nullable=True)
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

    created_at = Column(DateTime, default=func.now())

    contact = relationship("Contact", back_populates="messages")
    reply_to = relationship("Message", foreign_keys=[reply_to_msg_id], remote_side="Message.id")


class Tag(Base):
    __tablename__ = "tags"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    color = Column(String, default="#6366f1")
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
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

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=False)
    tg_account_id = Column(UUID(as_uuid=True), ForeignKey("tg_accounts.id"), nullable=False)
    assigned_at = Column(DateTime, default=func.now())


class PinnedChat(Base):
    """Per-user pinned chats."""
    __tablename__ = "pinned_chats"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=False)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=False)
    pinned_at = Column(DateTime, default=func.now())


class AuditLog(Base):
    """Logs sensitive actions like revealing real client data."""
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    staff_id = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=False)
    action = Column(String, nullable=False)  # reveal_data | block_contact | ...
    target_contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=True)
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
    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
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
    tag_filter = Column(ARRAY(String), default=list)  # only contacts with these tags (empty = all)
    max_recipients = Column(Integer, nullable=True)  # Random N from filtered set
    contact_ids = Column(ARRAY(UUID(as_uuid=True)), default=list)  # Manual selection

    # Delay between sends (seconds)
    delay_seconds = Column(Integer, default=1)  # 1s to 3600s

    # Status
    status = Column(String, default="draft")  # draft | running | paused | completed | cancelled
    total_recipients = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)

    created_by = Column(UUID(as_uuid=True), ForeignKey("staff.id"), nullable=True)
    org_id = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=func.now())
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)


class BroadcastRecipient(Base):
    """Individual recipient status for a broadcast."""
    __tablename__ = "broadcast_recipients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broadcast_id = Column(UUID(as_uuid=True), ForeignKey("broadcasts.id", ondelete="CASCADE"), nullable=False)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=False)
    status = Column(String, default="pending")  # pending | sent | failed
    sent_at = Column(DateTime, nullable=True)
    error = Column(Text, nullable=True)

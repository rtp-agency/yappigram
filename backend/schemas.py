from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


# --- Auth ---

class TgAuthRequest(BaseModel):
    init_data: str

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    role: str

class RefreshRequest(BaseModel):
    refresh_token: str


# --- Staff ---

class StaffOut(BaseModel):
    id: UUID
    tg_user_id: int
    tg_username: str | None = None
    role: str
    name: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}

class StaffUpdate(BaseModel):
    role: str | None = None
    is_active: bool | None = None


# --- Invites ---

class BotInviteCreate(BaseModel):
    role: str = "operator"

class BotInviteOut(BaseModel):
    code: str
    role: str
    bot_link: str
    expires_at: datetime


# --- Contacts ---

class ContactOut(BaseModel):
    id: UUID
    alias: str
    status: str
    chat_type: str = "private"
    is_forum: bool = False
    tags: list[str]
    notes: str | None
    assigned_to: UUID | None
    tg_account_id: UUID | None
    created_at: datetime
    approved_at: datetime | None
    last_message_at: datetime | None

    model_config = {"from_attributes": True}

class ContactReveal(BaseModel):
    real_name: str | None
    real_username: str | None
    real_tg_id: int

class ContactUpdate(BaseModel):
    alias: str | None = None
    tags: list[str] | None = None
    notes: str | None = None
    assigned_to: UUID | None = None

class CreateGroupRequest(BaseModel):
    title: str
    tg_account_id: UUID
    member_contact_ids: list[UUID] = []


# --- Messages ---

class MessageOut(BaseModel):
    id: UUID
    contact_id: UUID
    direction: str
    content: str | None
    media_type: str | None
    media_path: str | None
    sent_by: UUID | None
    is_read: bool
    is_deleted: bool = False
    is_edited: bool = False
    reply_to_msg_id: UUID | None = None
    reply_to_content_preview: str | None = None
    forwarded_from_alias: str | None = None
    sender_alias: str | None = None
    inline_buttons: str | None = None
    topic_id: int | None = None
    topic_name: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}

class SendMessage(BaseModel):
    content: str | None = None
    reply_to_msg_id: UUID | None = None

class ForwardMessage(BaseModel):
    message_ids: list[UUID]
    to_contact_id: UUID

class PressButton(BaseModel):
    message_id: UUID
    callback_data: str


# --- Telegram Account ---

class TgConnectRequest(BaseModel):
    phone: str

class TgVerifyRequest(BaseModel):
    phone: str
    code: str
    password_2fa: str | None = None

class TgAccountOut(BaseModel):
    id: UUID
    phone: str
    is_active: bool
    connected_at: datetime

    model_config = {"from_attributes": True}


# --- Tags ---

class TagCreate(BaseModel):
    name: str
    color: str = "#6366f1"

class TagOut(BaseModel):
    id: UUID
    name: str
    color: str

    model_config = {"from_attributes": True}

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# --- Auth ---

class TgAuthRequest(BaseModel):
    init_data: str
    force_select: bool = False

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    role: str

class RefreshRequest(BaseModel):
    refresh_token: str

class SsoAuthRequest(BaseModel):
    postforge_token: str

class TgWorkspaceItem(BaseModel):
    org_id: str
    name: str
    role: str

class TgAuthResponse(BaseModel):
    """Either tokens (single workspace) or workspace list (multi-workspace)."""
    access_token: str | None = None
    refresh_token: str | None = None
    role: str | None = None
    workspaces: list[TgWorkspaceItem] | None = None

class TgWorkspaceSelect(BaseModel):
    init_data: str
    org_id: str


# --- Staff ---

class StaffOut(BaseModel):
    id: UUID
    tg_user_id: int
    tg_username: str | None = None
    role: str
    name: str
    is_active: bool
    signature_mode: str = "named"
    timezone: str | None = "UTC"
    postforge_org_id: str | None = None
    # Surfaced so the frontend can detect cross-user token reuse: if the local
    # CRM token's postforge_user_id doesn't match the PostForge access_token's
    # user.id, the login page must clear and re-SSO.
    postforge_user_id: str | None = None
    # Global CRM admin flag (synced from PostForge beta_features.crm_admin).
    # Frontend uses it to show/hide the /admin menu item.
    is_crm_admin: bool = False
    created_at: datetime

    model_config = {"from_attributes": True}



# --- Invites ---

class BotInviteCreate(BaseModel):
    role: str = "operator"

    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        if v not in ("operator", "admin", "assistant"):
            raise ValueError("Role must be 'operator', 'admin', or 'assistant'")
        return v

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
    real_tg_id: int | None = None
    is_archived: bool = False
    created_at: datetime
    approved_at: datetime | None
    last_message_at: datetime | None
    last_message_content: str | None = None
    last_message_direction: str | None = None
    last_message_is_read: bool | None = None

    model_config = {"from_attributes": True}

class ContactReveal(BaseModel):
    real_name: str | None
    real_username: str | None
    real_tg_id: int

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
    grouped_id: int | None = None
    created_at: datetime

    model_config = {"from_attributes": True}

class SendMessage(BaseModel):
    content: str | None = None
    reply_to_msg_id: UUID | None = None

class ForwardMessage(BaseModel):
    message_ids: list[UUID]
    to_contact_id: UUID
    media_only: bool = False

class PressButton(BaseModel):
    message_id: UUID
    callback_data: str


# --- Telegram Account ---

class TgConnectRequest(BaseModel):
    phone: str = Field(..., pattern=r'^\+\d{7,15}$')

class TgVerifyRequest(BaseModel):
    phone: str
    code: str
    password_2fa: str | None = None

class TgAccountOut(BaseModel):
    id: UUID
    phone: str
    display_name: str | None = None
    is_active: bool
    connected_at: datetime
    show_real_names: bool = False
    connected: bool = False

    model_config = {"from_attributes": True}


# --- Tags ---

class TagCreate(BaseModel):
    name: str = Field(..., max_length=50)
    color: str = Field("#6366f1", max_length=20)
    tg_account_id: UUID | None = None

class TagOut(BaseModel):
    id: UUID
    name: str
    color: str
    tg_account_id: UUID | None = None

    model_config = {"from_attributes": True}


class MessageEditHistoryOut(BaseModel):
    id: UUID
    message_id: UUID
    old_content: str | None = None
    new_content: str | None = None
    edited_at: datetime

    model_config = {"from_attributes": True}


# --- Contact extended ---

class ContactUpdate(BaseModel):
    alias: str | None = Field(None, max_length=200)
    tags: list[str] | None = None
    notes: str | None = Field(None, max_length=5000)
    assigned_to: UUID | None = None
    is_archived: bool | None = None


# --- Templates ---

class TemplateMediaFileIn(BaseModel):
    path: str
    type: str = "photo"

class TemplateBlockIn(BaseModel):
    id: str  # client-generated UUID for the block
    type: str = "text"  # text | photo | video | video_note | voice | document | media_group
    content: str | None = None  # text or caption
    media_path: str | None = None
    media_type: str | None = None
    media_files: list[TemplateMediaFileIn] | None = None  # for media_group blocks
    delay_after: float = 0  # seconds to wait after this block

class TemplateCreate(BaseModel):
    title: str = Field(..., max_length=200)
    content: str = Field("", max_length=10000)  # legacy fallback
    category: str | None = Field(None, max_length=100)
    shortcut: str | None = Field(None, max_length=50)
    tg_account_id: UUID | None = None
    blocks_json: list[TemplateBlockIn] | None = None

class TemplateUpdate(BaseModel):
    title: str | None = Field(None, max_length=200)
    content: str | None = Field(None, max_length=10000)
    category: str | None = Field(None, max_length=100)
    shortcut: str | None = Field(None, max_length=50)
    blocks_json: list[TemplateBlockIn] | None = None

class TemplateOut(BaseModel):
    id: UUID
    title: str
    content: str
    category: str | None = None
    shortcut: str | None = None
    media_path: str | None = None
    media_type: str | None = None
    blocks_json: list | None = None
    tg_account_id: UUID | None = None
    created_by: UUID | None = None
    created_by_name: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


# --- Broadcasts ---

class BroadcastCreate(BaseModel):
    title: str = Field(..., max_length=200)
    content: str | None = Field(None, max_length=10000)
    tg_account_id: UUID
    tag_filter: list[str] = []
    delay_seconds: int = 1
    max_recipients: int | None = None  # Random N from filtered set
    contact_ids: list[UUID] = []  # Manual selection (overrides filters)

class BroadcastOut(BaseModel):
    id: UUID
    title: str
    content: str | None = None
    media_path: str | None = None
    media_type: str | None = None
    tg_account_id: UUID
    tag_filter: list[str] = []
    max_recipients: int | None = None
    delay_seconds: int
    status: str
    total_recipients: int
    sent_count: int
    failed_count: int
    created_by: UUID | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    model_config = {"from_attributes": True}


# --- Staff extended ---

class StaffUpdate(BaseModel):
    role: str | None = None
    is_active: bool | None = None
    signature_mode: str | None = None

    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        if v is not None and v not in ("operator", "admin", "super_admin"):
            raise ValueError("Invalid role")
        return v


# --- Translation ---

class TranslateRequest(BaseModel):
    text: str
    target_lang: str = "en"

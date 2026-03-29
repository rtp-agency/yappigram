"""YappiGram — Main FastAPI application with all routes."""

import asyncio
import os
import secrets
import uuid as uuid_mod
from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    create_token,
    decode_token,
    get_current_user,
    get_db,
    require_role,
    validate_tg_init_data,
)
from bot import get_bot_username, start_bot_polling, stop_bot
from config import settings
from crypto import decrypt
from models import AuditLog, Base, BotInvite, Contact, Message, PinnedChat, Staff, StaffTgAccount, Tag, TgAccount, engine
from schemas import (
    BotInviteCreate,
    BotInviteOut,
    ContactOut,
    ContactReveal,
    ContactUpdate,
    CreateGroupRequest,
    DeleteMessageRequest,
    EditMessageRequest,
    ForwardMessage,
    MessageOut,
    PressButton,
    RefreshRequest,
    SendMessage,
    TgAuthRequest,
    StaffOut,
    StaffUpdate,
    TagCreate,
    TagOut,
    TgAccountOut,
    TgConnectRequest,
    TgVerifyRequest,
    TokenResponse,
)
from telegram import (
    create_group,
    delete_message,
    disconnect_account,
    edit_message,
    fetch_history,
    forward_message,
    press_inline_button,
    send_message,
    shutdown_listeners,
    start_connect,
    startup_listeners,
    verify_code,
)
from ws import ws_manager

MEDIA_DIR = "media"
os.makedirs(MEDIA_DIR, exist_ok=True)

app = FastAPI(title="YappiGram", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve uploaded/downloaded media files
app.mount("/media", StaticFiles(directory=MEDIA_DIR), name="media")

# Type aliases for common dependencies
DB = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[Staff, Depends(get_current_user)]
AdminUser = Annotated[Staff, Depends(require_role("super_admin", "admin"))]
SuperAdmin = Annotated[Staff, Depends(require_role("super_admin"))]


# ============================================================
# Startup / Shutdown
# ============================================================

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await startup_listeners()
    asyncio.create_task(start_bot_polling())


@app.on_event("shutdown")
async def on_shutdown():
    await shutdown_listeners()
    await stop_bot()


# ============================================================
# Auth (Telegram-only)
# ============================================================

@app.post("/api/auth/tg", response_model=TokenResponse)
async def tg_auth(req: TgAuthRequest, db: DB):
    """Authenticate via Telegram Mini App initData.

    Auto-creates super_admin for TG_ADMIN_CHAT_ID on first access.
    """
    tg_user = validate_tg_init_data(req.init_data)
    tg_id = tg_user.get("id")
    if not tg_id:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "No user ID in initData")

    result = await db.execute(
        select(Staff).where(Staff.tg_user_id == tg_id, Staff.is_active.is_(True))
    )
    user = result.scalar_one_or_none()

    # Auto-create super_admin for the configured admin chat ID
    if not user and tg_id == settings.TG_ADMIN_CHAT_ID:
        user = Staff(
            tg_user_id=tg_id,
            tg_username=tg_user.get("username"),
            role="super_admin",
            name=tg_user.get("first_name", "Admin"),
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    if not user:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "No access. Use an invite link from the bot.")

    return TokenResponse(
        access_token=create_token(user.id, "access"),
        refresh_token=create_token(user.id, "refresh"),
        role=user.role,
    )


@app.post("/api/auth/refresh", response_model=TokenResponse)
async def refresh(req: RefreshRequest, db: DB):
    payload = decode_token(req.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token type")

    result = await db.execute(select(Staff).where(Staff.id == payload["sub"], Staff.is_active.is_(True)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found")

    return TokenResponse(
        access_token=create_token(user.id, "access"),
        refresh_token=create_token(user.id, "refresh"),
        role=user.role,
    )


# ============================================================
# Telegram Accounts
# ============================================================

@app.post("/api/tg/connect")
async def tg_connect(req: TgConnectRequest, user: SuperAdmin):
    await start_connect(req.phone)
    return {"status": "code_sent"}


@app.post("/api/tg/verify", response_model=TgAccountOut)
async def tg_verify(req: TgVerifyRequest, user: SuperAdmin):
    account = await verify_code(req.phone, req.code, req.password_2fa)
    return account


@app.get("/api/tg/status", response_model=list[TgAccountOut])
async def tg_status(user: AdminUser, db: DB):
    result = await db.execute(select(TgAccount))
    return result.scalars().all()


@app.delete("/api/tg/disconnect/{account_id}")
async def tg_disconnect(account_id: UUID, user: SuperAdmin, db: DB):
    result = await db.execute(select(TgAccount).where(TgAccount.id == account_id))
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    account.is_active = False
    await db.commit()
    await disconnect_account(account_id)
    return {"status": "disconnected"}


# ============================================================
# Contacts
# ============================================================

@app.get("/api/contacts", response_model=list[ContactOut])
async def list_contacts(
    user: CurrentUser,
    db: DB,
    status_filter: str | None = Query(None, alias="status"),
    assigned_to: UUID | None = None,
    tag: str | None = None,
    archived: bool = Query(False),
):
    query = select(Contact)

    # Filter by archive status
    query = query.where(Contact.is_archived.is_(archived))

    # Operators see contacts from their assigned TG accounts
    if user.role == "operator":
        sub = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(Contact.tg_account_id.in_(sub), Contact.status == "approved")
    elif status_filter:
        query = query.where(Contact.status == status_filter)

    if assigned_to:
        query = query.where(Contact.assigned_to == assigned_to)
    if tag:
        query = query.where(Contact.tags.any(tag))

    query = query.order_by(Contact.last_message_at.desc().nullslast())
    result = await db.execute(query)
    return result.scalars().all()


@app.get("/api/contacts/{contact_id}", response_model=ContactOut)
async def get_contact(contact_id: UUID, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)
    return contact


@app.patch("/api/contacts/{contact_id}", response_model=ContactOut)
async def update_contact(contact_id: UUID, req: ContactUpdate, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)

    if req.alias is not None:
        contact.alias = req.alias
    if req.tags is not None:
        contact.tags = req.tags
    if req.notes is not None:
        contact.notes = req.notes
    if req.assigned_to is not None and user.role in ("super_admin", "admin"):
        contact.assigned_to = req.assigned_to

    await db.commit()
    await db.refresh(contact)
    return contact


@app.post("/api/contacts/{contact_id}/approve", response_model=ContactOut)
async def approve_contact(contact_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    contact.status = "approved"
    contact.approved_at = func.now()
    await db.commit()
    await db.refresh(contact)

    # Fetch last 100 messages from Telegram and save to DB
    try:
        from telegram import sanitize_text, _extract_media, _extract_inline_buttons
        tg_messages = await fetch_history(contact.tg_account_id, contact.real_tg_id, limit=100)
        sorted_msgs = sorted(
            [m for m in tg_messages if m and (m.text or m.media)],
            key=lambda m: m.date
        )
        for msg_obj in sorted_msgs:
            # Skip duplicates
            existing = await db.execute(
                select(Message).where(
                    Message.tg_message_id == msg_obj.id,
                    Message.contact_id == contact.id,
                )
            )
            if existing.scalar_one_or_none():
                continue

            direction = "outgoing" if msg_obj.out else "incoming"

            # Media
            media_type, ext = _extract_media(msg_obj)
            media_path = None
            if media_type and ext is not None:
                filename = f"{contact.id}_{msg_obj.id}{ext}"
                filepath = os.path.join(MEDIA_DIR, filename)
                try:
                    actual_path = await msg_obj.download_media(file=filepath)
                    media_path = os.path.basename(actual_path) if actual_path else filename
                except Exception:
                    media_path = filename

            # Reply-to
            reply_to_tg_msg_id = None
            reply_to_msg_id = None
            reply_to_content_preview = None
            if msg_obj.reply_to and hasattr(msg_obj.reply_to, "reply_to_msg_id") and msg_obj.reply_to.reply_to_msg_id:
                reply_to_tg_msg_id = msg_obj.reply_to.reply_to_msg_id
                rr = await db.execute(
                    select(Message).where(
                        Message.tg_message_id == reply_to_tg_msg_id,
                        Message.contact_id == contact.id,
                    )
                )
                ref_msg = rr.scalar_one_or_none()
                if ref_msg:
                    reply_to_msg_id = ref_msg.id
                    preview = ref_msg.content or (f"[{ref_msg.media_type}]" if ref_msg.media_type else "...")
                    reply_to_content_preview = sanitize_text(preview[:200])

            # Forwarded from
            forwarded_from_alias = None
            if msg_obj.fwd_from:
                forwarded_from_alias = "[hidden]"

            # Inline buttons
            inline_buttons_json = _extract_inline_buttons(msg_obj)

            msg = Message(
                contact_id=contact.id,
                tg_message_id=msg_obj.id,
                direction=direction,
                content=sanitize_text(msg_obj.text),
                media_type=media_type,
                media_path=media_path,
                reply_to_tg_msg_id=reply_to_tg_msg_id,
                reply_to_msg_id=reply_to_msg_id,
                reply_to_content_preview=reply_to_content_preview,
                forwarded_from_alias=forwarded_from_alias,
                inline_buttons=inline_buttons_json,
                created_at=msg_obj.date.replace(tzinfo=None) if msg_obj.date else None,
            )
            db.add(msg)

        await db.commit()
    except Exception as e:
        print(f"[APPROVE] Failed to fetch history for {contact.id}: {e}")

    await ws_manager.broadcast_to_admins({
        "type": "contact_approved",
        "contact_id": str(contact.id),
    })
    return contact


@app.post("/api/contacts/{contact_id}/block", response_model=ContactOut)
async def block_contact(contact_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    contact.status = "blocked"
    await db.commit()
    await db.refresh(contact)

    await ws_manager.broadcast_to_admins({
        "type": "contact_blocked",
        "contact_id": str(contact.id),
    })
    return contact


@app.post("/api/contacts/{contact_id}/archive", response_model=ContactOut)
async def archive_contact(contact_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    contact.is_archived = True
    await db.commit()
    await db.refresh(contact)
    return contact


@app.post("/api/contacts/{contact_id}/unarchive", response_model=ContactOut)
async def unarchive_contact(contact_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    contact.is_archived = False
    await db.commit()
    await db.refresh(contact)
    return contact


@app.delete("/api/contacts/{contact_id}", status_code=204)
async def delete_contact(contact_id: UUID, user: CurrentUser, db: DB):
    """Delete a contact and its messages from CRM (does not affect Telegram)."""
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Delete related records first (FK constraints)
    from sqlalchemy import delete as sa_delete
    await db.execute(sa_delete(AuditLog).where(AuditLog.target_contact_id == contact_id))
    await db.execute(sa_delete(PinnedChat).where(PinnedChat.contact_id == contact_id))
    await db.execute(sa_delete(Message).where(Message.contact_id == contact_id))
    await db.delete(contact)
    await db.commit()

    await ws_manager.broadcast_to_admins({
        "type": "contact_deleted",
        "contact_id": str(contact_id),
    })
    return


# ---- Pinned chats (per-user) ----

@app.get("/api/pinned")
async def get_pinned(user: Annotated[Staff, Depends(get_current_user)], db: DB):
    result = await db.execute(
        select(PinnedChat.contact_id).where(PinnedChat.staff_id == user.id)
    )
    return [str(row[0]) for row in result.all()]


@app.post("/api/pinned/{contact_id}", status_code=204)
async def pin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    existing = await db.execute(
        select(PinnedChat).where(PinnedChat.staff_id == user.id, PinnedChat.contact_id == contact_id)
    )
    if existing.scalar_one_or_none():
        return
    db.add(PinnedChat(staff_id=user.id, contact_id=contact_id))
    await db.commit()


@app.delete("/api/pinned/{contact_id}", status_code=204)
async def unpin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    from sqlalchemy import delete as sa_delete
    await db.execute(
        sa_delete(PinnedChat).where(PinnedChat.staff_id == user.id, PinnedChat.contact_id == contact_id)
    )
    await db.commit()


@app.get("/api/contacts/{contact_id}/reveal", response_model=ContactReveal)
async def reveal_contact(contact_id: UUID, user: AdminUser, db: DB):
    """Reveal real client data. Logged in audit."""
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Audit log
    db.add(AuditLog(staff_id=user.id, action="reveal_data", target_contact_id=contact.id))
    await db.commit()

    return ContactReveal(
        real_name=decrypt(contact.real_name_encrypted),
        real_username=decrypt(contact.real_username_encrypted),
        real_tg_id=contact.real_tg_id,
    )


@app.post("/api/contacts/create-group", response_model=ContactOut)
async def create_group_endpoint(req: CreateGroupRequest, user: AdminUser, db: DB):
    """Create a new Telegram group via Telethon with selected CRM contacts."""
    from crypto import encrypt
    from telegram import generate_alias

    # Resolve CRM contact IDs to real TG IDs
    member_tg_ids = []
    for cid in req.member_contact_ids:
        r = await db.execute(select(Contact).where(Contact.id == cid))
        c = r.scalar_one_or_none()
        if c and c.real_tg_id:
            member_tg_ids.append(c.real_tg_id)

    chat_id = await create_group(req.tg_account_id, req.title, member_tg_ids)

    seq = (await db.execute(select(func.count(Contact.id)))).scalar() + 1
    alias = generate_alias(req.title, seq)

    contact = Contact(
        tg_account_id=req.tg_account_id,
        real_tg_id=chat_id,
        group_title_encrypted=encrypt(req.title),
        chat_type="group",
        alias=alias,
        status="approved",
    )
    db.add(contact)
    await db.commit()
    await db.refresh(contact)
    return contact


@app.post("/api/contacts/{contact_id}/add-member")
async def add_member_endpoint(contact_id: UUID, req: dict, user: AdminUser, db: DB):
    """Add a CRM contact to a Telegram group."""
    from telegram import add_group_member

    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if group.chat_type not in ("group", "channel"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Not a group chat")

    member_contact_id = req.get("member_contact_id")
    if not member_contact_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "member_contact_id required")

    mr = await db.execute(select(Contact).where(Contact.id == member_contact_id))
    member = mr.scalar_one_or_none()
    if not member:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Member contact not found")

    try:
        await add_group_member(group.tg_account_id, group.real_tg_id, str(member.real_tg_id))
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))

    return {"ok": True}


async def _get_contact_with_access(contact_id: UUID, user: Staff, db: AsyncSession) -> Contact:
    result = await db.execute(select(Contact).where(Contact.id == contact_id))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if user.role == "operator":
        # Check if operator has access via assigned TG account
        sub = await db.execute(
            select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        )
        allowed_accounts = {row[0] for row in sub.all()}
        if contact.tg_account_id not in allowed_accounts:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "No access to this contact")
    return contact


# ============================================================
# Messages
# ============================================================

@app.get("/api/messages/{contact_id}", response_model=list[MessageOut])
async def get_messages(
    contact_id: UUID,
    user: CurrentUser,
    db: DB,
    limit: int = Query(100, le=500),
    offset: int = Query(0),
):
    await _get_contact_with_access(contact_id, user, db)

    query = (
        select(Message)
        .where(Message.contact_id == contact_id)
        .order_by(Message.created_at.desc(), Message.tg_message_id.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(query)
    return list(reversed(result.scalars().all()))


@app.post("/api/messages/{contact_id}/send", response_model=MessageOut)
async def send_msg(contact_id: UUID, req: SendMessage, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)

    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

    # Resolve reply-to
    reply_to_tg_msg_id = None
    reply_to_msg_id = None
    reply_to_content_preview = None
    if req.reply_to_msg_id:
        rr = await db.execute(select(Message).where(Message.id == req.reply_to_msg_id))
        ref_msg = rr.scalar_one_or_none()
        if ref_msg:
            reply_to_tg_msg_id = ref_msg.tg_message_id
            reply_to_msg_id = ref_msg.id
            preview = ref_msg.content or (f"[{ref_msg.media_type}]" if ref_msg.media_type else "...")
            reply_to_content_preview = preview[:200]

    # Translate outgoing message if requested
    send_content = req.content
    if req.translate_to and req.content:
        try:
            from deep_translator import GoogleTranslator
            send_content = GoogleTranslator(source="auto", target=req.translate_to).translate(req.content)
        except Exception as e:
            print(f"[TRANSLATE] Failed to translate outgoing: {e}")

    # Send via Telethon (translated text goes to Telegram)
    tg_msg_id = await send_message(
        contact.tg_account_id, contact.real_tg_id, send_content,
        reply_to_tg_msg_id=reply_to_tg_msg_id,
    )

    # Store original text in DB
    msg = Message(
        contact_id=contact.id,
        tg_message_id=tg_msg_id,
        direction="outgoing",
        content=req.content,
        sent_by=user.id,
        reply_to_tg_msg_id=reply_to_tg_msg_id,
        reply_to_msg_id=reply_to_msg_id,
        reply_to_content_preview=reply_to_content_preview,
    )
    db.add(msg)
    contact.last_message_at = func.now()
    await db.commit()
    await db.refresh(msg)

    return msg


@app.post("/api/messages/{contact_id}/send-media", response_model=MessageOut)
async def send_media(
    contact_id: UUID,
    user: CurrentUser,
    db: DB,
    file: UploadFile = File(...),
    caption: str | None = Query(None),
):
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

    # Determine media type from content type
    ct = file.content_type or ""
    if ct.startswith("image/"):
        media_type = "photo"
    elif ct.startswith("video/"):
        media_type = "video"
    elif ct.startswith("audio/"):
        media_type = "voice"
    else:
        media_type = "document"

    # Save file locally
    ext = os.path.splitext(file.filename or "")[1] or ""
    filename = f"{uuid_mod.uuid4()}{ext}"
    filepath = os.path.join(MEDIA_DIR, filename)
    data = await file.read()
    with open(filepath, "wb") as f:
        f.write(data)

    # Send via Telethon
    tg_msg_id = await send_message(
        contact.tg_account_id, contact.real_tg_id,
        text=caption, file_path=filepath,
    )

    msg = Message(
        contact_id=contact.id,
        tg_message_id=tg_msg_id,
        direction="outgoing",
        content=caption,
        media_type=media_type,
        media_path=filename,
        sent_by=user.id,
    )
    db.add(msg)
    contact.last_message_at = func.now()
    await db.commit()
    await db.refresh(msg)
    return msg


@app.patch("/api/messages/{contact_id}/read")
async def mark_read(contact_id: UUID, user: CurrentUser, db: DB):
    """Mark all unread incoming messages in a contact chat as read."""
    from sqlalchemy import update
    await db.execute(
        update(Message)
        .where(Message.contact_id == contact_id, Message.direction == "incoming", Message.is_read.is_(False))
        .values(is_read=True)
    )
    await db.commit()
    return {"status": "ok"}


@app.get("/api/unread")
async def get_unread_counts(user: CurrentUser, db: DB):
    """Get unread message counts per contact for approved contacts."""
    from sqlalchemy import case
    result = await db.execute(
        select(
            Message.contact_id,
            func.count(Message.id).label("count"),
        )
        .join(Contact, Contact.id == Message.contact_id)
        .where(
            Message.direction == "incoming",
            Message.is_read.is_(False),
            Contact.status == "approved",
        )
        .group_by(Message.contact_id)
    )
    return {str(row.contact_id): row.count for row in result.all()}


@app.post("/api/messages/{contact_id}/forward")
async def forward_msg(contact_id: UUID, req: ForwardMessage, user: CurrentUser, db: DB):
    """Forward messages from one contact chat to another."""
    source = await _get_contact_with_access(contact_id, user, db)
    target = await _get_contact_with_access(req.to_contact_id, user, db)

    if target.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Target contact not approved")

    # Get source messages with content/media
    src_messages = []
    tg_msg_ids = []
    for msg_id in req.message_ids:
        rr = await db.execute(select(Message).where(Message.id == msg_id, Message.contact_id == contact_id))
        msg = rr.scalar_one_or_none()
        if msg and msg.tg_message_id:
            tg_msg_ids.append(msg.tg_message_id)
            src_messages.append(msg)

    if not tg_msg_ids:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No valid messages to forward")

    fwd_ids = await forward_message(
        source.tg_account_id, source.real_tg_id, tg_msg_ids, target.real_tg_id,
    )

    # Save forwarded messages in CRM — carry over content + media from source
    saved = []
    for i, fwd_tg_id in enumerate(fwd_ids):
        src = src_messages[i] if i < len(src_messages) else None
        fwd_msg = Message(
            contact_id=target.id,
            tg_message_id=fwd_tg_id,
            direction="outgoing",
            content=src.content if src else None,
            media_type=src.media_type if src else None,
            media_path=src.media_path if src else None,
            sent_by=user.id,
            forwarded_from_alias=source.alias,
        )
        db.add(fwd_msg)
        saved.append(fwd_msg)

    target.last_message_at = func.now()
    await db.commit()
    return {"status": "ok", "forwarded_count": len(fwd_ids)}


@app.post("/api/messages/{contact_id}/press-button")
async def press_btn(contact_id: UUID, req: PressButton, user: CurrentUser, db: DB):
    """Press an inline bot button."""
    contact = await _get_contact_with_access(contact_id, user, db)

    rr = await db.execute(select(Message).where(Message.id == req.message_id, Message.contact_id == contact_id))
    msg = rr.scalar_one_or_none()
    if not msg or not msg.tg_message_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Message not found")

    import base64
    try:
        cb_data = base64.b64decode(req.callback_data)
    except Exception:
        cb_data = req.callback_data.encode("utf-8")
    response_text = await press_inline_button(
        contact.tg_account_id, contact.real_tg_id,
        msg.tg_message_id, cb_data,
    )
    return {"status": "ok", "response": response_text}


@app.post("/api/messages/{contact_id}/edit")
async def edit_msg(contact_id: UUID, req: EditMessageRequest, user: CurrentUser, db: DB):
    """Edit a message in Telegram and update in CRM."""
    contact = await _get_contact_with_access(contact_id, user, db)
    rr = await db.execute(select(Message).where(Message.id == req.message_id, Message.contact_id == contact_id))
    msg = rr.scalar_one_or_none()
    if not msg or not msg.tg_message_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Message not found")
    if msg.direction != "outgoing":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Can only edit outgoing messages")

    await edit_message(contact.tg_account_id, contact.real_tg_id, msg.tg_message_id, req.content)
    msg.content = req.content
    msg.is_edited = True
    await db.commit()
    return {"status": "ok"}


@app.post("/api/messages/{contact_id}/delete")
async def delete_msg(contact_id: UUID, req: DeleteMessageRequest, user: CurrentUser, db: DB):
    """Delete a message in Telegram and mark as deleted in CRM."""
    contact = await _get_contact_with_access(contact_id, user, db)
    rr = await db.execute(select(Message).where(Message.id == req.message_id, Message.contact_id == contact_id))
    msg = rr.scalar_one_or_none()
    if not msg or not msg.tg_message_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Message not found")

    await delete_message(contact.tg_account_id, contact.real_tg_id, [msg.tg_message_id])
    msg.is_deleted = True
    await db.commit()
    return {"status": "ok"}


# ============================================================
# Translation
# ============================================================

@app.post("/api/translate")
async def translate_text(req: dict, user: CurrentUser):
    """Translate text using Google Translate."""
    text = req.get("text", "")
    target_lang = req.get("target_lang", "en")
    if not text:
        return {"translated": ""}
    try:
        from deep_translator import GoogleTranslator
        translated = GoogleTranslator(source="auto", target=target_lang).translate(text)
        return {"translated": translated}
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Translation failed: {e}")


# ============================================================
# Staff
# ============================================================

@app.get("/api/staff/me", response_model=StaffOut)
async def get_me(user: CurrentUser):
    """Return current authenticated staff member."""
    return user


@app.get("/api/staff", response_model=list[StaffOut])
async def list_staff(user: AdminUser, db: DB):
    result = await db.execute(select(Staff).order_by(Staff.created_at))
    return result.scalars().all()


@app.post("/api/staff/invite", response_model=BotInviteOut)
async def create_invite(req: BotInviteCreate, user: AdminUser, db: DB):
    code = secrets.token_urlsafe(6)[:8]
    invite = BotInvite(
        code=code,
        role=req.role,
        created_by=user.id,
        expires_at=datetime.utcnow() + timedelta(hours=48),
    )
    db.add(invite)
    await db.commit()
    await db.refresh(invite)
    bot_name = get_bot_username()
    bot_link = f"https://t.me/{bot_name}?start={code}" if bot_name else f"Bot link: /start {code}"
    return BotInviteOut(code=code, role=invite.role, bot_link=bot_link, expires_at=invite.expires_at)


@app.patch("/api/staff/{staff_id}", response_model=StaffOut)
async def update_staff(staff_id: UUID, req: StaffUpdate, user: AdminUser, db: DB):
    result = await db.execute(select(Staff).where(Staff.id == staff_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Prevent modifying super_admin unless you are super_admin
    if target.role == "super_admin" and user.role != "super_admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN)

    if req.role is not None:
        target.role = req.role
    if req.is_active is not None:
        target.is_active = req.is_active

    await db.commit()
    await db.refresh(target)
    return target


@app.get("/api/staff/{staff_id}/accounts")
async def get_staff_accounts(staff_id: UUID, user: AdminUser, db: DB):
    """Get TG accounts assigned to a staff member."""
    result = await db.execute(
        select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == staff_id)
    )
    return [str(row[0]) for row in result.all()]


@app.put("/api/staff/{staff_id}/accounts")
async def set_staff_accounts(staff_id: UUID, account_ids: list[UUID], user: AdminUser, db: DB):
    """Set TG accounts for a staff member (replace all)."""
    # Verify staff exists
    result = await db.execute(select(Staff).where(Staff.id == staff_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Delete existing assignments
    await db.execute(
        select(StaffTgAccount).where(StaffTgAccount.staff_id == staff_id)
    )
    from sqlalchemy import delete
    await db.execute(delete(StaffTgAccount).where(StaffTgAccount.staff_id == staff_id))

    # Create new assignments
    for acc_id in account_ids:
        db.add(StaffTgAccount(staff_id=staff_id, tg_account_id=acc_id))

    await db.commit()
    return {"status": "ok", "account_ids": [str(a) for a in account_ids]}


@app.delete("/api/staff/{staff_id}")
async def deactivate_staff(staff_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Staff).where(Staff.id == staff_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if target.role == "super_admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Cannot deactivate super admin")
    target.is_active = False
    await db.commit()
    return {"status": "deactivated"}


# ============================================================
# Tags
# ============================================================

@app.get("/api/tags", response_model=list[TagOut])
async def list_tags(user: CurrentUser, db: DB):
    result = await db.execute(select(Tag).order_by(Tag.name))
    return result.scalars().all()


@app.post("/api/tags", response_model=TagOut)
async def create_tag(req: TagCreate, user: AdminUser, db: DB):
    tag = Tag(name=req.name, color=req.color, created_by=user.id)
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return tag


# ============================================================
# WebSocket
# ============================================================

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, token: str = Query(...)):
    try:
        payload = decode_token(token)
    except HTTPException:
        # Token expired/invalid — must accept before closing
        await ws.accept()
        await ws.close(code=4001, reason="Token expired")
        return

    if payload.get("type") != "access":
        await ws.accept()
        await ws.close(code=4001, reason="Invalid token type")
        return

    staff_id = UUID(payload["sub"])

    # Verify user exists and is active
    from models import async_session as get_session
    async with get_session() as db:
        result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True)))
        user = result.scalar_one_or_none()
        if not user:
            await ws.accept()
            await ws.close(code=4003, reason="User not found")
            return

    await ws_manager.connect(staff_id, ws)
    try:
        while True:
            data = await ws.receive_text()
            # Handle typing indicators etc.
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        ws_manager.disconnect(staff_id, ws)


# ============================================================
# Health
# ============================================================

@app.get("/api/health")
async def health():
    return {"status": "ok"}

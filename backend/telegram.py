import asyncio
import json
import os
import re
from uuid import UUID

from sqlalchemy import func, select
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

from config import settings
from crypto import encrypt
from models import Contact, Message, MessageEditHistory, TgAccount, async_session
from ws import ws_manager

SESSIONS_DIR = "sessions"
MEDIA_DIR = "media"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

# Active Telethon clients: tg_account_id -> TelegramClient
_clients: dict[UUID, TelegramClient] = {}

# Pending auth flows: phone -> (TelegramClient, created_at)
_pending_auth: dict[str, tuple[TelegramClient, float]] = {}

def _cleanup_pending_auth():
    """Remove stale pending auth sessions older than 5 minutes."""
    import time
    now = time.time()
    stale = [phone for phone, (_, ts) in _pending_auth.items() if now - ts > 300]
    for phone in stale:
        client, _ = _pending_auth.pop(phone)
        asyncio.create_task(client.disconnect())

_USERNAME_RE = re.compile(r"@[A-Za-z0-9_]{4,}")

# Topic name cache: (account_id, peer_id, topic_id) -> topic_name
_topic_cache: dict[tuple, str] = {}


async def _resolve_topic_name(client: TelegramClient, peer_id: int, topic_id: int, account_id: UUID) -> str | None:
    """Resolve topic name for a forum supergroup, with caching."""
    if topic_id == 1:
        return "General"
    cache_key = (account_id, peer_id, topic_id)
    if cache_key in _topic_cache:
        return _topic_cache[cache_key]
    try:
        # The topic creation message (service message) has the topic title
        # Topic ID = message ID of the service message that created the topic
        from telethon.tl.functions.channels import GetForumTopicsByIDRequest
        entity = await client.get_input_entity(peer_id)
        result = await client(GetForumTopicsByIDRequest(channel=entity, topics=[topic_id]))
        if result.topics:
            name = result.topics[0].title
            _topic_cache[cache_key] = name
            return name
    except ImportError:
        pass
    except Exception:
        pass
    # Fallback: try to read the service message that created the topic
    try:
        msgs = await client.get_messages(peer_id, ids=topic_id)
        if msgs and hasattr(msgs, "action") and hasattr(msgs.action, "title"):
            name = msgs.action.title
            _topic_cache[cache_key] = name
            return name
    except Exception:
        pass
    return None


def _session_path(phone: str) -> str:
    clean = phone.replace("+", "").replace(" ", "")
    return os.path.join(SESSIONS_DIR, clean)


def generate_alias(real_name: str | None, sequence: int) -> str:
    if not real_name or not real_name.strip():
        return f"Us-{sequence:03d}"
    # Strip HTML-unsafe and problematic chars from prefix
    clean = re.sub(r'[<>&"\'\\/@#]', '', real_name.strip())
    prefix = clean[:2] if clean else "Us"
    return f"{prefix}-{sequence:03d}"


def sanitize_text(text: str | None) -> str | None:
    """Replace @username mentions with [hidden]."""
    if not text:
        return text
    return _USERNAME_RE.sub("[hidden]", text)


async def start_connect(phone: str) -> dict:
    """Step 1: send auth code to the phone."""
    import logging
    log = logging.getLogger("tg_connect")
    log.info(f"start_connect called for phone={phone}")
    client = TelegramClient(
        _session_path(phone),
        settings.TG_API_ID,
        settings.TG_API_HASH,
    )
    await client.connect()
    print(f"[TG_CONNECT] connected, is_authorized={await client.is_user_authorized()}", flush=True)
    try:
        result = await client.send_code_request(phone, force_sms=True)
        print(f"[TG_CONNECT] send_code OK: type={type(result.type).__name__}, hash={result.phone_code_hash[:8]}...", flush=True)
        # Disconnect previous pending client for same phone
        import time
        if phone in _pending_auth:
            old_client, _ = _pending_auth.pop(phone)
            await old_client.disconnect()
        _cleanup_pending_auth()
        _pending_auth[phone] = (client, time.time())
        return {"type": type(result.type).__name__, "hash": result.phone_code_hash[:8]}
    except Exception as e:
        print(f"[TG_CONNECT] FAILED: {type(e).__name__}: {e}", flush=True)
        await client.disconnect()
        raise


async def verify_code(phone: str, code: str, password_2fa: str | None = None) -> TgAccount:
    """Step 2: verify code (and optional 2FA), save session, return TgAccount."""
    entry = _pending_auth.get(phone)
    client = entry[0] if entry else None
    if not client:
        raise ValueError("No pending auth for this phone. Call connect first.")

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        if not password_2fa:
            raise ValueError("2FA password required")
        await client.sign_in(password=password_2fa)

    # Fetch display name from Telegram
    me = await client.get_me()
    display_name = None
    if me:
        parts = [getattr(me, "first_name", "") or "", getattr(me, "last_name", "") or ""]
        display_name = " ".join(p for p in parts if p).strip() or getattr(me, "username", None) or None

    # Save to DB (upsert — reactivate if already exists)
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.phone == phone))
        account = result.scalar_one_or_none()
        if account:
            account.session_file = _session_path(phone)
            account.is_active = True
            account.display_name = display_name
        else:
            account = TgAccount(
                phone=phone,
                session_file=_session_path(phone),
                is_active=True,
                display_name=display_name,
            )
            db.add(account)
        await db.commit()
        await db.refresh(account)

    _pending_auth.pop(phone, None)

    # Start listening
    await _start_listener(account, client)
    return account


def _extract_media(msg_obj) -> tuple[str | None, str | None]:
    """Determine media type and extension from a Telethon message."""
    if msg_obj.photo:
        return "photo", ".jpg"
    if msg_obj.video:
        return "video", ".mp4"
    if msg_obj.voice:
        return "voice", ".ogg"
    if msg_obj.sticker:
        # Determine sticker format: webp (static), webm (video), tgs (animated lottie)
        sticker_ext = ".webp"
        if msg_obj.document and msg_obj.document.mime_type:
            mime = msg_obj.document.mime_type
            if "webm" in mime:
                sticker_ext = ".webm"
            elif "tgs" in mime or "gzip" in mime:
                sticker_ext = ".tgs"
        return "sticker", sticker_ext
    if msg_obj.document:
        # Try to get extension from document filename attribute
        ext = ""
        for attr in (msg_obj.document.attributes or []):
            if hasattr(attr, "file_name") and attr.file_name:
                parts = attr.file_name.rsplit(".", 1)
                if len(parts) == 2:
                    ext = "." + parts[1].lower()
                break
        # Fallback: derive from mime_type
        if not ext and msg_obj.document.mime_type:
            mime = msg_obj.document.mime_type.lower()
            mime_map = {
                "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
                "image/webp": ".webp", "image/bmp": ".bmp",
                "video/mp4": ".mp4", "video/quicktime": ".mov",
                "audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/mp4": ".m4a",
                "application/pdf": ".pdf",
                "application/zip": ".zip",
            }
            ext = mime_map.get(mime, "")
        return "document", ext
    return None, None


def _extract_inline_buttons(msg_obj) -> str | None:
    """Serialize inline/reply keyboard buttons from a Telethon message to JSON."""
    import base64 as b64
    from telethon.tl.types import ReplyInlineMarkup, ReplyKeyboardMarkup, ReplyKeyboardHide
    if not msg_obj.reply_markup:
        return None
    # Keyboard removal marker
    if isinstance(msg_obj.reply_markup, ReplyKeyboardHide):
        return json.dumps({"hide_keyboard": True})
    if not hasattr(msg_obj.reply_markup, "rows"):
        return None
    try:
        is_inline = isinstance(msg_obj.reply_markup, ReplyInlineMarkup)
        is_reply_kb = isinstance(msg_obj.reply_markup, ReplyKeyboardMarkup)
        if not is_inline and not is_reply_kb:
            return None
        rows = []
        for row in msg_obj.reply_markup.rows:
            btn_row = []
            for btn in row.buttons:
                btn_data = {"text": btn.text}
                if hasattr(btn, "data") and btn.data:
                    btn_data["callback_data"] = b64.b64encode(btn.data).decode("ascii")
                elif hasattr(btn, "url") and btn.url:
                    btn_data["url"] = btn.url
                elif is_reply_kb:
                    # ReplyKeyboard buttons send text as message
                    btn_data["send_text"] = btn.text
                btn_row.append(btn_data)
            rows.append(btn_row)
        return json.dumps(rows) if rows else None
    except Exception:
        return None


async def _start_listener(account: TgAccount, client: TelegramClient) -> None:
    """Register message handler and store client."""
    _clients[account.id] = client

    @client.on(events.NewMessage(outgoing=True))
    async def on_outgoing_message(event):
        """Capture messages sent directly from Telegram (not through CRM)."""
        msg_obj = event.message
        chat = await event.get_chat()
        if not chat:
            return

        # Wait for CRM API to finish saving the message (avoid duplicate)
        await asyncio.sleep(1.5)

        is_group = event.is_group or event.is_channel
        peer_tg_id = event.chat_id

        async with async_session() as db:
            # Only save if contact exists and is approved
            result = await db.execute(
                select(Contact).where(
                    Contact.real_tg_id == peer_tg_id,
                    Contact.tg_account_id == account.id,
                ).limit(1)
            )
            contact = result.scalars().first()
            if not contact or contact.status != "approved":
                return

            # Skip if message already saved (sent through CRM)
            existing = await db.execute(
                select(Message).where(
                    Message.tg_message_id == msg_obj.id,
                    Message.contact_id == contact.id,
                ).limit(1)
            )
            if existing.scalars().first():
                return

            # Media
            media_type, ext = _extract_media(msg_obj)
            media_path = None
            if media_type and ext is not None:
                filename = f"{contact.id}_{msg_obj.id}{ext}"
                filepath = os.path.join(MEDIA_DIR, filename)
                actual_path = await msg_obj.download_media(file=filepath)
                if actual_path:
                    media_path = os.path.basename(actual_path)
                else:
                    media_path = filename

            # Forwarded from
            forwarded_from_alias = None
            if msg_obj.fwd_from:
                fwd = msg_obj.fwd_from
                fwd_peer_id = None
                if hasattr(fwd, "from_id") and fwd.from_id:
                    fid = fwd.from_id
                    if hasattr(fid, "user_id"):
                        fwd_peer_id = fid.user_id
                    elif hasattr(fid, "channel_id"):
                        fwd_peer_id = -fid.channel_id
                if fwd_peer_id:
                    fr = await db.execute(select(Contact).where(Contact.real_tg_id == fwd_peer_id))
                    fwd_contact = fr.scalar_one_or_none()
                    forwarded_from_alias = fwd_contact.alias if fwd_contact else "[hidden]"
                else:
                    forwarded_from_alias = "[hidden]"

            sanitized_content = sanitize_text(msg_obj.text)
            # For stickers, use emoji as content fallback
            if media_type == "sticker" and not sanitized_content:
                sanitized_content = getattr(msg_obj.document, "attributes", None) and next(
                    (getattr(a, "alt", None) for a in msg_obj.document.attributes if getattr(a, "alt", None)),
                    None,
                ) or None
            msg = Message(
                contact_id=contact.id,
                tg_message_id=msg_obj.id,
                direction="outgoing",
                content=sanitized_content,
                media_type=media_type,
                media_path=media_path,
                forwarded_from_alias=forwarded_from_alias,
            )
            db.add(msg)
            contact.last_message_at = func.now()
            await db.commit()
            await db.refresh(msg)

            # Broadcast to CRM (scoped to this account's org)
            await ws_manager.broadcast_to_admins({
                "type": "new_message",
                "contact_id": str(contact.id),
                "message": {
                    "id": str(msg.id),
                    "direction": "outgoing",
                    "content": msg.content,
                    "media_type": msg.media_type,
                    "media_path": msg.media_path,
                    "forwarded_from_alias": msg.forwarded_from_alias,
                    "is_deleted": False,
                    "created_at": str(msg.created_at),
                },
            }, org_id=account.org_id)

    @client.on(events.NewMessage(incoming=True))
    async def on_new_message(event):
      try:
        msg_obj = event.message
        chat = await event.get_chat()
        if not chat:
            return

        is_group = event.is_group or event.is_channel
        peer_tg_id = event.chat_id

        # Skip Telegram service account
        if peer_tg_id == 777000:
            return

        is_forum = getattr(chat, "forum", False)
        if is_forum or (is_group and getattr(chat, "megagroup", False)):
            chat_type = "supergroup"
        elif event.is_group:
            chat_type = "group"
        elif event.is_channel:
            chat_type = "channel"
        else:
            chat_type = "private"

        # Extract topic info for forum supergroups
        topic_id = None
        topic_name = None
        if is_forum and msg_obj.reply_to and hasattr(msg_obj.reply_to, "forum_topic") and msg_obj.reply_to.forum_topic:
            topic_id = getattr(msg_obj.reply_to, "reply_to_msg_id", None)
        elif is_forum:
            # General topic (id=1)
            topic_id = 1

        if topic_id is not None:
            topic_name = await _resolve_topic_name(client, peer_tg_id, topic_id, account.id)

        async with async_session() as db:
            # --- CONTACT LOOKUP / CREATION ---
            result = await db.execute(
                select(Contact).where(
                    Contact.real_tg_id == peer_tg_id,
                    Contact.tg_account_id == account.id,
                ).limit(1)
            )
            contact = result.scalars().first()

            is_new_contact = False
            first_name = ""
            username = None

            if not contact:
                is_new_contact = True

                if is_group:
                    group_title = getattr(chat, "title", None) or ""
                else:
                    sender = await event.get_sender()
                    first_name = getattr(sender, "first_name", "") or ""
                    username = getattr(sender, "username", None)

                # Retry alias generation on collision
                for _attempt in range(5):
                    count_result = await db.execute(select(func.count(Contact.id)))
                    seq = count_result.scalar() + 1 + _attempt

                    if is_group:
                        contact = Contact(
                            tg_account_id=account.id,
                            real_tg_id=peer_tg_id,
                            real_name_encrypted=encrypt(group_title),
                            real_username_encrypted=encrypt(getattr(chat, "username", None) or ""),
                            group_title_encrypted=encrypt(group_title),
                            alias=generate_alias(group_title, seq),
                            chat_type=chat_type,
                            is_forum=is_forum,
                            status="approved",
                        )
                    else:
                        contact = Contact(
                            tg_account_id=account.id,
                            real_tg_id=peer_tg_id,
                            real_name_encrypted=encrypt(first_name),
                            real_username_encrypted=encrypt(username) if username else None,
                            alias=generate_alias(first_name, seq),
                            chat_type="private",
                            status="approved",
                        )
                    db.add(contact)
                    try:
                        await db.commit()
                        await db.refresh(contact)
                        break
                    except Exception as e:
                        await db.rollback()
                        contact = None
                        if "unique" not in str(e).lower() and "duplicate" not in str(e).lower():
                            raise
                else:
                    print(f"[WARN] Failed to create contact after 5 attempts for {peer_tg_id}")
                    return

            elif contact.status == "blocked":
                return

            # --- SENDER ALIAS (groups only) ---
            sender_tg_id_val = None
            sender_alias_val = None
            if is_group:
                sender = await event.get_sender()
                if sender:
                    sender_tg_id_val = sender.id
                    sr = await db.execute(
                        select(Contact).where(
                            Contact.real_tg_id == sender.id,
                            Contact.chat_type == "private",
                        )
                    )
                    sender_contact = sr.scalar_one_or_none()
                    if sender_contact:
                        sender_alias_val = sender_contact.alias
                    else:
                        fname = getattr(sender, "first_name", None) or ""
                        sender_alias_val = f"{fname[:2]}-grp" if fname else "Usr-grp"

            # --- FORWARDED FROM ---
            forwarded_from_alias = None
            if msg_obj.fwd_from:
                fwd = msg_obj.fwd_from
                fwd_peer_id = None
                if hasattr(fwd, "from_id") and fwd.from_id:
                    fid = fwd.from_id
                    if hasattr(fid, "user_id"):
                        fwd_peer_id = fid.user_id
                    elif hasattr(fid, "channel_id"):
                        fwd_peer_id = -fid.channel_id

                if fwd_peer_id:
                    fr = await db.execute(select(Contact).where(Contact.real_tg_id == fwd_peer_id))
                    fwd_contact = fr.scalar_one_or_none()
                    forwarded_from_alias = fwd_contact.alias if fwd_contact else "[hidden]"
                else:
                    forwarded_from_alias = "[hidden]"

            # --- REPLY TO ---
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

            # --- INLINE BUTTONS ---
            inline_buttons_json = _extract_inline_buttons(msg_obj)

            # --- MEDIA ---
            media_type, ext = _extract_media(msg_obj)
            media_path = None
            if media_type and ext is not None:
                filename = f"{contact.id}_{msg_obj.id}{ext}"
                filepath = os.path.join(MEDIA_DIR, filename)
                actual_path = await msg_obj.download_media(file=filepath)
                if actual_path:
                    # Telethon may append extension for documents
                    media_path = os.path.basename(actual_path)
                else:
                    media_path = filename

            # --- SAVE MESSAGE ---
            sanitized_content = sanitize_text(msg_obj.text)
            # For stickers, use emoji as content fallback
            if media_type == "sticker" and not sanitized_content:
                sanitized_content = getattr(msg_obj.document, "attributes", None) and next(
                    (getattr(a, "alt", None) for a in msg_obj.document.attributes if getattr(a, "alt", None)),
                    None,
                ) or None
            msg = Message(
                contact_id=contact.id,
                tg_message_id=msg_obj.id,
                direction="incoming",
                content=sanitized_content,
                media_type=media_type,
                media_path=media_path,
                reply_to_tg_msg_id=reply_to_tg_msg_id,
                reply_to_msg_id=reply_to_msg_id,
                reply_to_content_preview=reply_to_content_preview,
                forwarded_from_alias=forwarded_from_alias,
                sender_tg_id=sender_tg_id_val,
                sender_alias=sender_alias_val,
                inline_buttons=inline_buttons_json,
                topic_id=topic_id,
                topic_name=topic_name,
            )
            db.add(msg)
            contact.last_message_at = func.now()
            # Save before commit — attributes expire after commit
            contact_assigned_to = contact.assigned_to
            contact_tg_account_id = contact.tg_account_id
            await db.commit()
            await db.refresh(msg)

            # --- NOTIFICATIONS ---
            if is_new_contact:
                try:
                    await ws_manager.broadcast_to_admins({
                        "type": "new_contact",
                        "contact_id": str(contact.id),
                        "alias": contact.alias,
                    }, org_id=account.org_id)
                    from bot import notify_new_contact
                    if is_group:
                        group_title = getattr(chat, "title", None) or ""
                        await notify_new_contact(
                            contact, group_title, None, sanitized_content,
                            chat_type=chat_type,
                        )
                    else:
                        if not first_name:
                            sender = await event.get_sender()
                            first_name = getattr(sender, "first_name", "") or ""
                            username = getattr(sender, "username", None)
                        await notify_new_contact(contact, first_name, username, sanitized_content)
                except Exception as e:
                    print(f"Bot notification failed: {e}")

            # --- WS BROADCAST ---
            if contact.status == "approved":
                ws_event = {
                    "type": "new_message",
                    "contact_id": str(contact.id),
                    "message": {
                        "id": str(msg.id),
                        "direction": "incoming",
                        "content": msg.content,
                        "media_type": msg.media_type,
                        "media_path": msg.media_path,
                        "reply_to_msg_id": str(msg.reply_to_msg_id) if msg.reply_to_msg_id else None,
                        "reply_to_content_preview": msg.reply_to_content_preview,
                        "forwarded_from_alias": msg.forwarded_from_alias,
                        "sender_alias": msg.sender_alias,
                        "topic_id": msg.topic_id,
                        "topic_name": msg.topic_name,
                        "inline_buttons": msg.inline_buttons,
                        "is_deleted": False,
                        "created_at": str(msg.created_at),
                    },
                }
                await ws_manager.broadcast_to_admins(ws_event, org_id=account.org_id)

                # Bot notification for new messages in approved chats
                if not is_new_contact:
                    try:
                        from bot import notify_new_message
                        # Pass assigned_to explicitly — contact may be detached from session
                        await notify_new_message(
                            contact, sanitized_content, sender_alias_val,
                            assigned_to=contact_assigned_to,
                            tg_account_id=contact_tg_account_id,
                        )
                    except Exception as e:
                        print(f"[NOTIFY] Message notification failed: {e}")
      except Exception as e:
        import traceback
        print(f"[LISTENER] Error processing incoming message: {e}")
        traceback.print_exc()

    @client.on(events.MessageEdited)
    async def on_message_edited(event):
        msg_obj = event.message
        async with async_session() as db:
            result = await db.execute(
                select(Message).where(
                    Message.tg_message_id == msg_obj.id,
                )
            )
            msg = result.scalar_one_or_none()
            if not msg:
                return

            new_content = sanitize_text(msg_obj.text)
            new_buttons = _extract_inline_buttons(msg_obj)

            # Detect if content actually changed (reactions trigger MessageEdited too)
            content_changed = (new_content or "") != (msg.content or "")
            buttons_changed = new_buttons != msg.inline_buttons

            if not content_changed and not buttons_changed:
                return  # Reaction or pin — not a real edit

            # Save edit history before updating
            if content_changed:
                history = MessageEditHistory(
                    message_id=msg.id,
                    old_content=msg.content,
                    new_content=new_content,
                )
                db.add(history)

            msg.content = new_content
            msg.inline_buttons = new_buttons
            if content_changed:
                msg.is_edited = True
            await db.commit()

            await ws_manager.broadcast_to_admins({
                "type": "message_edited",
                "contact_id": str(msg.contact_id),
                "message_id": str(msg.id),
                "content": msg.content,
                "inline_buttons": msg.inline_buttons,
                "is_edited": msg.is_edited,
            }, org_id=account.org_id)

    @client.on(events.MessageRead(inbox=False))
    async def on_message_read(event):
        """Track when the other party reads our outgoing messages."""
        max_id = event.max_id
        peer_id = event.chat_id
        if not max_id or not peer_id:
            return
        async with async_session() as db:
            # Find contact by peer tg_id
            result = await db.execute(
                select(Contact).where(
                    Contact.tg_account_id == account.id,
                    Contact.real_tg_id == peer_id,
                )
            )
            contact = result.scalar_one_or_none()
            if not contact:
                return

            # Mark all outgoing messages up to max_id as read
            result = await db.execute(
                select(Message).where(
                    Message.contact_id == contact.id,
                    Message.direction == "outgoing",
                    Message.tg_message_id <= max_id,
                    Message.is_read.is_(False),
                )
            )
            unread_msgs = result.scalars().all()
            if not unread_msgs:
                return
            msg_ids = []
            for m in unread_msgs:
                m.is_read = True
                msg_ids.append(str(m.id))
            await db.commit()

            await ws_manager.broadcast_to_admins({
                "type": "messages_read",
                "contact_id": str(contact.id),
                "message_ids": msg_ids,
            }, org_id=account.org_id)

    @client.on(events.MessageDeleted)
    async def on_message_deleted(event):
        deleted_ids = event.deleted_ids
        if not deleted_ids:
            return
        async with async_session() as db:
            for tg_msg_id in deleted_ids:
                result = await db.execute(
                    select(Message).where(
                        Message.tg_message_id == tg_msg_id,
                        Message.direction == "incoming",
                    )
                )
                msg = result.scalar_one_or_none()
                if msg:
                    msg.is_deleted = True
                    await db.commit()
                    await ws_manager.broadcast_to_admins({
                        "type": "message_deleted",
                        "contact_id": str(msg.contact_id),
                        "message_id": str(msg.id),
                    }, org_id=account.org_id)

    # Run client in background with auto-reconnect
    async def _run_with_reconnect():
        while True:
            try:
                await client.run_until_disconnected()
            except Exception as e:
                print(f"[TELETHON] Client for {account.phone} disconnected: {e}")
            # Check if we were intentionally removed
            if account.id not in _clients:
                break
            print(f"[TELETHON] Reconnecting {account.phone} in 5s...")
            await asyncio.sleep(5)
            try:
                await client.connect()
                if await client.is_user_authorized():
                    print(f"[TELETHON] Reconnected {account.phone}")
                else:
                    print(f"[TELETHON] {account.phone} no longer authorized, stopping")
                    break
            except Exception as e:
                print(f"[TELETHON] Reconnect failed for {account.phone}: {e}")

    asyncio.create_task(_run_with_reconnect())


async def _try_reconnect(account_id: UUID) -> TelegramClient | None:
    """Try to reconnect a disconnected account and return the client."""
    async with async_session() as db:
        result = await db.execute(
            select(TgAccount).where(TgAccount.id == account_id, TgAccount.is_active.is_(True))
        )
        account = result.scalar_one_or_none()
    if not account:
        return None
    try:
        client = TelegramClient(
            account.session_file,
            settings.TG_API_ID,
            settings.TG_API_HASH,
        )
        await client.connect()
        if await client.is_user_authorized():
            await _start_listener(account, client)
            print(f"[RECONNECT] Successfully reconnected {account.phone}")
            return client
        else:
            print(f"[RECONNECT] {account.phone} not authorized")
            await client.disconnect()
            return None
    except Exception as e:
        print(f"[RECONNECT] Failed for account {account_id}: {e}")
        return None


async def send_message(
    account_id: UUID,
    tg_id: int,
    text: str | None = None,
    file_path: str | None = None,
    reply_to_tg_msg_id: int | None = None,
    media_type: str | None = None,
) -> int | None:
    """Send a message or file via Telethon and return tg_message_id.

    media_type: photo | video | document | voice | video_note
    - voice: sends as voice message (ogg opus)
    - video_note: sends as round video message (circle)
    """
    client = _clients.get(account_id)
    if not client:
        client = await _try_reconnect(account_id)
    if not client:
        raise ValueError("Telegram-аккаунт не подключён. Проверьте подключение в настройках.")
    kwargs = {}
    if reply_to_tg_msg_id:
        kwargs["reply_to"] = reply_to_tg_msg_id
    if file_path:
        if media_type == "voice":
            kwargs["voice_note"] = True
            result = await client.send_file(tg_id, file_path, caption=text or "", **kwargs)
        elif media_type == "video_note":
            kwargs["video_note"] = True
            result = await client.send_file(tg_id, file_path, **kwargs)  # video notes have no caption
        else:
            result = await client.send_file(tg_id, file_path, caption=text or "", **kwargs)
    else:
        result = await client.send_message(tg_id, text, **kwargs)
    return result.id


async def forward_message(
    account_id: UUID,
    from_tg_id: int,
    tg_msg_ids: list[int],
    to_tg_id: int,
    media_only: bool = False,
) -> list[int]:
    """Copy messages to another chat without 'Forwarded from' header."""
    client = _clients.get(account_id)
    if not client:
        client = await _try_reconnect(account_id)
    if not client:
        raise ValueError("Telegram-аккаунт не подключён. Проверьте подключение в настройках.")

    sent_ids = []
    for tg_msg_id in tg_msg_ids:
        try:
            # Get the original message
            orig = await client.get_messages(from_tg_id, ids=tg_msg_id)
            if not orig:
                continue

            # Re-send as new message (no forward header)
            if orig.media:
                caption = "" if media_only else (orig.text or "")
                result = await client.send_file(
                    to_tg_id, orig.media, caption=caption,
                )
            elif orig.text and not media_only:
                result = await client.send_message(to_tg_id, orig.text)
            else:
                continue
            sent_ids.append(result.id)
        except Exception as e:
            print(f"[FORWARD] Failed to copy message {tg_msg_id}: {e}")
    return sent_ids


async def delete_messages(
    account_id: UUID,
    tg_peer_id: int,
    tg_msg_ids: list[int],
) -> None:
    """Delete messages from Telegram chat."""
    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")
    from telethon.tl.functions.messages import DeleteMessagesRequest
    try:
        await client.delete_messages(tg_peer_id, tg_msg_ids)
    except Exception as e:
        print(f"[DELETE] Failed to delete messages: {e}")


async def press_inline_button(
    account_id: UUID,
    tg_peer_id: int,
    tg_msg_id: int,
    callback_data: bytes,
) -> str | None:
    """Press a bot inline button and return the response text."""
    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")
    result = await client(GetBotCallbackAnswerRequest(
        peer=tg_peer_id,
        msg_id=tg_msg_id,
        data=callback_data,
    ))
    return result.message or result.url


async def startup_listeners() -> None:
    """On app startup, reconnect all active TG accounts."""
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.is_active.is_(True)))
        accounts = result.scalars().all()

    print(f"[STARTUP] Found {len(accounts)} active TG accounts")
    for account in accounts:
        try:
            print(f"[STARTUP] Connecting {account.phone} (session: {account.session_file})")
            client = TelegramClient(
                account.session_file,
                settings.TG_API_ID,
                settings.TG_API_HASH,
            )
            await client.connect()
            authorized = await client.is_user_authorized()
            print(f"[STARTUP] {account.phone} authorized={authorized}")
            if authorized:
                # Backfill display_name if missing
                if not account.display_name:
                    try:
                        me = await client.get_me()
                        if me:
                            parts = [getattr(me, "first_name", "") or "", getattr(me, "last_name", "") or ""]
                            dn = " ".join(p for p in parts if p).strip() or getattr(me, "username", None) or None
                            if dn:
                                async with async_session() as db2:
                                    res2 = await db2.execute(select(TgAccount).where(TgAccount.id == account.id))
                                    acc2 = res2.scalar_one_or_none()
                                    if acc2:
                                        acc2.display_name = dn
                                        await db2.commit()
                                        account.display_name = dn
                    except Exception as e2:
                        print(f"[STARTUP] Could not fetch display_name for {account.phone}: {e2}")
                await _start_listener(account, client)
                print(f"[STARTUP] Listener started for {account.phone}")
            else:
                print(f"[STARTUP] {account.phone} NOT authorized, skipping")
        except Exception as e:
            print(f"[STARTUP] Failed to reconnect {account.phone}: {e}")


async def shutdown_listeners() -> None:
    """Disconnect all clients on shutdown."""
    for client in _clients.values():
        try:
            await client.disconnect()
        except Exception:
            pass
    _clients.clear()


async def disconnect_account(account_id: UUID) -> None:
    """Disconnect and deactivate a specific TG account."""
    client = _clients.pop(account_id, None)
    if client:
        await client.disconnect()


async def create_group(account_id: UUID, title: str, user_ids: list[int] = []) -> int:
    """Create a new Telegram group chat and return its ID."""
    from telethon.tl.functions.messages import CreateChatRequest

    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")

    # CreateChatRequest requires at least one other user; use bot if none specified
    members = user_ids if user_ids else []
    if not members:
        bot_username = settings.TG_BOT_TOKEN.split(":")[0]
        try:
            bot_entity = await client.get_input_entity(int(bot_username))
            members_input = [bot_entity]
        except Exception:
            members_input = [await client.get_me()]
    else:
        members_input = members

    result = await client(CreateChatRequest(
        users=members_input,
        title=title,
    ))

    # Extract chat ID from the result
    # InvitedUsers has .updates (Updates obj) which has .chats and .updates (list)
    obj = result
    # Unwrap InvitedUsers -> Updates
    if hasattr(obj, "updates") and not isinstance(getattr(obj, "updates", None), (list, tuple)):
        inner = obj.updates
        if hasattr(inner, "chats") and inner.chats:
            return inner.chats[0].id
    # Direct .chats
    if hasattr(obj, "chats") and obj.chats:
        return obj.chats[0].id
    # Walk .updates list
    update_list = getattr(obj, "updates", None)
    if isinstance(update_list, (list, tuple)):
        for upd in update_list:
            if hasattr(upd, "message") and hasattr(upd.message, "peer_id"):
                peer = upd.message.peer_id
                if hasattr(peer, "chat_id"):
                    return peer.chat_id
    print(f"[CREATE_GROUP] Failed. type={type(result)}, attrs={[a for a in dir(result) if not a.startswith('_')]}")
    if hasattr(result, "updates"):
        u = result.updates
        print(f"[CREATE_GROUP] .updates type={type(u)}, attrs={[a for a in dir(u) if not a.startswith('_')]}")
    raise ValueError(f"Could not extract chat ID from result: {type(result)}")


async def add_group_member(account_id: UUID, chat_tg_id: int, user_identifier: str) -> None:
    """Add a user to a Telegram group by username or user ID."""
    from telethon.tl.functions.messages import AddChatUserRequest

    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")

    # Resolve user by username or numeric ID
    try:
        user_id = int(user_identifier)
        user_entity = await client.get_input_entity(user_id)
    except ValueError:
        # It's a username
        username = user_identifier.lstrip("@")
        user_entity = await client.get_input_entity(username)

    await client(AddChatUserRequest(
        chat_id=chat_tg_id,
        user_id=user_entity,
        fwd_limit=50,
    ))

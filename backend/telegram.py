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
from models import Contact, Message, TgAccount, async_session
from ws import ws_manager

SESSIONS_DIR = "sessions"
MEDIA_DIR = "media"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

# Active Telethon clients: tg_account_id -> TelegramClient
_clients: dict[UUID, TelegramClient] = {}

# Pending auth flows: phone -> TelegramClient (temporary, before session saved)
_pending_auth: dict[str, TelegramClient] = {}

_USERNAME_RE = re.compile(r"@[A-Za-z0-9_]{4,}")


def _session_path(phone: str) -> str:
    clean = phone.replace("+", "").replace(" ", "")
    return os.path.join(SESSIONS_DIR, clean)


def generate_alias(real_name: str | None, sequence: int) -> str:
    if not real_name or not real_name.strip():
        return f"Us-{sequence:03d}"
    prefix = real_name.strip()[:2]
    return f"{prefix}-{sequence:03d}"


def sanitize_text(text: str | None) -> str | None:
    """Replace @username mentions with [hidden]."""
    if not text:
        return text
    return _USERNAME_RE.sub("[hidden]", text)


async def start_connect(phone: str) -> None:
    """Step 1: send auth code to the phone."""
    client = TelegramClient(
        _session_path(phone),
        settings.TG_API_ID,
        settings.TG_API_HASH,
    )
    await client.connect()
    await client.send_code_request(phone)
    _pending_auth[phone] = client


async def verify_code(phone: str, code: str, password_2fa: str | None = None) -> TgAccount:
    """Step 2: verify code (and optional 2FA), save session, return TgAccount."""
    client = _pending_auth.get(phone)
    if not client:
        raise ValueError("No pending auth for this phone. Call connect first.")

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        if not password_2fa:
            raise ValueError("2FA password required")
        await client.sign_in(password=password_2fa)

    # Save to DB
    async with async_session() as db:
        account = TgAccount(
            phone=phone,
            session_file=_session_path(phone),
            is_active=True,
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
    if msg_obj.document:
        return "document", ""
    return None, None


def _extract_inline_buttons(msg_obj) -> str | None:
    """Serialize inline keyboard buttons from a Telethon message to JSON."""
    import base64 as b64
    if not msg_obj.reply_markup or not hasattr(msg_obj.reply_markup, "rows"):
        return None
    try:
        rows = []
        for row in msg_obj.reply_markup.rows:
            btn_row = []
            for btn in row.buttons:
                btn_data = {"text": btn.text}
                if hasattr(btn, "data") and btn.data:
                    # Store as base64 to preserve binary callback data
                    btn_data["callback_data"] = b64.b64encode(btn.data).decode("ascii")
                elif hasattr(btn, "url") and btn.url:
                    btn_data["url"] = btn.url
                btn_row.append(btn_data)
            rows.append(btn_row)
        return json.dumps(rows) if rows else None
    except Exception:
        return None


async def _start_listener(account: TgAccount, client: TelegramClient) -> None:
    """Register message handler and store client."""
    _clients[account.id] = client

    @client.on(events.NewMessage(incoming=True))
    async def on_new_message(event):
        msg_obj = event.message
        chat = await event.get_chat()
        if not chat:
            return

        is_group = event.is_group or event.is_channel
        peer_tg_id = event.chat_id
        chat_type = "group" if event.is_group else ("channel" if event.is_channel else "private")

        async with async_session() as db:
            # --- CONTACT LOOKUP / CREATION ---
            result = await db.execute(
                select(Contact).where(
                    Contact.real_tg_id == peer_tg_id,
                    Contact.tg_account_id == account.id,
                )
            )
            contact = result.scalar_one_or_none()

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
                            status="pending",
                        )
                    else:
                        contact = Contact(
                            tg_account_id=account.id,
                            real_tg_id=peer_tg_id,
                            real_name_encrypted=encrypt(first_name),
                            real_username_encrypted=encrypt(username) if username else None,
                            alias=generate_alias(first_name, seq),
                            chat_type="private",
                            status="pending",
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
                await msg_obj.download_media(file=filepath)
                media_path = filename

            # --- SAVE MESSAGE ---
            sanitized_content = sanitize_text(msg_obj.text)
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
                        "type": "new_pending",
                        "contact_id": str(contact.id),
                        "alias": contact.alias,
                    })
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
                        "inline_buttons": msg.inline_buttons,
                        "is_deleted": False,
                        "created_at": str(msg.created_at),
                    },
                }
                await ws_manager.broadcast_to_admins(ws_event)

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

    @client.on(events.MessageEdited)
    async def on_message_edited(event):
        msg_obj = event.message
        async with async_session() as db:
            # Match by tg_message_id (could be incoming or outgoing)
            result = await db.execute(
                select(Message).where(
                    Message.tg_message_id == msg_obj.id,
                )
            )
            msg = result.scalar_one_or_none()
            if not msg:
                return

            # Update content, inline buttons, mark as edited
            msg.content = sanitize_text(msg_obj.text)
            msg.inline_buttons = _extract_inline_buttons(msg_obj)
            msg.is_edited = True
            await db.commit()

            await ws_manager.broadcast_to_admins({
                "type": "message_edited",
                "contact_id": str(msg.contact_id),
                "message_id": str(msg.id),
                "content": msg.content,
                "inline_buttons": msg.inline_buttons,
                "is_edited": True,
            })

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
                    })

    # Run client in background
    asyncio.create_task(client.run_until_disconnected())


async def send_message(
    account_id: UUID,
    tg_id: int,
    text: str | None = None,
    file_path: str | None = None,
    reply_to_tg_msg_id: int | None = None,
) -> int | None:
    """Send a message or file via Telethon and return tg_message_id."""
    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")
    kwargs = {}
    if reply_to_tg_msg_id:
        kwargs["reply_to"] = reply_to_tg_msg_id
    if file_path:
        result = await client.send_file(tg_id, file_path, caption=text or "", **kwargs)
    else:
        result = await client.send_message(tg_id, text, **kwargs)
    return result.id


async def forward_message(
    account_id: UUID,
    from_tg_id: int,
    tg_msg_ids: list[int],
    to_tg_id: int,
) -> list[int]:
    """Forward messages natively via Telegram."""
    client = _clients.get(account_id)
    if not client:
        raise ValueError("Telegram account not connected")
    result = await client.forward_messages(
        entity=to_tg_id,
        messages=tg_msg_ids,
        from_peer=from_tg_id,
    )
    if isinstance(result, list):
        return [r.id for r in result if r]
    return [result.id] if result else []


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

    for account in accounts:
        try:
            client = TelegramClient(
                account.session_file,
                settings.TG_API_ID,
                settings.TG_API_HASH,
            )
            await client.connect()
            if await client.is_user_authorized():
                await _start_listener(account, client)
        except Exception as e:
            print(f"Failed to reconnect {account.phone}: {e}")


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

import asyncio
import json
import os
import re
import time as _time_module
from uuid import UUID

from sqlalchemy import func, select
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError
import functools


# ─── last_message_* debounce ─────────────────────────────────────────
# Every incoming/outgoing TG message writes a new Message row AND updates
# the denormalized preview on Contact (last_message_at/_content/_direction/
# _is_read). During a burst (e.g. 50 messages in one chat within a second)
# those contact updates all target the same row and serialize behind
# row-level locks, burning one DB connection each until the pool is dry.
#
# Debounce: skip the Contact field update if the same contact had an
# identical-direction update in the last 200ms. The Message row itself is
# still persisted — only the denormalized preview is slightly stale. On a
# direction flip (incoming→outgoing or vice-versa) we always update so the
# UI's read/unread state doesn't lag.
_LAST_MSG_DEBOUNCE: dict[UUID, tuple[float, str]] = {}
_LAST_MSG_DEBOUNCE_WINDOW_S = 0.2


def _should_skip_last_message_update(contact_id: UUID, direction: str) -> bool:
    now = _time_module.monotonic()
    prev = _LAST_MSG_DEBOUNCE.get(contact_id)
    if prev and (now - prev[0]) < _LAST_MSG_DEBOUNCE_WINDOW_S and prev[1] == direction:
        return True
    _LAST_MSG_DEBOUNCE[contact_id] = (now, direction)
    return False


def _safe_handler(func):
    """Wrap Telethon event handler in try-except to prevent crashes."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            print(f"[HANDLER-ERROR] {func.__name__}: {type(e).__name__}: {e}", flush=True)
    return wrapper
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest, GetAllDraftsRequest

from config import settings
from crypto import encrypt, encrypt_session, decrypt_session, is_session_encrypted


def _compress_photo(filepath: str) -> str:
    """Compress photo to JPEG 85% quality. Returns new path."""
    try:
        from PIL import Image
        img = Image.open(filepath)
        if img.mode == "RGBA":
            img = img.convert("RGB")
        # Resize if too large (max 2048px on longest side)
        max_dim = 2048
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        # Save as JPEG
        new_path = os.path.splitext(filepath)[0] + ".jpg"
        img.save(new_path, "JPEG", quality=85, optimize=True)
        # Remove original if different path
        if new_path != filepath and os.path.exists(filepath):
            os.remove(filepath)
        return new_path
    except Exception:
        return filepath
from models import Contact, Message, MessageEditHistory, TgAccount, async_session
from ws import ws_manager

SESSIONS_DIR = "sessions"
MEDIA_DIR = "media"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

# Active Telethon clients: tg_account_id -> TelegramClient
_clients: dict[UUID, TelegramClient] = {}

# Tracker for messages sent by the CRM API (via `send_message` below).
# The Telethon "outgoing" event fires for these too, racing with the CRM
# API's own Message insert. Before this tracker, the outgoing handler
# slept 1.5s to let the CRM commit first — a per-send latency hit that
# also capped throughput at ~0.67 outgoing events/sec. Now the CRM path
# marks the tg_msg_id here and the listener skips it immediately.
#
# Key: (account_id, tg_message_id). Value: expiry unix ts. Bounded size;
# stale entries are evicted opportunistically on insert.
_crm_sent_tracker: dict[tuple, float] = {}
_CRM_SENT_TTL = 30.0


def _mark_crm_sent(account_id: UUID, tg_msg_id: int) -> None:
    now = _time_module.time()
    _crm_sent_tracker[(account_id, tg_msg_id)] = now + _CRM_SENT_TTL
    # Opportunistic GC: when the dict gets "big", drop expired entries.
    if len(_crm_sent_tracker) > 1000:
        expired = [k for k, exp in _crm_sent_tracker.items() if exp < now]
        for k in expired:
            _crm_sent_tracker.pop(k, None)


def _is_crm_sent(account_id: UUID, tg_msg_id: int) -> bool:
    exp = _crm_sent_tracker.get((account_id, tg_msg_id))
    if exp is None:
        return False
    if exp < _time_module.time():
        _crm_sent_tracker.pop((account_id, tg_msg_id), None)
        return False
    return True

_reconnect_locks: dict[UUID, asyncio.Lock] = {}  # per-account reconnect lock

# Per-account error tracking for circuit breaker
_account_errors: dict[UUID, int] = {}  # consecutive error count
_account_status: dict[UUID, str] = {}  # "connected" | "reconnecting" | "failed" | "disabled"
MAX_CONSECUTIVE_ERRORS = 10  # disable account handler after this many errors

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
_topic_cache: dict[tuple, tuple[str, float]] = {}  # key -> (name, timestamp)
_TOPIC_CACHE_TTL = 3600  # 1 hour


async def _resolve_topic_name(client: TelegramClient, peer_id: int, topic_id: int, account_id: UUID) -> str | None:
    """Resolve topic name for a forum supergroup, with caching."""
    if topic_id == 1:
        return "General"
    import time as _time
    cache_key = (account_id, peer_id, topic_id)
    cached = _topic_cache.get(cache_key)
    if cached and _time.time() - cached[1] < _TOPIC_CACHE_TTL:
        return cached[0]
    try:
        # The topic creation message (service message) has the topic title
        # Topic ID = message ID of the service message that created the topic
        from telethon.tl.functions.channels import GetForumTopicsByIDRequest
        entity = await client.get_input_entity(peer_id)
        result = await client(GetForumTopicsByIDRequest(channel=entity, topics=[topic_id]))
        if result.topics:
            name = result.topics[0].title
            _topic_cache[cache_key] = (name, _time.time())
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
            _topic_cache[cache_key] = (name, _time.time())
            return name
    except Exception:
        pass
    return None


def _session_path(phone: str) -> str:
    clean = phone.replace("+", "").replace(" ", "")
    return os.path.join(SESSIONS_DIR, clean)


# Telegram stripped-thumbnail reconstruction constants. A stripped_thumb is
# a ~100-byte compressed JPEG with header and footer removed to save bandwidth.
# To render it we prepend the exact canonical JPEG header below (substituting
# the width/height bytes from the payload at offsets 164 and 166), append the
# JFIF footer, and the result is a valid ~700-1200 byte JPEG (blurry, 40-60px).
# Byte sequence verified against Pyrogram's parse_thumbnail_strip and
# Telegram Desktop's image reconstruction code.
_STRIPPED_JPEG_HEADER = (
    b"\xff\xd8\xff\xe0\x00\x10\x4a\x46\x49\x46\x00\x01\x01\x00\x00\x01"
    b"\x00\x01\x00\x00\xff\xdb\x00\x43\x00\x28\x1c\x1e\x23\x1e\x19\x28"
    b"\x23\x21\x23\x2d\x2b\x28\x30\x3c\x64\x41\x3c\x37\x37\x3c\x7b\x58"
    b"\x5d\x49\x64\x91\x80\x99\x96\x8f\x80\x8c\x8a\xa0\xb4\xe6\xc3\xa0"
    b"\xaa\xda\xad\x8a\x8c\xc8\xff\xcb\xda\xee\xf5\xff\xff\xff\x9b\xc1"
    b"\xff\xff\xff\xfa\xff\xe6\xfd\xff\xf8\xff\xdb\x00\x43\x01\x2b\x2d"
    b"\x2d\x3c\x35\x3c\x76\x41\x41\x76\xf8\xa5\x8c\xa5\xf8\xf8\xf8\xf8"
    b"\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8"
    b"\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8"
    b"\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8\xf8"
    b"\xff\xc0\x00\x11\x08\x00\x00\x00\x00\x03\x01\x22\x00\x02\x11\x01"
    b"\x03\x11\x01\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01"
    b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07"
    b"\x08\x09\x0a\x0b\xff\xc4\x00\xb5\x10\x00\x02\x01\x03\x03\x02\x04"
    b"\x03\x05\x05\x04\x04\x00\x00\x01\x7d\x01\x02\x03\x00\x04\x11\x05"
    b"\x12\x21\x31\x41\x06\x13\x51\x61\x07\x22\x71\x14\x32\x81\x91\xa1"
    b"\x08\x23\x42\xb1\xc1\x15\x52\xd1\xf0\x24\x33\x62\x72\x82\x09\x0a"
    b"\x16\x17\x18\x19\x1a\x25\x26\x27\x28\x29\x2a\x34\x35\x36\x37\x38"
    b"\x39\x3a\x43\x44\x45\x46\x47\x48\x49\x4a\x53\x54\x55\x56\x57\x58"
    b"\x59\x5a\x63\x64\x65\x66\x67\x68\x69\x6a\x73\x74\x75\x76\x77\x78"
    b"\x79\x7a\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97"
    b"\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5"
    b"\xb6\xb7\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3"
    b"\xd4\xd5\xd6\xd7\xd8\xd9\xda\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9"
    b"\xea\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xff\xc4\x00\x1f\x01"
    b"\x00\x03\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00"
    b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\xff\xc4\x00\xb5"
    b"\x11\x00\x02\x01\x02\x04\x04\x03\x04\x07\x05\x04\x04\x00\x01\x02"
    b"\x77\x00\x01\x02\x03\x11\x04\x05\x21\x31\x06\x12\x41\x51\x07\x61"
    b"\x71\x13\x22\x32\x81\x08\x14\x42\x91\xa1\xb1\xc1\x09\x23\x33\x52"
    b"\xf0\x15\x62\x72\xd1\x0a\x16\x24\x34\xe1\x25\xf1\x17\x18\x19\x1a"
    b"\x26\x27\x28\x29\x2a\x35\x36\x37\x38\x39\x3a\x43\x44\x45\x46\x47"
    b"\x48\x49\x4a\x53\x54\x55\x56\x57\x58\x59\x5a\x63\x64\x65\x66\x67"
    b"\x68\x69\x6a\x73\x74\x75\x76\x77\x78\x79\x7a\x82\x83\x84\x85\x86"
    b"\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4"
    b"\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2"
    b"\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9"
    b"\xda\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf2\xf3\xf4\xf5\xf6\xf7"
    b"\xf8\xf9\xfa\xff\xda\x00\x0c\x03\x01\x00\x02\x11\x03\x11\x00\x3f"
    b"\x00"
)
_STRIPPED_JPEG_FOOTER = b"\xff\xd9"
# Exact length check: the width/height substitution below depends on SOF0
# starting at byte 158, so the header must be exactly 623 bytes. Any drift
# would corrupt the JPEG and the browser would show a broken image.
assert len(_STRIPPED_JPEG_HEADER) == 623, (
    f"stripped JPEG header must be 623 bytes, got {len(_STRIPPED_JPEG_HEADER)}"
)


def stripped_thumb_to_data_url(stripped: bytes | None) -> str | None:
    """Expand a Telegram stripped thumbnail into a base64 JPEG data URL.

    Returns `data:image/jpeg;base64,...` or None on malformed input.
    The frontend uses this as `<img src>` for instant preview while the
    full-resolution avatar downloads in the background.
    """
    if not stripped or len(stripped) < 3 or stripped[0] != 0x01:
        return None
    try:
        import base64 as _b64
        header = bytearray(_STRIPPED_JPEG_HEADER)
        # Bytes 164 and 166 of the header hold width/height placeholders.
        header[164] = stripped[1]
        header[166] = stripped[2]
        jpg = bytes(header) + stripped[3:] + _STRIPPED_JPEG_FOOTER
        return "data:image/jpeg;base64," + _b64.b64encode(jpg).decode("ascii")
    except Exception:
        return None


def extract_stripped_thumb(entity) -> str | None:
    """Pull the stripped thumbnail out of a Telethon User/Chat/Channel entity.

    The field lives under `entity.photo.stripped_thumb` for users and
    `entity.photo.stripped_thumb` for chats/channels too. Returns a data URL
    ready to send to the frontend, or None if the entity has no photo or a
    photo without a stripped thumbnail.
    """
    photo = getattr(entity, "photo", None)
    if not photo:
        return None
    stripped = getattr(photo, "stripped_thumb", None)
    return stripped_thumb_to_data_url(stripped)


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
    # Try to load existing session_string from DB. Sessions are
    # encrypted at rest — decrypt_session transparently handles legacy
    # plaintext rows so we don't break existing accounts mid-rollout.
    session = StringSession()
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.phone == phone))
        existing = result.scalar_one_or_none()
        if existing and existing.session_string:
            session = StringSession(decrypt_session(existing.session_string))
            print(f"[TG_CONNECT] Loaded StringSession from DB for {phone}", flush=True)

    for attempt in range(2):
        client = TelegramClient(
            session,
            settings.TG_API_ID,
            settings.TG_API_HASH,
        )
        await client.connect()
        print(f"[TG_CONNECT] connected, is_authorized={await client.is_user_authorized()}", flush=True)
        try:
            result = await client.send_code_request(phone, force_sms=True)
            print(f"[TG_CONNECT] send_code OK: type={type(result.type).__name__}, hash={result.phone_code_hash[:8]}...", flush=True)
            import time
            if phone in _pending_auth:
                old_client, _ = _pending_auth.pop(phone)
                await old_client.disconnect()
            _cleanup_pending_auth()
            _pending_auth[phone] = (client, time.time())
            return {"type": type(result.type).__name__, "hash": result.phone_code_hash[:8]}
        except (ConnectionError, OSError) as e:
            print(f"[TG_CONNECT] Connection error (attempt {attempt+1}): {e}", flush=True)
            await client.disconnect()
            if attempt == 0:
                # Retry with fresh session
                session = StringSession()
                continue
            raise
        except Exception as e:
            print(f"[TG_CONNECT] FAILED: {type(e).__name__}: {e}", flush=True)
            await client.disconnect()
            raise
    raise ConnectionError("Failed to connect after retries")


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

    # Save StringSession to DB. Always Fernet-encrypt before storing —
    # plaintext sessions in DB == full Telegram account takeover on a
    # DB breach. encrypt_session() returns a `gAAAA...` Fernet token.
    session_str = client.session.save()
    if not session_str or len(session_str) < 10:
        raise ValueError("Failed to serialize Telegram session")
    encrypted_session = encrypt_session(session_str)
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.phone == phone))
        account = result.scalar_one_or_none()
        if account:
            account.session_file = _session_path(phone)  # keep for compat
            account.session_string = encrypted_session
            account.is_active = True
            account.display_name = display_name
        else:
            account = TgAccount(
                phone=phone,
                session_file=_session_path(phone),
                session_string=encrypted_session,
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
    """Determine media type and extension from a Telethon message.

    Check order matters! Telethon's properties overlap:
      - `.video` is True for regular video AND video_notes AND .webm stickers
      - `.sticker` is True for stickers that also have DocumentAttributeVideo
    So we must check the MOST SPECIFIC types first (sticker, video_note)
    before falling through to the generic `.video`.
    """
    if msg_obj.photo:
        return "photo", ".jpg"
    # Stickers MUST be checked before video — animated .webm stickers
    # have both DocumentAttributeSticker and DocumentAttributeVideo,
    # so msg_obj.video returns True for them too.
    if msg_obj.sticker:
        sticker_ext = ".webp"
        if msg_obj.document and msg_obj.document.mime_type:
            mime = msg_obj.document.mime_type
            if "webm" in mime:
                sticker_ext = ".webm"
            elif "tgs" in mime or "gzip" in mime:
                sticker_ext = ".tgs"
        return "sticker", sticker_ext
    if msg_obj.video_note:
        return "video_note", ".mp4"
    if msg_obj.video:
        return "video", ".mp4"
    if msg_obj.voice:
        return "voice", ".ogg"
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
    @_safe_handler
    async def on_outgoing_message(event):
        """Capture messages sent directly from Telegram (not through CRM)."""
        msg_obj = event.message
        chat = await event.get_chat()
        if not chat:
            return

        # Skip messages sent through the CRM API — they're already being
        # saved by the API handler. The previous 1.5s sleep here was a
        # race-condition workaround that capped outgoing throughput at
        # ~0.67 events/sec; the in-memory tracker is O(1) and avoids the
        # sleep entirely. DB dedup check below is kept as a safety net.
        if _is_crm_sent(account.id, msg_obj.id):
            return

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
                # Preserve original filename for documents
                orig_name = getattr(msg_obj.file, 'name', None) if msg_obj.file else None
                if orig_name and media_type == "document":
                    # Sanitize: remove path separators, keep safe chars
                    safe_name = orig_name.replace("/", "_").replace("\\", "_").replace("..", "_")
                    filename = f"{contact.id}_{msg_obj.id}_{safe_name}"
                else:
                    filename = f"{contact.id}_{msg_obj.id}{ext}"
                filepath = os.path.join(MEDIA_DIR, filename)
                actual_path = await asyncio.wait_for(msg_obj.download_media(file=filepath), timeout=60)
                if actual_path:
                    # Compress photos to save disk space
                    if media_type == "photo":
                        actual_path = await asyncio.to_thread(_compress_photo, actual_path)
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

            # Dedup check for outgoing
            existing = await db.execute(
                select(Message.id).where(
                    Message.contact_id == contact.id,
                    Message.tg_message_id == msg_obj.id,
                ).limit(1)
            )
            if existing.scalar_one_or_none():
                return  # Already saved via CRM send API

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
                grouped_id=getattr(msg_obj, "grouped_id", None),
            )
            db.add(msg)
            if not _should_skip_last_message_update(contact.id, "outgoing"):
                contact.last_message_at = func.now()
                _preview = sanitized_content or (f"[{media_type}]" if media_type else None)
                contact.last_message_content = (_preview or "")[:200] or None
                contact.last_message_direction = "outgoing"
                contact.last_message_is_read = False
            await db.commit()
            await db.refresh(msg)

            # Broadcast to CRM (scoped to this account's org). media_url is
            # an HMAC-signed /media/ path — frontend renders it directly
            # so no unauthenticated media access is required.
            from app import _build_media_signed_url
            signed_media_url = _build_media_signed_url(msg.media_path) if msg.media_path else None
            await ws_manager.broadcast_to_admins({
                "type": "new_message",
                "contact_id": str(contact.id),
                "message": {
                    "id": str(msg.id),
                    "direction": "outgoing",
                    "content": msg.content,
                    "media_type": msg.media_type,
                    "media_path": msg.media_path,
                    "media_url": signed_media_url,
                    "forwarded_from_alias": msg.forwarded_from_alias,
                    "is_deleted": False,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                },
            }, org_id=account.org_id)

    @client.on(events.NewMessage(incoming=True))
    @_safe_handler
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
            # Use FOR UPDATE to prevent race conditions with parallel messages
            result = await db.execute(
                select(Contact).where(
                    Contact.real_tg_id == peer_tg_id,
                    Contact.tg_account_id == account.id,
                ).limit(1).with_for_update(skip_locked=True)
            )
            contact = result.scalars().first()

            # Double-check without lock if locked row exists
            if not contact:
                result2 = await db.execute(
                    select(Contact).where(
                        Contact.real_tg_id == peer_tg_id,
                        Contact.tg_account_id == account.id,
                    ).limit(1)
                )
                contact = result2.scalars().first()

            is_new_contact = False
            first_name = ""
            username = None

            if not contact:
                is_new_contact = True

                if is_group:
                    group_title = getattr(chat, "title", None) or ""
                else:
                    sender = await event.get_sender()
                    fn = getattr(sender, "first_name", "") or ""
                    ln = getattr(sender, "last_name", "") or ""
                    first_name = f"{fn} {ln}".strip() if ln else fn
                    username = getattr(sender, "username", None)

                # Alias seed is random — avoids SELECT count(*) FROM contacts
                # on every new peer, which was O(N) per incoming message and
                # dominated DB CPU once the contact table grew past 100k rows.
                # Collisions fall back to the retry loop (rare at 6 digits).
                import random as _random
                for _attempt in range(5):
                    seq = _random.randint(1, 999999)

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

                # === AUTO-TAG + AUTO-GREETING on first contact ===
                if not is_group and contact:
                    try:
                        # Auto-tag: apply tags from tg_account.auto_tags
                        if account.auto_tags:
                            contact.tags = list(set((contact.tags or []) + list(account.auto_tags)))
                            await db.commit()

                        # Auto-greeting: send template on first message
                        if account.auto_greeting_template_id:
                            from models import MessageTemplate
                            tpl_result = await db.execute(
                                select(MessageTemplate).where(MessageTemplate.id == account.auto_greeting_template_id)
                            )
                            greeting_tpl = tpl_result.scalar_one_or_none()
                            if greeting_tpl:
                                # Simple text greeting (first block or content)
                                greeting_text = None
                                if greeting_tpl.blocks_json:
                                    for block in sorted(greeting_tpl.blocks_json, key=lambda b: b.get("order", 0)):
                                        if block.get("type") in ("static", "variable") and block.get("content"):
                                            greeting_text = (greeting_text or "") + block["content"]
                                elif greeting_tpl.content:
                                    greeting_text = greeting_tpl.content

                                if greeting_text:
                                    import asyncio as _aio
                                    # Small delay so the greeting doesn't arrive before the user's message is processed
                                    await _aio.sleep(1.0)
                                    try:
                                        await send_message(account.id, peer_tg_id, greeting_text)
                                        print(f"[AUTO-GREET] Sent greeting to {peer_tg_id} via template {account.auto_greeting_template_id}")
                                    except Exception as greet_err:
                                        print(f"[AUTO-GREET] Failed for {peer_tg_id}: {greet_err}")
                    except Exception as auto_err:
                        print(f"[AUTO] Error applying auto-tag/greeting for {peer_tg_id}: {auto_err}")

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
                orig_name = getattr(msg_obj.file, 'name', None) if msg_obj.file else None
                if orig_name and media_type == "document":
                    safe_name = orig_name.replace("/", "_").replace("\\", "_").replace("..", "_")
                    filename = f"{contact.id}_{msg_obj.id}_{safe_name}"
                else:
                    filename = f"{contact.id}_{msg_obj.id}{ext}"
                filepath = os.path.join(MEDIA_DIR, filename)
                actual_path = await asyncio.wait_for(msg_obj.download_media(file=filepath), timeout=60)
                if actual_path:
                    # Compress photos to save disk space
                    if media_type == "photo":
                        actual_path = await asyncio.to_thread(_compress_photo, actual_path)
                    media_path = os.path.basename(actual_path)
                else:
                    media_path = filename

            # --- DEDUP CHECK ---
            existing = await db.execute(
                select(Message.id).where(
                    Message.contact_id == contact.id,
                    Message.tg_message_id == msg_obj.id,
                ).limit(1)
            )
            if existing.scalar_one_or_none():
                return  # Already saved, skip duplicate

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
                grouped_id=getattr(msg_obj, "grouped_id", None),
            )
            db.add(msg)
            # Keep the denormalized preview fields in sync. /api/contacts
            # reads them directly — no subquery over messages on every call.
            # Debounced: same-direction burst updates within 200ms skip the
            # Contact row update to avoid pool exhaustion under TG floods.
            if not _should_skip_last_message_update(contact.id, "incoming"):
                contact.last_message_at = func.now()
                _preview = sanitized_content or (f"[{media_type}]" if media_type else None)
                contact.last_message_content = (_preview or "")[:200] or None
                contact.last_message_direction = "incoming"
                contact.last_message_is_read = False
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
                from app import _build_media_signed_url
                signed_media_url = _build_media_signed_url(msg.media_path) if msg.media_path else None
                ws_event = {
                    "type": "new_message",
                    "contact_id": str(contact.id),
                    "message": {
                        "id": str(msg.id),
                        "direction": "incoming",
                        "content": msg.content,
                        "media_type": msg.media_type,
                        "media_path": msg.media_path,
                        "media_url": signed_media_url,
                        "reply_to_msg_id": str(msg.reply_to_msg_id) if msg.reply_to_msg_id else None,
                        "reply_to_content_preview": msg.reply_to_content_preview,
                        "forwarded_from_alias": msg.forwarded_from_alias,
                        "sender_alias": msg.sender_alias,
                        "topic_id": msg.topic_id,
                        "topic_name": msg.topic_name,
                        "inline_buttons": msg.inline_buttons,
                        "is_deleted": False,
                        "created_at": msg.created_at.isoformat() if msg.created_at else None,
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
        _account_errors[account.id] = _account_errors.get(account.id, 0) + 1
        err_count = _account_errors[account.id]
        print(f"[LISTENER] Error processing incoming message (account {account.phone}, errors={err_count}): {e}")
        if err_count <= 3:
            traceback.print_exc()
        if err_count >= MAX_CONSECUTIVE_ERRORS:
            print(f"[LISTENER] Account {account.phone} disabled after {err_count} consecutive errors")
            _account_status[account.id] = "disabled"
      else:
        # Reset error counter on success
        _account_errors[account.id] = 0
        _account_status[account.id] = "connected"

    @client.on(events.MessageEdited)
    @_safe_handler
    async def on_message_edited(event):
        msg_obj = event.message
        # Resolve the contact from the peer FIRST so the message lookup
        # can use the composite (contact_id, tg_message_id) index. Without
        # the contact_id filter the query falls back to a seq scan of the
        # entire messages table — every edit across every account used
        # to churn through tens of millions of rows.
        peer_tg_id = event.chat_id
        if not peer_tg_id:
            return
        async with async_session() as db:
            contact_row = await db.execute(
                select(Contact.id).where(
                    Contact.tg_account_id == account.id,
                    Contact.real_tg_id == peer_tg_id,
                ).limit(1)
            )
            contact_pk = contact_row.scalar_one_or_none()
            if not contact_pk:
                return
            result = await db.execute(
                select(Message).where(
                    Message.contact_id == contact_pk,
                    Message.tg_message_id == msg_obj.id,
                ).limit(1)
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
    @_safe_handler
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
            # Denormalized preview: when the other party reads our last
            # outgoing message, flip the contact's is_read flag so the
            # chat list shows the double-checkmark on refetch.
            if contact.last_message_direction == "outgoing":
                contact.last_message_is_read = True
            await db.commit()

            await ws_manager.broadcast_to_admins({
                "type": "messages_read",
                "contact_id": str(contact.id),
                "message_ids": msg_ids,
                "direction": "outgoing",
            }, org_id=account.org_id)

    @client.on(events.MessageRead(inbox=True))
    @_safe_handler
    async def on_inbox_read(event):
        """Mirror read state from the native Telegram client into CRM.

        Fires when the user reads messages in their own Telegram app — the
        operator reads a chat on their phone, then opens CRM expecting no
        unread badge. Without this handler, CRM kept every message as
        is_read=False until the operator explicitly re-read in CRM.
        """
        max_id = event.max_id
        peer_id = event.chat_id
        if not max_id or not peer_id:
            return
        async with async_session() as db:
            contact = (await db.execute(
                select(Contact).where(
                    Contact.tg_account_id == account.id,
                    Contact.real_tg_id == peer_id,
                ).limit(1)
            )).scalar_one_or_none()
            if not contact:
                return

            # Mark all incoming messages up to max_id as read in one shot.
            from sqlalchemy import update as sa_update
            await db.execute(
                sa_update(Message)
                .where(
                    Message.contact_id == contact.id,
                    Message.direction == "incoming",
                    Message.tg_message_id <= max_id,
                    Message.is_read.is_(False),
                )
                .values(is_read=True)
            )
            # Flip denormalized preview flag if the latest preview is
            # incoming — clears the chat-list unread badge immediately.
            if contact.last_message_direction == "incoming":
                contact.last_message_is_read = True
            await db.commit()

            # Broadcast so open CRM tabs clear their local unread counters.
            await ws_manager.broadcast_to_admins({
                "type": "messages_read",
                "contact_id": str(contact.id),
                "direction": "incoming",
                "max_tg_id": max_id,
            }, org_id=account.org_id)

    @client.on(events.MessageDeleted)
    @_safe_handler
    async def on_message_deleted(event):
        """Mirror message deletions from Telegram into CRM.

        Previously only handled incoming deletions (direction="incoming"),
        so deleting your OWN message in native TG didn't sync. Now handles
        both directions. Also resolves contact_id when chat_id is available
        (channels/groups) to use the composite index and avoid a seq scan.
        """
        deleted_ids = event.deleted_ids
        if not deleted_ids:
            return
        peer_id = getattr(event, "chat_id", None)
        async with async_session() as db:
            # Resolve contact if peer is known (channels/groups).
            # For private chats Telethon may not provide chat_id.
            contact_pk = None
            if peer_id:
                cr = await db.execute(
                    select(Contact.id).where(
                        Contact.tg_account_id == account.id,
                        Contact.real_tg_id == peer_id,
                    ).limit(1)
                )
                contact_pk = cr.scalar_one_or_none()

            for tg_msg_id in deleted_ids:
                q = select(Message).where(Message.tg_message_id == tg_msg_id)
                if contact_pk:
                    q = q.where(Message.contact_id == contact_pk)
                q = q.order_by(Message.created_at.desc()).limit(1)
                result = await db.execute(q)
                msg = result.scalar_one_or_none()
                if msg and not msg.is_deleted:
                    msg.is_deleted = True
                    await db.commit()
                    await ws_manager.broadcast_to_admins({
                        "type": "message_deleted",
                        "contact_id": str(msg.contact_id),
                        "message_id": str(msg.id),
                    }, org_id=account.org_id)

    # Run client in background with auto-reconnect and exponential backoff
    _account_status[account.id] = "connected"

    async def _run_with_reconnect():
        reconnect_delay = 5
        while True:
            try:
                _account_status[account.id] = "connected"
                reconnect_delay = 5  # Reset on successful connection
                await client.run_until_disconnected()
            except Exception as e:
                print(f"[TELETHON] Client for {account.phone} disconnected: {e}")
            # Check if we were intentionally removed
            if account.id not in _clients:
                _account_status[account.id] = "disabled"
                break
            _account_status[account.id] = "reconnecting"
            print(f"[TELETHON] Reconnecting {account.phone} in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            try:
                await client.connect()
                if await client.is_user_authorized():
                    print(f"[TELETHON] Reconnected {account.phone}")
                    _account_status[account.id] = "connected"
                    reconnect_delay = 5
                else:
                    print(f"[TELETHON] {account.phone} no longer authorized, stopping")
                    _account_status[account.id] = "failed"
                    break
            except Exception as e:
                print(f"[TELETHON] Reconnect failed for {account.phone}: {e}")
                reconnect_delay = min(reconnect_delay * 2, 300)  # Exponential backoff, max 5 min
                _account_status[account.id] = "reconnecting"

    asyncio.create_task(_run_with_reconnect())


async def _try_reconnect(account_id: UUID) -> TelegramClient | None:
    """Try to reconnect a disconnected account and return the client."""
    # Prevent duplicate reconnect attempts
    if account_id not in _reconnect_locks:
        _reconnect_locks[account_id] = asyncio.Lock()
    if _reconnect_locks[account_id].locked():
        return _clients.get(account_id)
    await _reconnect_locks[account_id].acquire()
    try:
      return await _try_reconnect_inner(account_id)
    finally:
      _reconnect_locks[account_id].release()


async def _try_reconnect_inner(account_id: UUID) -> TelegramClient | None:
    async with async_session() as db:
        result = await db.execute(
            select(TgAccount).where(TgAccount.id == account_id, TgAccount.is_active.is_(True))
        )
        account = result.scalar_one_or_none()
    if not account:
        return None
    try:
        session = StringSession(decrypt_session(account.session_string)) if account.session_string else account.session_file
        client = TelegramClient(
            session,
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


async def set_chat_mute(account_id: UUID, tg_id: int, muted: bool) -> None:
    """Toggle Telegram's native per-peer mute for a chat via Telethon.

    Sends UpdateNotifySettingsRequest with `mute_until` set to the far
    future (mute forever) or 0 (unmute). This affects the real Telegram
    state — on the next _do_sync_dialogs run, the Dialog's notify_settings
    will reflect the change and Contact.is_muted will stay consistent.

    Raises ValueError if the account isn't connected or the peer can't
    be resolved.
    """
    from telethon.tl.functions.account import UpdateNotifySettingsRequest
    from telethon.tl.types import InputNotifyPeer, InputPeerNotifySettings

    client = _clients.get(account_id)
    if not client:
        client = await _try_reconnect(account_id)
    if not client:
        raise ValueError("Telegram-аккаунт не подключён. Проверьте подключение в настройках.")

    peer = await client.get_input_entity(tg_id)
    # "Mute forever" = int32 max (2^31 - 1). Unmute = 0 + explicit silent/show
    # overrides so the peer doesn't inherit a "mute new chats by default"
    # global preference and stay effectively muted.
    if muted:
        settings = InputPeerNotifySettings(mute_until=2 ** 31 - 1, silent=True)
    else:
        settings = InputPeerNotifySettings(mute_until=0, silent=False, show_previews=True)
    try:
        await client(UpdateNotifySettingsRequest(
            peer=InputNotifyPeer(peer=peer),
            settings=settings,
        ))
    except FloodWaitError as e:
        wait = min(e.seconds, 60)
        print(f"[FLOOD] set_chat_mute rate limited, waiting {wait}s", flush=True)
        await asyncio.sleep(wait)
        await client(UpdateNotifySettingsRequest(
            peer=InputNotifyPeer(peer=peer),
            settings=settings,
        ))


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

    async def _do_send():
        if file_path:
            if media_type == "voice":
                kwargs["voice_note"] = True
                return await client.send_file(tg_id, file_path, caption=text or "", **kwargs)
            elif media_type == "video_note":
                kwargs["video_note"] = True
                return await client.send_file(tg_id, file_path, **kwargs)
            else:
                return await client.send_file(tg_id, file_path, caption=text or "", **kwargs)
        else:
            return await client.send_message(tg_id, text, **kwargs)

    try:
        result = await _do_send()
    except FloodWaitError as e:
        wait = min(e.seconds, 60)  # cap wait at 60s
        print(f"[FLOOD] Rate limited, waiting {wait}s before retry", flush=True)
        await asyncio.sleep(wait)
        result = await _do_send()
    except ValueError as e:
        # "Could not find the input entity for PeerUser(user_id=...)"
        # Telethon's entity cache doesn't have the access_hash for this user.
        # One-shot fix: warm up the cache via iter_dialogs and retry once.
        if "Could not find the input entity" in str(e):
            print(f"[SEND] Entity cache miss for tg_id={tg_id}, warming up via iter_dialogs...", flush=True)
            try:
                _warmed = 0
                async for _ in client.iter_dialogs(limit=None):
                    _warmed += 1
                print(f"[SEND] Warmed {_warmed} dialogs, retrying send...", flush=True)
                result = await _do_send()
            except Exception as retry_err:
                raise ValueError(
                    f"Контакт недоступен (Telegram не может найти получателя). "
                    f"Возможно, он удалил свой аккаунт или заблокировал бота."
                ) from retry_err
        else:
            raise
    except Exception as e:
        # Catch Telethon-specific errors and convert to user-friendly messages.
        err_name = type(e).__name__
        err_str = str(e)
        if err_name == "InputUserDeactivatedError" or "InputUserDeactivatedError" in err_str:
            raise ValueError(
                "Аккаунт получателя удалён или деактивирован в Telegram. "
                "Сообщение не может быть доставлено."
            )
        if err_name == "PeerFloodError" or "PEER_FLOOD" in err_str:
            raise ValueError(
                "Telegram временно ограничил отправку сообщений с этого аккаунта. "
                "Подождите несколько минут и попробуйте снова."
            )
        if err_name == "UserBannedInChannelError":
            raise ValueError("Этот аккаунт заблокирован в канале/группе получателя.")
        # Unknown Telethon error — re-raise as-is so it surfaces in Sentry
        raise
    # Mark this tg_msg_id as CRM-originated so the outgoing listener
    # skips it without sleeping.
    _mark_crm_sent(account_id, result.id)
    return result.id


async def send_media_group(
    account_id: UUID,
    tg_id: int,
    file_paths: list[str],
    caption: str | None = None,
) -> list[int]:
    """Send multiple photos/videos as a media group (album). Returns list of tg_message_ids."""
    client = _clients.get(account_id)
    if not client:
        client = await _try_reconnect(account_id)
    if not client:
        raise ValueError("Telegram-аккаунт не подключён.")
    # Telethon: send_file with list of files creates a media group
    results = await client.send_file(
        tg_id,
        file_paths,
        caption=caption or "",
    )
    if not isinstance(results, list):
        results = [results]
    ids = [getattr(r, 'id', None) or (r.get('id') if isinstance(r, dict) else 0) for r in results]
    for _id in ids:
        if _id:
            _mark_crm_sent(account_id, _id)
    return ids


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
            _mark_crm_sent(account_id, result.id)
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


async def get_drafts(account_id: UUID) -> list[dict]:
    """Get all drafts for a TG account. Returns list of {peer_id, text, date}."""
    client = _clients.get(account_id)
    if not client:
        return []
    try:
        result = await client(GetAllDraftsRequest())
        drafts = []
        for update in result.updates:
            if hasattr(update, 'draft') and hasattr(update.draft, 'message') and update.draft.message:
                peer_id = None
                if hasattr(update, 'peer'):
                    p = update.peer
                    if hasattr(p, 'user_id'):
                        peer_id = p.user_id
                    elif hasattr(p, 'chat_id'):
                        peer_id = -p.chat_id
                    elif hasattr(p, 'channel_id'):
                        peer_id = int(f"-100{p.channel_id}")
                if peer_id:
                    drafts.append({
                        "peer_id": peer_id,
                        "text": update.draft.message,
                        "date": update.draft.date.isoformat() if update.draft.date else None,
                    })
        return drafts
    except Exception as e:
        print(f"[DRAFTS] Error fetching drafts for {account_id}: {e}")
        return []


async def download_missing_media(account_id: UUID, chat_tg_id: int, tg_msg_id: int, contact_id: UUID) -> str | None:
    """Download media for a specific message from Telegram. Returns media_path or None."""
    client = _clients.get(account_id)
    if not client:
        client = await _try_reconnect(account_id)
    if not client:
        return None
    try:
        msg = await asyncio.wait_for(client.get_messages(chat_tg_id, ids=tg_msg_id), timeout=15)
        if not msg or not msg.media:
            return None
        media_type, ext = _extract_media(msg)
        if not media_type:
            return None
        orig_name = getattr(msg.file, 'name', None) if msg.file else None
        if orig_name and media_type == "document":
            safe_name = orig_name.replace("/", "_").replace("\\", "_").replace("..", "_")
            filename = f"{contact_id}_{tg_msg_id}_{safe_name}"
        else:
            filename = f"{contact_id}_{tg_msg_id}{ext or ''}"
        filepath = os.path.join(MEDIA_DIR, filename)
        actual_path = await asyncio.wait_for(msg.download_media(file=filepath), timeout=60)
        if actual_path:
            if media_type == "photo":
                actual_path = await asyncio.to_thread(_compress_photo, actual_path)
            return os.path.basename(actual_path)
        return filename
    except Exception as e:
        print(f"[DOWNLOAD] Failed to download media msg={tg_msg_id}: {e}")
        return None


async def startup_listeners() -> None:
    """On app startup, reconnect all active TG accounts."""
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.is_active.is_(True)))
        accounts = result.scalars().all()

    print(f"[STARTUP] Found {len(accounts)} active TG accounts")
    for account in accounts:
        try:
            # Prefer StringSession from DB (no SQLite lock), fallback to file.
            # decrypt_session() handles both Fernet-encrypted and legacy
            # plaintext rows transparently. Plaintext rows get re-encrypted
            # below once we confirm the session works.
            if account.session_string:
                session = StringSession(decrypt_session(account.session_string))
                print(f"[STARTUP] Connecting {account.phone} (StringSession from DB)")
            else:
                session = account.session_file
                print(f"[STARTUP] Connecting {account.phone} (SQLite file: {account.session_file})")
            client = TelegramClient(
                session,
                settings.TG_API_ID,
                settings.TG_API_HASH,
            )
            await client.connect()
            authorized = await client.is_user_authorized()
            print(f"[STARTUP] {account.phone} authorized={authorized}")
            if authorized:
                # Migration path A: SQLite-only account → export to encrypted StringSession.
                # Migration path B: existing plaintext StringSession → re-encrypt in place.
                needs_migration = (not account.session_string) or (
                    not is_session_encrypted(account.session_string)
                )
                if needs_migration:
                    try:
                        # Create a StringSession from the connected client's auth data
                        ss = StringSession()
                        ss._dc_id = client.session.dc_id
                        ss._port = client.session.port
                        ss._server_address = client.session.server_address
                        ss._auth_key = client.session.auth_key
                        ss_str = ss.save()
                        if ss_str and len(ss_str) > 10:
                            encrypted = encrypt_session(ss_str)
                            async with async_session() as db_mig:
                                res_mig = await db_mig.execute(select(TgAccount).where(TgAccount.id == account.id))
                                acc_mig = res_mig.scalar_one_or_none()
                                if acc_mig:
                                    acc_mig.session_string = encrypted
                                    await db_mig.commit()
                                    print(f"[STARTUP] Encrypted/migrated session for {account.phone} ({len(ss_str)} chars)")
                    except Exception as e_mig:
                        print(f"[STARTUP] Could not migrate session for {account.phone}: {e_mig}")

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

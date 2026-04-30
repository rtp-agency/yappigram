"""YappiGram — Main FastAPI application with all routes."""

import asyncio
import hashlib
import hmac
import os
import re
import secrets
import time
import uuid as uuid_mod
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel as PydanticBaseModel
from typing import Annotated
from uuid import UUID

# Allowed file extensions for uploads (security whitelist)
ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp",  # images (no .svg — XSS risk)
    ".mp4", ".mov", ".avi",  # video
    ".ogg", ".mp3", ".wav",  # audio
    ".pdf", ".doc", ".docx",  # docs
}

# Dangerous extensions that must never be uploaded
BLOCKED_EXTENSIONS = {
    ".html", ".htm", ".svg", ".js", ".php", ".py", ".sh",
    ".bat", ".exe", ".cmd", ".ps1", ".msi", ".scr", ".com",
}

MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB


def _validate_block_id(block_id: str) -> str:
    """Validate block_id to prevent path traversal."""
    if not re.match(r"^[a-zA-Z0-9_-]+$", block_id):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid block_id")
    return block_id


def _safe_media_path(raw: str | None) -> str | None:
    """Resolve a user-supplied media path against MEDIA_DIR and verify the
    result is still inside that directory. Used to protect template/block
    media-path fields from being set to `../../etc/passwd` and read via
    Telethon's file= parameter.

    Returns the validated absolute path, or None if `raw` is falsy.
    Raises HTTP 400 on escape, HTTP 400 on missing file.
    """
    if not raw:
        return None
    media_root = os.path.realpath(MEDIA_DIR)
    candidate = os.path.realpath(os.path.join(media_root, raw))
    if not (candidate == media_root or candidate.startswith(media_root + os.sep)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid media path")
    if not os.path.isfile(candidate):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Media file not found")
    return candidate


# Magic byte signatures used by _validate_upload() to catch MIME spoofing —
# a file renamed to foo.jpg with Content-Type image/jpeg can still be HTML/JS.
# Extensions map to the allowed leading byte sequences. Checked only for
# formats where a wrong payload is actually dangerous (images, video, audio).
_MAGIC_SIGNATURES: dict[str, tuple[bytes, ...]] = {
    ".jpg":  (b"\xff\xd8\xff",),
    ".jpeg": (b"\xff\xd8\xff",),
    ".png":  (b"\x89PNG\r\n\x1a\n",),
    ".gif":  (b"GIF87a", b"GIF89a"),
    ".webp": (b"RIFF",),   # followed by "WEBP" at offset 8; checked below
    ".bmp":  (b"BM",),
    ".mp4":  (b"\x00\x00\x00",),  # ftyp box — sniffed further below
    ".mov":  (b"\x00\x00\x00",),
    ".webm": (b"\x1aE\xdf\xa3",),
    ".ogg":  (b"OggS",),
    ".mp3":  (b"ID3", b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"),
    ".wav":  (b"RIFF",),
    ".pdf":  (b"%PDF-",),
    ".zip":  (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"),
}


async def _validate_upload(file: "UploadFile") -> None:
    """Validate uploaded file: extension + content type + magic bytes.

    Extension/content-type checks are NOT sufficient on their own — a client
    can set any Content-Type header and rename any file. The magic-byte
    check reads the first 16 bytes and verifies they match the claimed
    extension, stopping MIME spoofing (HTML-as-JPG, JS-as-PNG, etc).
    """
    safe_name = os.path.basename(file.filename or "")
    ext = os.path.splitext(safe_name)[1].lower()
    if ext in BLOCKED_EXTENSIONS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"File type '{ext}' not allowed")
    if ext and ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"File type '{ext}' not allowed")
    ct = (file.content_type or "").lower()
    blocked_ct = {
        "application/x-executable", "application/x-msdos-program",
        "application/x-sh", "application/x-shellscript", "application/x-bat",
        "text/html", "application/javascript", "image/svg+xml",
    }
    if ct in blocked_ct:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "File type not allowed")

    sigs = _MAGIC_SIGNATURES.get(ext)
    if not sigs:
        return  # Extension has no configured magic signature — skip
    head = await file.read(16)
    try:
        await file.seek(0)
    except Exception:
        pass
    if not head:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Empty file")
    if not any(head.startswith(sig) for sig in sigs):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"File content does not match its extension ({ext})",
        )
    # WebP and WAV both use the RIFF container — verify the sub-type tag
    # at offset 8 to prevent a WAV being uploaded as .webp and vice versa.
    if ext == ".webp" and head[8:12] != b"WEBP":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Not a valid WebP file")
    if ext == ".wav" and head[8:12] != b"WAVE":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Not a valid WAV file")

from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import delete as sa_delete, func, select, text as sa_text, update as sa_update
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
from models import (
    AuditLog, Base, BotInvite, Broadcast, BroadcastRecipient, Contact,
    Message, MessageEditHistory, MessageTemplate, PinnedChat, ScheduledMessage, Staff, StaffTgAccount, Tag, TgAccount, async_session, engine,
)
from schemas import (
    BotInviteCreate,
    BotInviteOut,
    BroadcastCreate,
    BroadcastOut,
    BroadcastRecipientOut,
    ContactOut,
    ContactReveal,
    ContactUpdate,
    CreateGroupRequest,
    ForwardMessage,
    MessageEditHistoryOut,
    MessageOut,
    PressButton,
    RefreshRequest,
    SendMessage,
    SsoAuthRequest,
    TgAuthRequest,
    TgAuthResponse,
    TgWorkspaceItem,
    TgWorkspaceSelect,
    StaffOut,
    StaffUpdate,
    TagCreate,
    TagOut,
    TemplateCreate,
    TemplateOut,
    TemplateUpdate,
    TgAccountOut,
    TgConnectRequest,
    TgVerifyRequest,
    TokenResponse,
    TranslateRequest,
)
from telegram import (
    create_group,
    disconnect_account,
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


# ─── PostForge CRM billing helpers ────────────────────────────────
# Called from connect/verify/disconnect endpoints to sync billing
# state with PostForge's coin balance system.

async def _postforge_crm_billing_check(user) -> dict | None:
    """Check if user can afford to connect a new CRM TG account."""
    if not settings.POSTFORGE_API_URL or not settings.POSTFORGE_BOT_SECRET:
        return None  # Billing not configured — allow connect
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.POSTFORGE_API_URL}/api/internal/crm-billing/check",
                json={"postforge_user_id": user.postforge_user_id},
                headers={"Authorization": f"Bot {settings.POSTFORGE_BOT_SECRET}"},
            )
        if resp.status_code == 200:
            return resp.json()
        return None  # Non-200 → allow connect (don't block on billing issues)
    except Exception:
        return None


async def _postforge_crm_billing_charge(user, crm_account_id: str, phone_number: str) -> dict | None:
    """Charge the user for first month of a CRM TG account."""
    if not settings.POSTFORGE_API_URL or not settings.POSTFORGE_BOT_SECRET:
        return None
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.POSTFORGE_API_URL}/api/internal/crm-billing/charge",
                json={
                    "postforge_user_id": user.postforge_user_id,
                    "crm_account_id": crm_account_id,
                    "phone_number": phone_number,
                },
                headers={"Authorization": f"Bot {settings.POSTFORGE_BOT_SECRET}"},
            )
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


async def _postforge_crm_billing_disconnect(crm_account_id: str) -> None:
    """Notify PostForge that this CRM account is disconnected."""
    if not settings.POSTFORGE_API_URL or not settings.POSTFORGE_BOT_SECRET:
        return
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{settings.POSTFORGE_API_URL}/api/internal/crm-billing/disconnect",
                json={"crm_account_id": crm_account_id},
                headers={"Authorization": f"Bot {settings.POSTFORGE_BOT_SECRET}"},
            )
    except Exception:
        pass


async def _postforge_funnel_stage_update(
    campaign_id: str, telegram_user_id: int, stage: str
) -> None:
    """Push a funnel-stage change to PostForge after operator tags a Contact.

    Best-effort, fire-and-forget. Any failure (network, 4xx, 5xx) is logged
    and swallowed so that yappigram's PATCH /api/contacts/{id} never fails
    because PostForge is unreachable. The tag still saves locally in CRM.
    """
    if not settings.POSTFORGE_API_URL or not settings.POSTFORGE_BOT_SECRET:
        return
    import httpx
    # Use the internal/service-bot endpoint, not the user-facing one — the
    # internal variant accepts `Authorization: Bot <BACKEND_BOT_SECRET>` and
    # skips the JWT/permission machinery that the UI version requires.
    url = (
        f"{settings.POSTFORGE_API_URL.rstrip('/')}"
        f"/api/internal/traffic/campaigns/{campaign_id}/subscribers/{telegram_user_id}/funnel-stage"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                json={"stage": stage},
                headers={"Authorization": f"Bot {settings.POSTFORGE_BOT_SECRET}"},
            )
        if resp.status_code >= 400:
            print(
                f"[POSTFORGE] funnel-stage push non-200: status={resp.status_code} "
                f"campaign={campaign_id} tg_user={telegram_user_id} stage={stage} "
                f"body={resp.text[:300]}",
                flush=True,
            )
        else:
            print(
                f"[POSTFORGE] funnel-stage pushed: campaign={campaign_id} "
                f"tg_user={telegram_user_id} stage={stage}",
                flush=True,
            )
    except Exception as e:
        print(
            f"[POSTFORGE] funnel-stage push failed: {e} "
            f"campaign={campaign_id} tg_user={telegram_user_id} stage={stage}",
            flush=True,
        )


# Parse the tag→stage mapping once at import time. If the env var is malformed,
# we log and fall back to an empty dict (webhook becomes a no-op rather than
# crashing the whole module).
def _load_tag_to_stage_map() -> dict[str, str]:
    """Returns a dict whose KEYS are normalized to lowercase + stripped.

    Operators in CRM write tags as free-form strings — `qualified`,
    `Qualified`, `QUALIFIED`, ` qualified ` are all the same intent. We
    lowercase both the map keys here and the tag at lookup time so the
    webhook fires regardless of how the operator capitalised the tag.
    """
    import json as _json
    raw = settings.POSTFORGE_TAG_TO_FUNNEL_STAGE or "{}"
    try:
        parsed = _json.loads(raw)
        if not isinstance(parsed, dict):
            print(f"[POSTFORGE] POSTFORGE_TAG_TO_FUNNEL_STAGE is not an object: {raw!r}", flush=True)
            return {}
        return {str(k).strip().lower(): str(v) for k, v in parsed.items()}
    except Exception as e:
        print(f"[POSTFORGE] failed to parse POSTFORGE_TAG_TO_FUNNEL_STAGE ({e!r}): {raw!r}", flush=True)
        return {}


_TAG_TO_STAGE: dict[str, str] = _load_tag_to_stage_map()


async def _postforge_crm_billing_accounts(user) -> list:
    """Get billing info for all user's CRM accounts from PostForge."""
    if not settings.POSTFORGE_API_URL or not settings.POSTFORGE_BOT_SECRET:
        return []
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{settings.POSTFORGE_API_URL}/api/internal/crm-billing/accounts",
                params={"postforge_user_id": user.postforge_user_id},
                headers={"Authorization": f"Bot {settings.POSTFORGE_BOT_SECRET}"},
            )
        if resp.status_code == 200:
            return resp.json().get("accounts", [])
        return []
    except Exception:
        return []


# --- Rate limiting for auth endpoints ---
_rate_limits: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 30  # per real IP per minute

# --- Telethon flood protection ---
# Telegram MTProto limits (empirical, not officially documented):
#   - ~1 message per second to the same peer
#   - ~20 messages per second across all peers per account
#   - sustained spam → FLOOD_WAIT X seconds → temporary ban
# We stay well below these to avoid flood_wait errors and TG account bans.
#
# Strategy: sliding window per (tg_account_id, peer_id) AND per tg_account_id.
#   - Per-peer: max 4 messages per 10 seconds (2x margin below TG's 1/sec)
#   - Per-account: max 60 messages per 60 seconds (3x margin below 20/sec burst)
_tg_send_limits_peer: dict[tuple, list[float]] = defaultdict(list)
_tg_send_limits_account: dict[str, list[float]] = defaultdict(list)
TG_PEER_WINDOW = 10  # seconds
TG_PEER_MAX = 4      # messages per peer per window
TG_ACCT_WINDOW = 60  # seconds
TG_ACCT_MAX = 60     # messages per account per window


def check_tg_send_limit(tg_account_id: str, peer_id: int) -> None:
    """
    Throttle outgoing messages to prevent Telegram flood bans.

    Raises 429 Too Many Requests if the caller is sending faster than
    our safe-zone limits. Users should see this as "Подождите пару
    секунд" in the UI rather than as a TG account ban.

    For automated broadcast workers, use `await wait_tg_send_slot()`
    instead — it sleeps until a slot frees up instead of raising.
    """
    now = time.time()
    acct_key = str(tg_account_id)
    peer_key = (str(tg_account_id), peer_id)

    # Per-peer window
    peer_hits = _tg_send_limits_peer[peer_key]
    peer_hits[:] = [t for t in peer_hits if now - t < TG_PEER_WINDOW]
    if len(peer_hits) >= TG_PEER_MAX:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"Слишком много сообщений этому контакту. Подождите {TG_PEER_WINDOW} сек.",
        )

    # Per-account window
    acct_hits = _tg_send_limits_account[acct_key]
    acct_hits[:] = [t for t in acct_hits if now - t < TG_ACCT_WINDOW]
    if len(acct_hits) >= TG_ACCT_MAX:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"Аккаунт отправил слишком много сообщений. Подождите {TG_ACCT_WINDOW} сек чтобы избежать бана Telegram.",
        )

    peer_hits.append(now)
    acct_hits.append(now)


async def wait_tg_send_slot(tg_account_id: str, peer_id: int) -> None:
    """
    Async version of check_tg_send_limit for broadcast workers.

    Instead of raising 429, this sleeps until a slot frees up. Used by
    the broadcast executor so a long recipient list naturally throttles
    itself below TG's flood thresholds without failing partway through.
    """
    import asyncio as _asyncio

    while True:
        now = time.time()
        acct_key = str(tg_account_id)
        peer_key = (str(tg_account_id), peer_id)

        peer_hits = _tg_send_limits_peer[peer_key]
        peer_hits[:] = [t for t in peer_hits if now - t < TG_PEER_WINDOW]
        acct_hits = _tg_send_limits_account[acct_key]
        acct_hits[:] = [t for t in acct_hits if now - t < TG_ACCT_WINDOW]

        # Calculate how long to sleep before the oldest hit expires
        peer_wait = 0.0
        if len(peer_hits) >= TG_PEER_MAX:
            peer_wait = TG_PEER_WINDOW - (now - peer_hits[0]) + 0.1

        acct_wait = 0.0
        if len(acct_hits) >= TG_ACCT_MAX:
            acct_wait = TG_ACCT_WINDOW - (now - acct_hits[0]) + 0.1

        wait = max(peer_wait, acct_wait)
        if wait <= 0:
            peer_hits.append(now)
            acct_hits.append(now)
            return

        await _asyncio.sleep(wait)


def _get_real_ip(request) -> str:
    """Get real client IP from proxy headers."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip", "")
    if real_ip:
        return real_ip
    return request.client.host if request.client else "unknown"


def check_rate_limit(request):
    ip = _get_real_ip(request)
    now = time.time()
    _rate_limits[ip] = [t for t in _rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limits[ip]) >= RATE_LIMIT_MAX:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many requests")
    _rate_limits[ip].append(now)


# ─── Sentry error monitoring ──────────────────────────────────────
# Initialized BEFORE FastAPI() so the SDK's ASGI middleware picks up
# every request. Only fires if SENTRY_DSN is set in env — safe to leave
# the code in dev where there's no DSN.
_sentry_dsn = os.environ.get("SENTRY_DSN", "").strip()
if _sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

        sentry_sdk.init(
            dsn=_sentry_dsn,
            environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
            # Capture 10% of transactions for performance monitoring.
            # Full tracing on a Telegram CRM with hot WS traffic is
            # expensive and Sentry free tier has low quotas.
            traces_sample_rate=0.1,
            # Capture 100% of errors, obviously.
            sample_rate=1.0,
            # Send PII (user email, IP) so we can tell who hit the error.
            # CRM is internal, not public-facing — this is fine.
            send_default_pii=True,
            integrations=[
                StarletteIntegration(transaction_style="endpoint"),
                FastApiIntegration(transaction_style="endpoint"),
                SqlalchemyIntegration(),
            ],
            # Drop noisy integrations that flood Sentry with non-issues.
            # Telethon disconnects are normal operation, not errors.
            ignore_errors=[
                "telethon.errors.rpcerrorlist.FloodWaitError",
            ],
        )
        print(f"[SENTRY] Initialized (env={os.environ.get('SENTRY_ENVIRONMENT', 'production')})", flush=True)
    except Exception as _sentry_err:
        print(f"[SENTRY] Init failed: {_sentry_err}", flush=True)


app = FastAPI(title="YappiGram", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Cache-Control headers are set in nginx (nginx.conf and nginx.prod.conf)
# on ALL /api/ responses: `Cache-Control: no-store, no-cache, must-revalidate, private`.
# We intentionally do NOT add a Python middleware for this because
# BaseHTTPMiddleware buffers FileResponse (avatars) into memory.
# Nginx is always in front in production, so headers are enforced there.

# Redis cache for hot paths
_redis_cache = None

async def _get_redis():
    global _redis_cache
    if _redis_cache is None:
        try:
            import redis.asyncio as aioredis
            _redis_cache = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        except Exception:
            pass
    return _redis_cache

async def cache_get(key: str) -> str | None:
    r = await _get_redis()
    if r:
        try:
            return await r.get(key)
        except Exception:
            pass
    return None

async def cache_set(key: str, value: str, ttl: int = 30):
    r = await _get_redis()
    if r:
        try:
            await r.set(key, value, ex=ttl)
        except Exception:
            pass

async def cache_invalidate(pattern: str):
    r = await _get_redis()
    if r:
        try:
            keys = []
            async for key in r.scan_iter(match=pattern):
                keys.append(key)
            if keys:
                await r.delete(*keys)
        except Exception:
            pass


# Media files served via custom endpoint with security headers + HMAC auth
import mimetypes


def _attach_media_url(msg) -> None:
    """Populate `media_url` on an ORM Message with a signed URL, in place.
    Called on every Message returned to clients so the frontend can render
    `<img src={m.media_url}>` without hitting the unauthenticated path.
    No-op for messages without media.
    """
    if getattr(msg, "media_path", None):
        msg.media_url = _build_media_signed_url(msg.media_path)


def _touch_contact_preview(contact, content: str | None, media_type: str | None, direction: str) -> None:
    """Mirror the latest-message fields onto the Contact row so
    /api/contacts can read them without a per-contact subquery over
    `messages`. Called from every send path so the denormalization
    stays in sync with reality. `direction` is "incoming" | "outgoing".
    """
    preview = content or (f"[{media_type}]" if media_type else None)
    contact.last_message_content = (preview or "")[:200] or None
    contact.last_message_direction = direction
    contact.last_message_is_read = False


def _build_media_signed_url(media_path: str, ttl_seconds: int = 86400) -> str:
    """Sign a media URL for a TTL. The HMAC payload binds the media path
    and the expiry so the signature can't be reused for other files.

    Frontend gets the signed URL inline on MessageOut.media_url and uses
    it directly — no JWT in the URL, no need to fetch-first.
    """
    expires = int(time.time()) + ttl_seconds
    payload = f"{media_path}:{expires}"
    sig = hmac.new(settings.JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    from urllib.parse import quote as _url_quote
    return f"/media/{_url_quote(media_path)}?expires={expires}&sig={sig}"


@app.get("/media/{file_path:path}")
async def serve_media(
    file_path: str,
    request: Request,
    # NOTE: the `DB = Annotated[...]` alias is declared further down in this
    # file — can't use it here without a forward-reference hack, so we spell
    # the dependency out explicitly. Same signature contract, no behavior
    # difference.
    db: Annotated[AsyncSession, Depends(get_db)],
    expires: int = Query(0),
    sig: str = Query(""),
    token: str = Query(""),  # Legacy: JWT in query, still accepted during rollout
):
    """Serve media files with security headers + HMAC auth.

    Previously this endpoint was completely unauthenticated — anyone who
    learned a media filename (via WS leak, log, etc) could download it.
    Now requires either a valid HMAC signature (preferred, issued by the
    backend inline on MessageOut.media_url) or a JWT token (legacy fallback
    so old cached frontend assets don't break mid-rollout).
    """
    from fastapi.responses import FileResponse
    from urllib.parse import unquote
    # URL-decode path (handles cyrillic and special chars)
    file_path = unquote(file_path)

    # --- AUTH --- --------------------------------------------------------
    authed = False
    if sig and expires:
        if time.time() <= expires:
            payload = f"{file_path}:{expires}"
            expected = hmac.new(
                settings.JWT_SECRET.encode(), payload.encode(), hashlib.sha256,
            ).hexdigest()[:32]
            if hmac.compare_digest(sig, expected):
                authed = True
    if not authed and token:
        # Legacy JWT fallback — keep until all clients ship the new frontend
        try:
            payload_jwt = decode_token(token)
            if payload_jwt.get("type") == "access":
                staff_id = payload_jwt.get("sub")
                staff_result = await db.execute(
                    select(Staff.id).where(Staff.id == staff_id, Staff.is_active.is_(True))
                )
                if staff_result.scalar_one_or_none():
                    authed = True
        except Exception:
            pass
    if not authed:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Media access requires a signed URL")
    # Prevent path traversal. The bare startswith() on os.path.abspath
    # would let `/app/media-evil/foo` slip past the check because
    # `/app/media-evil/foo`.startswith(`/app/media`) is True. Use realpath
    # (resolves symlinks) and require an os.sep boundary, or exact match.
    media_root = os.path.realpath(MEDIA_DIR)
    safe_path = os.path.realpath(os.path.join(media_root, file_path))
    if not (safe_path == media_root or safe_path.startswith(media_root + os.sep)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid path")
    if not os.path.isfile(safe_path):
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    ext = os.path.splitext(safe_path)[1].lower()
    # Never serve HTML or SVG with native content-type
    if ext in (".html", ".htm", ".svg"):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "File type not allowed")

    content_type = mimetypes.guess_type(safe_path)[0]
    # If no extension, try to detect from file header (magic bytes)
    if not content_type or content_type == "application/octet-stream":
        try:
            def _read_header():
                with open(safe_path, "rb") as f:
                    return f.read(16)
            header = await asyncio.to_thread(_read_header)
            if header[:8] == b'\x89PNG\r\n\x1a\n':
                content_type = "image/png"
            elif header[:3] == b'\xff\xd8\xff':
                content_type = "image/jpeg"
            elif header[:4] == b'GIF8':
                content_type = "image/gif"
            elif header[:4] == b'RIFF' and header[8:12] == b'WEBP':
                content_type = "image/webp"
            elif header[:4] == b'\x1aE\xdf\xa3':
                content_type = "video/webm"
            elif header[:3] == b'OGG' or header[:4] == b'OggS':
                content_type = "audio/ogg"
            elif header[:4] == b'%PDF':
                content_type = "application/pdf"
            elif header[:2] == b'PK':
                # ZIP-based (docx, xlsx, zip)
                fname = os.path.basename(safe_path).lower()
                if 'doc' in fname:
                    content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                else:
                    content_type = "application/zip"
            elif header[:4] in (b'\x00\x00\x00\x18', b'\x00\x00\x00\x1c', b'\x00\x00\x00 '):
                content_type = "video/mp4"
            else:
                content_type = "application/octet-stream"
        except Exception:
            content_type = "application/octet-stream"
    # Override dangerous content types
    if content_type in ("text/html", "image/svg+xml", "application/javascript"):
        content_type = "application/octet-stream"

    # Images and video served inline, documents as attachment
    is_inline = content_type.startswith("image/") or content_type.startswith("video/") or content_type.startswith("audio/")
    disposition = "inline" if is_inline else "attachment"
    raw_filename = os.path.basename(safe_path)
    # For Telethon files like "{contactid}_{msgid}.ext" — use a clean name with extension
    import re
    # Check if filename is just UUID_msgid.ext (no original name embedded)
    match = re.match(r"^[0-9a-f-]+_\d+(\.[\w]+)$", raw_filename)
    if match:
        # Technical name only — use "file.ext" as download name
        ext_part = match.group(1)
        filename = f"file{ext_part}"
    else:
        # Try to extract original name after UUID_msgid_ prefix
        cleaned = re.sub(r"^[0-9a-f-]+_\d+_?", "", raw_filename, count=1)
        filename = cleaned if cleaned and len(cleaned) > 1 else raw_filename

    # Build Content-Disposition with RFC 5987 for non-ASCII filenames
    from urllib.parse import quote as url_quote
    try:
        filename.encode("ascii")
        cd_header = f'{disposition}; filename="{filename}"'
    except UnicodeEncodeError:
        ascii_name = "file" + os.path.splitext(filename)[1]
        encoded = url_quote(filename)
        cd_header = f"{disposition}; filename=\"{ascii_name}\"; filename*=UTF-8''{encoded}"

    return FileResponse(
        safe_path,
        media_type=content_type,
        headers={
            "X-Content-Type-Options": "nosniff",
            "Content-Disposition": cd_header,
        },
    )


# Type aliases for common dependencies
DB = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[Staff, Depends(get_current_user)]
AdminUser = Annotated[Staff, Depends(require_role("super_admin", "admin"))]
ContentManager = Annotated[Staff, Depends(require_role("super_admin", "admin", "assistant"))]
SuperAdmin = Annotated[Staff, Depends(require_role("super_admin"))]


async def require_crm_admin(user: CurrentUser) -> Staff:
    """
    Dependency for CRM admin panel endpoints. Only allows users with
    `is_crm_admin = True`, which is synced from PostForge's
    beta_features["crm_admin"] on every SSO login. Per-org roles
    (super_admin/admin) are NOT sufficient — this is a global flag
    granted ONLY by the PostForge admin panel toggle.
    """
    if not user.is_crm_admin:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "CRM admin access required. Grant via PostForge admin panel.",
        )
    return user


CrmAdminUser = Annotated[Staff, Depends(require_crm_admin)]


def _org_id(user: Staff) -> str | None:
    """Get the effective org_id for data scoping."""
    return user.postforge_org_id


def _audit(
    db,
    user: Staff,
    action: str,
    *,
    target_id: str | None = None,
    target_type: str | None = None,
    target_contact_id=None,
    metadata: dict | None = None,
    ip: str | None = None,
):
    """Append an entry to the audit trail. Fire-and-forget (caller commits)."""
    db.add(AuditLog(
        staff_id=user.id,
        action=action,
        target_contact_id=target_contact_id,
        target_id=target_id,
        target_type=target_type,
        metadata_json=metadata,
        ip_address=ip,
    ))


def _org_accounts_subq(user: Staff):
    """Subquery: TgAccount IDs belonging to user's org (active only).

    If org_id is None, returns an impossible condition (no matches)
    to prevent cross-workspace data leakage.
    """
    org = _org_id(user)
    if org is None:
        # No org = no data. Prevents matching all NULL org_id accounts.
        return select(TgAccount.id).where(sa_text("false"))
    return select(TgAccount.id).where(TgAccount.org_id == org, TgAccount.is_active.is_(True))


# ============================================================
# Startup / Shutdown
# ============================================================

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Add new columns to existing tables (idempotent)
        await conn.execute(
            sa_text("""
            DO $$ BEGIN
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS postforge_user_id VARCHAR;
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS postforge_org_id VARCHAR;
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS signature_mode VARCHAR DEFAULT 'named';
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS show_real_names BOOLEAN DEFAULT false;
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS real_tg_id BIGINT;
                CREATE INDEX IF NOT EXISTS ix_staff_real_tg_id ON staff (real_tg_id);
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS is_archived BOOLEAN DEFAULT false;
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS is_muted BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS crm_muted BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS avatar_thumb TEXT;
                -- PostForge integration: link contact to the campaign that brought
                -- this user (set on /api/internal/postforge-contact-upsert). When
                -- operator tags the contact, we use this to know which PostForge
                -- BotSubscriber row to update funnel_stage on.
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS postforge_campaign_id UUID;
                CREATE INDEX IF NOT EXISTS ix_contacts_postforge_campaign
                    ON contacts (postforge_campaign_id) WHERE postforge_campaign_id IS NOT NULL;
                -- Denormalized last-message preview. Replaces the expensive
                -- subquery that /api/contacts ran on every call.
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_message_content VARCHAR(200);
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_message_direction VARCHAR;
                ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_message_is_read BOOLEAN;
                -- One-shot backfill: populate the denormalized preview from
                -- the latest message per contact. Without this every
                -- historical chat shows an empty subtitle until its next
                -- message arrives. Guarded by a NULL check so it's a no-op
                -- on subsequent deploys.
                UPDATE contacts c SET
                    last_message_content = LEFT(COALESCE(sub.content, '[' || sub.media_type || ']', ''), 200),
                    last_message_direction = sub.direction,
                    last_message_is_read = sub.is_read
                FROM (
                    SELECT DISTINCT ON (m.contact_id)
                        m.contact_id, m.content, m.media_type, m.direction, m.is_read
                    FROM messages m
                    ORDER BY m.contact_id, m.created_at DESC
                ) sub
                WHERE c.id = sub.contact_id
                  AND c.last_message_content IS NULL
                  AND c.last_message_direction IS NULL;
                -- Supports the contacts-list sort: pinned first, then by recency
                CREATE INDEX IF NOT EXISTS ix_contacts_pin_recency
                    ON contacts (tg_account_id, is_archived, is_pinned DESC, last_message_at DESC NULLS LAST);
                -- Wider covering index including `status` and excluding
                -- service chat 777000. Planner can service list_contacts
                -- entirely from this index without touching the heap for
                -- the ordering step. The predicate narrows it further.
                CREATE INDEX IF NOT EXISTS ix_contacts_list_fast
                    ON contacts (tg_account_id, is_archived, status, is_pinned DESC, last_message_at DESC NULLS LAST)
                    WHERE real_tg_id <> 777000;
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS max_recipients INTEGER;
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS contact_ids UUID[] DEFAULT '{}';
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS last_error TEXT;
                -- Exclude list: contacts with ANY of these tags are filtered out
                -- of the recipient set, applied AFTER include/manual selection as
                -- defense-in-depth ("I excluded RD but accidentally ticked a
                -- contact with RD — server should still drop them").
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS tag_exclude TEXT[] DEFAULT '{}';
                -- Opt-in inclusion of archived contacts in the recipient set.
                -- Default false keeps existing broadcasts archived-blind.
                -- Tags live on contacts regardless of archive state, so a
                -- tag whose holders all sit in archive used to silently
                -- yield "0 подходит" with no recourse — flipping this on
                -- lifts the archive cut for that broadcast.
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS include_archived BOOLEAN NOT NULL DEFAULT FALSE;
                -- org_id columns for multi-tenancy
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE tags ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS media_path VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS media_type VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS blocks_json JSONB;
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                -- Tags: tg_account_id
                ALTER TABLE tags ADD COLUMN IF NOT EXISTS tg_account_id UUID REFERENCES tg_accounts(id);
                -- Pinned chats: org_id for org-wide pins
                ALTER TABLE pinned_chats ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                -- TG accounts: show_real_names, display_name
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS show_real_names BOOLEAN DEFAULT true;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS session_string TEXT;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS display_name VARCHAR;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS disconnected_at TIMESTAMP;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS auto_tags TEXT[] DEFAULT '{}';
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS auto_greeting_template_id UUID;
                ALTER TABLE messages ADD COLUMN IF NOT EXISTS grouped_id BIGINT;
                CREATE INDEX IF NOT EXISTS ix_messages_grouped_id ON messages (grouped_id) WHERE grouped_id IS NOT NULL;
                -- Scheduled messages
                CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    contact_id UUID NOT NULL REFERENCES contacts(id),
                    content TEXT,
                    media_path VARCHAR,
                    media_type VARCHAR,
                    scheduled_at TIMESTAMP NOT NULL,
                    timezone VARCHAR DEFAULT 'UTC',
                    status VARCHAR DEFAULT 'pending',
                    created_by UUID REFERENCES staff(id),
                    org_id VARCHAR,
                    created_at TIMESTAMP DEFAULT now(),
                    sent_at TIMESTAMP
                );
                -- Per-staff timezone
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS timezone VARCHAR DEFAULT 'UTC';
                -- Message edit history table
                CREATE TABLE IF NOT EXISTS message_edit_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    message_id UUID REFERENCES messages(id),
                    old_content TEXT,
                    new_content TEXT,
                    edited_at TIMESTAMP DEFAULT NOW()
                );
                -- Indexes
                CREATE INDEX IF NOT EXISTS ix_staff_postforge_org_id ON staff (postforge_org_id);
                CREATE INDEX IF NOT EXISTS ix_tg_accounts_org_id ON tg_accounts (org_id);
                CREATE INDEX IF NOT EXISTS ix_tags_org_id ON tags (org_id);
                CREATE INDEX IF NOT EXISTS ix_message_templates_org_id ON message_templates (org_id);
                CREATE INDEX IF NOT EXISTS ix_broadcasts_org_id ON broadcasts (org_id);
                CREATE INDEX IF NOT EXISTS ix_contacts_tg_account_id ON contacts (tg_account_id);
                CREATE INDEX IF NOT EXISTS ix_contacts_last_message_at ON contacts (last_message_at DESC NULLS LAST);
                CREATE INDEX IF NOT EXISTS ix_contacts_status ON contacts (status);
                CREATE UNIQUE INDEX IF NOT EXISTS uq_contacts_tg_account_peer ON contacts (tg_account_id, real_tg_id);
                -- Performance indexes for message queries
                CREATE INDEX IF NOT EXISTS ix_messages_contact_tgmsg_desc ON messages (contact_id, tg_message_id DESC NULLS LAST);
                CREATE INDEX IF NOT EXISTS ix_messages_unread ON messages (contact_id, direction, is_read) WHERE is_read = false AND direction = 'incoming';
                CREATE INDEX IF NOT EXISTS ix_messages_contact_created ON messages (contact_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS ix_messages_contact_id ON messages (contact_id);
                -- Composite index for contacts list query (most used query)
                CREATE INDEX IF NOT EXISTS ix_contacts_account_archived_status ON contacts (tg_account_id, is_archived, status);
                -- staff_tg_accounts FK indexes (eliminates 1.5M+ seq scans)
                CREATE INDEX IF NOT EXISTS ix_staff_tg_accounts_staff ON staff_tg_accounts(staff_id);
                CREATE INDEX IF NOT EXISTS ix_staff_tg_accounts_tg ON staff_tg_accounts(tg_account_id);
                CREATE INDEX IF NOT EXISTS ix_scheduled_messages_status ON scheduled_messages(status) WHERE status = 'pending';
                CREATE INDEX IF NOT EXISTS ix_staff_postforge_user ON staff(postforge_user_id);
                CREATE INDEX IF NOT EXISTS ix_messages_media_missing ON messages (contact_id, media_type) WHERE media_type IS NOT NULL AND media_type != 'sticker';
                -- NB: ix_contacts_tags_gin is built CONCURRENTLY outside this
                -- DO block (DDL inside DO can't use CONCURRENTLY). See the
                -- AUTOCOMMIT connection right after this transaction.
                -- Audit log: extended columns for SOC2-ready trail
                ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS target_id VARCHAR;
                ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS target_type VARCHAR;
                ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS metadata_json JSONB;
                ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS ip_address VARCHAR;
                -- CRM super-admin flag (synced from PostForge beta_features.crm_admin)
                ALTER TABLE staff ADD COLUMN IF NOT EXISTS is_crm_admin BOOLEAN NOT NULL DEFAULT false;
                CREATE INDEX IF NOT EXISTS ix_staff_is_crm_admin ON staff(is_crm_admin) WHERE is_crm_admin = true;
                -- Auto-approve all pending contacts (no approval flow)
                UPDATE contacts SET status = 'approved' WHERE status = 'pending';
            EXCEPTION WHEN OTHERS THEN NULL;
            END $$;
            """)
        )
        # Change Staff unique constraint: (postforge_user_id, postforge_org_id) instead of postforge_user_id alone
        await conn.execute(
            sa_text("""
            DO $$ BEGIN
                ALTER TABLE staff DROP CONSTRAINT IF EXISTS staff_postforge_user_id_key;
            EXCEPTION WHEN OTHERS THEN NULL;
            END $$;
            """)
        )
        await conn.execute(
            sa_text("""
            DO $$ BEGIN
                ALTER TABLE staff ADD CONSTRAINT uq_staff_pf_user_org UNIQUE (postforge_user_id, postforge_org_id);
            EXCEPTION WHEN duplicate_table THEN NULL;
            WHEN duplicate_object THEN NULL;
            END $$;
            """)
        )
        # Backfill org_id on existing data
        await conn.execute(sa_text("""
            UPDATE tg_accounts SET org_id = (
                SELECT s.postforge_org_id FROM staff s
                JOIN staff_tg_accounts sta ON sta.staff_id = s.id
                WHERE sta.tg_account_id = tg_accounts.id
                LIMIT 1
            ) WHERE org_id IS NULL
        """))
        await conn.execute(sa_text(
            "UPDATE tags SET org_id = (SELECT postforge_org_id FROM staff WHERE id = tags.created_by) WHERE org_id IS NULL AND created_by IS NOT NULL"
        ))
        await conn.execute(sa_text(
            "UPDATE message_templates SET org_id = (SELECT postforge_org_id FROM staff WHERE id = message_templates.created_by) WHERE org_id IS NULL AND created_by IS NOT NULL"
        ))
        await conn.execute(sa_text(
            "UPDATE broadcasts SET org_id = (SELECT postforge_org_id FROM staff WHERE id = broadcasts.created_by) WHERE org_id IS NULL AND created_by IS NOT NULL"
        ))
        # Clean up duplicate messages (keep oldest per contact_id + tg_message_id)
        await conn.execute(sa_text("""
            DELETE FROM messages WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY contact_id, tg_message_id
                        ORDER BY created_at ASC
                    ) AS rn
                    FROM messages
                    WHERE tg_message_id IS NOT NULL
                ) sub WHERE rn > 1
            )
        """))
        # Unique index to prevent future duplicates
        await conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_contact_tg_msg
            ON messages (contact_id, tg_message_id) WHERE tg_message_id IS NOT NULL
        """))

    # GIN index on contacts.tags built outside the transaction so we can use
    # CREATE INDEX CONCURRENTLY (which requires its own transaction). Without
    # CONCURRENTLY a plain CREATE INDEX takes a SHARE lock on contacts, which
    # blocks new-message inserts from the listener for the duration of the
    # build — at thousands of contacts/account that can be tens of seconds on
    # first deploy. Partial: only contacts with at least one tag get indexed,
    # which materially shrinks the index since plenty of contacts are untagged.
    # Idempotent (IF NOT EXISTS) and best-effort (any failure is logged but
    # doesn't crash startup — the broadcast query path still works without
    # this index, just slower). Nothing else depends on its presence.
    try:
        # AUTOCOMMIT isolation lets us run CREATE INDEX CONCURRENTLY which
        # cannot run inside a transaction block. In SQLAlchemy 2.x async,
        # AsyncConnection.execution_options IS a coroutine — must await.
        async with engine.connect() as ac_conn:
            ac_conn = await ac_conn.execution_options(isolation_level="AUTOCOMMIT")
            await ac_conn.execute(sa_text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_contacts_tags_gin "
                "ON contacts USING GIN (tags) WHERE array_length(tags, 1) > 0"
            ))
    except Exception as e:
        print(f"[STARTUP] ix_contacts_tags_gin build skipped: {e}", flush=True)

    await startup_listeners()
    asyncio.create_task(start_bot_polling())
    # Auto-sync dialogs for all connected accounts on startup
    await ws_manager.init_redis()
    from tasks import (
        auto_sync_on_startup, cleanup_disconnected_accounts,
        process_scheduled_messages, telethon_health_monitor, cleanup_old_media,
        periodic_sync,
    )
    asyncio.create_task(auto_sync_on_startup())
    asyncio.create_task(cleanup_disconnected_accounts())
    asyncio.create_task(process_scheduled_messages())
    asyncio.create_task(telethon_health_monitor())
    asyncio.create_task(cleanup_old_media())
    asyncio.create_task(periodic_sync())


@app.on_event("shutdown")
async def on_shutdown():
    await shutdown_listeners()
    await stop_bot()


# ============================================================
# Auth (Telegram-only)
# ============================================================

@app.post("/api/auth/tg")
async def tg_auth(req: TgAuthRequest, request: Request, db: DB):
    """Authenticate via Telegram Mini App initData.

    Returns tokens directly if user has one workspace,
    or a list of workspaces to choose from if multiple.
    """
    check_rate_limit(request)
    tg_user = validate_tg_init_data(req.init_data)
    tg_id = tg_user.get("id")
    if not tg_id:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "No user ID in initData")

    # 1. Find staff by exact tg_user_id (legacy non-SSO staff only).
    # We require postforge_user_id IS NULL to prevent collisions: SSO-created staff use a
    # synthetic 18-digit hash for tg_user_id, which can theoretically (though rarely) match a
    # real Telegram user ID. Filtering legacy staff this way guarantees a real-tg-id match.
    result = await db.execute(
        select(Staff).where(
            Staff.tg_user_id == tg_id,
            Staff.is_active.is_(True),
            Staff.postforge_user_id.is_(None),
        )
    )
    legacy_user = result.scalar_one_or_none()

    # 2. Find all staff with this real_tg_id (SSO-created staff with linked TG)
    result = await db.execute(
        select(Staff).where(Staff.real_tg_id == tg_id, Staff.is_active.is_(True))
    )
    sso_staff = list(result.scalars().all())

    # Merge: legacy + SSO (deduplicate by id)
    all_staff = []
    seen_ids = set()
    for s in ([legacy_user] if legacy_user else []) + sso_staff:
        if s and s.id not in seen_ids:
            all_staff.append(s)
            seen_ids.add(s.id)

    # 3. If no staff found, look up PostForge user by TG ID and link/create staff
    if not all_staff and settings.POSTFORGE_API_URL:
        import httpx, hashlib
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{settings.POSTFORGE_API_URL}/api/users/by-telegram/{tg_id}",
                )
            if resp.status_code == 200:
                pf_data = resp.json()
                pf_user_id = str(pf_data.get("id"))
                pf_nickname = pf_data.get("nickname") or tg_user.get("first_name", "User")
                pf_orgs = pf_data.get("organizations", [])

                # CRM is team-only. Solo users (no team org) cannot auto-provision
                # a personal workspace here — they must create a team in METRA AI first.
                if not pf_orgs:
                    raise HTTPException(
                        status.HTTP_403_FORBIDDEN,
                        detail={"code": "team_required", "message": "CRM доступен только для команд. Создайте команду в METRA AI."},
                    )
                org_contexts = [str(org["id"]) for org in pf_orgs]

                # For each context: find existing staff or create new.
                # IMPORTANT: only consider active staff. Deactivated rows are intentional
                # (e.g. personal-was-migrated-to-team) and must NOT be silently revived.
                for org_id in org_contexts:
                    is_personal = org_id.startswith("personal_")
                    result = await db.execute(
                        select(Staff).where(
                            Staff.postforge_user_id == pf_user_id,
                            Staff.postforge_org_id == org_id,
                            Staff.is_active.is_(True),
                        )
                    )
                    existing = result.scalar_one_or_none()

                    if existing:
                        # Link real_tg_id to this active staff
                        existing.real_tg_id = tg_id
                        all_staff.append(existing)
                    else:
                        # No active staff for this (user, org). Check if there is a
                        # DEACTIVATED row — if so, leave it deactivated and skip creating
                        # a new one (the deactivation was intentional, e.g. personal→team
                        # migration). The unique (postforge_user_id, postforge_org_id) key
                        # also makes a fresh INSERT here impossible.
                        deact_result = await db.execute(
                            select(Staff).where(
                                Staff.postforge_user_id == pf_user_id,
                                Staff.postforge_org_id == org_id,
                                Staff.is_active.is_(False),
                            )
                        )
                        if deact_result.scalar_one_or_none() is not None:
                            print(f"[TG_AUTH] Skipping deactivated staff for pf_user={pf_user_id} org={org_id}", flush=True)
                            continue
                        # Determine role
                        if is_personal:
                            crm_role = "super_admin"
                        else:
                            org_info = next((o for o in pf_orgs if str(o["id"]) == org_id), None)
                            if org_info and org_info.get("is_owner"):
                                crm_role = "super_admin"
                            elif org_info and org_info.get("role") in ("OWNER", "ADMIN", "owner", "admin"):
                                crm_role = "admin"
                            else:
                                crm_role = "operator"

                        synthetic = int(hashlib.sha256(f"{pf_user_id}:{org_id}".encode()).hexdigest()[:15], 16)
                        staff = Staff(
                            tg_user_id=synthetic,
                            tg_username=tg_user.get("username"),
                            role=crm_role,
                            name=pf_nickname,
                            real_tg_id=tg_id,
                            postforge_user_id=pf_user_id,
                            postforge_org_id=org_id,
                        )
                        db.add(staff)
                        all_staff.append(staff)

                if all_staff:
                    await db.commit()
                    for s in all_staff:
                        await db.refresh(s)
        except HTTPException:
            # Re-raise our own auth errors (e.g. team_required) — don't swallow them.
            raise
        except Exception as e:
            import traceback
            print(f"[TG_AUTH] PostForge lookup failed: {e}")
            traceback.print_exc()

    # 3b. Auto-create for admin chat ID if still no staff
    if not all_staff and tg_id == settings.TG_ADMIN_CHAT_ID:
        import hashlib
        personal_org_id = f"personal_admin_{tg_id}"
        synthetic = int(hashlib.sha256(f"tg:{tg_id}:{personal_org_id}".encode()).hexdigest()[:15], 16)
        user = Staff(
            tg_user_id=synthetic,
            tg_username=tg_user.get("username"),
            role="super_admin",
            name=tg_user.get("first_name", "Admin"),
            real_tg_id=tg_id,
            postforge_org_id=personal_org_id,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        all_staff = [user]

    print(f"[AUTH] tg_id={tg_id} found {len(all_staff)} staff: {[(s.postforge_org_id, s.role) for s in all_staff]} force_select={req.force_select}")

    if not all_staff:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Нет доступа. Привяжите Telegram в METRA AI.")

    # 4. Single workspace — auto-login (unless force_select requested)
    if len(all_staff) == 1 and not req.force_select:
        user = all_staff[0]
        return TgAuthResponse(
            access_token=create_token(user.id, "access"),
            refresh_token=create_token(user.id, "refresh"),
            role=user.role,
        )

    # 5. Multiple workspaces — return list for user to choose
    workspaces = []
    for s in all_staff:
        org_id = s.postforge_org_id or "unknown"
        if org_id.startswith("personal_"):
            name = "Личное"
        else:
            name = f"Команда"  # Could fetch org name from PostForge, but keep simple
        workspaces.append(TgWorkspaceItem(org_id=org_id, name=name, role=s.role))

    return TgAuthResponse(workspaces=workspaces)


@app.post("/api/auth/tg/select", response_model=TokenResponse)
async def tg_auth_select(req: TgWorkspaceSelect, db: DB):
    """Select a workspace after TG auth returned multiple options."""
    tg_user = validate_tg_init_data(req.init_data)
    tg_id = tg_user.get("id")
    if not tg_id:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "No user ID")

    # Find staff matching this TG user + selected org.
    # We match by real_tg_id (the verified Telegram ID from initData) AND postforge_org_id.
    # The legacy tg_user_id fallback was removed: SSO-created staff use a synthetic 18-digit
    # tg_user_id and a stray collision could let a user log into the wrong workspace.
    result = await db.execute(
        select(Staff).where(
            Staff.real_tg_id == tg_id,
            Staff.postforge_org_id == req.org_id,
            Staff.is_active.is_(True),
        )
    )
    user = result.scalar_one_or_none()

    # Legacy non-SSO staff (postforge_user_id IS NULL) — match by tg_user_id but ONLY if no
    # SSO link exists, to avoid the synthetic-id collision.
    if not user:
        result = await db.execute(
            select(Staff).where(
                Staff.tg_user_id == tg_id,
                Staff.postforge_org_id == req.org_id,
                Staff.is_active.is_(True),
                Staff.postforge_user_id.is_(None),
            )
        )
        user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Нет доступа к этому пространству")

    return TokenResponse(
        access_token=create_token(user.id, "access"),
        refresh_token=create_token(user.id, "refresh"),
        role=user.role,
    )


@app.post("/api/auth/refresh", response_model=TokenResponse)
async def refresh(req: RefreshRequest, request: Request, db: DB):
    check_rate_limit(request)
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


@app.post("/api/auth/sso", response_model=TokenResponse)
async def sso_auth(req: SsoAuthRequest, request: Request, db: DB):
    """Authenticate via PostForge SSO token.

    Verifies the token against PostForge API, auto-creates Staff if needed.
    Used when CRM is embedded inside PostForge (iframe) or opened from PostForge.
    """
    check_rate_limit(request)
    import httpx

    if not settings.POSTFORGE_API_URL:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "SSO not configured")

    # Verify PostForge token by calling PostForge /api/me. The CRM does
    # NOT independently validate the JWT signature — it trusts that a
    # 200 response from PostForge means the token is valid. This is
    # acceptable because the only way to get a 200 is to have a valid
    # PostForge JWT, which is the same credential the user would have
    # to compromise to access PostForge anyway.
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{settings.POSTFORGE_API_URL}/api/me",
                headers={
                    "Authorization": f"Bearer {req.postforge_token}",
                    # Tell PostForge this is a trusted service-to-service
                    # call — it validates the JWT + session liveness but
                    # skips the per-request fingerprint/country binding
                    # check. Without this our origin IP/UA fails that
                    # match and PostForge revokes the user's session on
                    # every CRM iframe load (symptom: users can't open
                    # CRM, 401 on /api/auth/sso).
                    "X-Service-Bot": settings.POSTFORGE_BOT_SECRET or "",
                },
            )
        if resp.status_code != 200:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid PostForge token")
        pf_user = resp.json()
    except httpx.RequestError as e:
        import logging
        logging.getLogger(__name__).error(f"Cannot reach PostForge: {e}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Cannot reach PostForge")
    except ValueError:  # JSON decode error
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Invalid response from PostForge")

    # Defense in depth: validate the PostForge response shape. A 200
    # with a missing/null 'id' would otherwise be coerced to the string
    # "None" and create a shared "None" Staff row anyone could claim.
    if not isinstance(pf_user, dict) or not pf_user.get("id"):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Malformed PostForge user response")

    pf_user_id = str(pf_user["id"])
    pf_email = pf_user.get("email", "")
    pf_nickname = pf_user.get("nickname", "User")
    pf_tg_id = pf_user.get("telegram_id")  # If PostForge user has linked Telegram
    pf_org_id = pf_user.get("organization_id")  # Team/org context
    pf_org_role = pf_user.get("organization_role")  # owner | admin | buyer | editor | custom
    pf_is_org_owner = pf_user.get("is_org_owner", False)

    # CRM super-admin flag: explicitly granted via PostForge admin panel
    # by adding "crm_admin" to the user's beta_features list. This is the
    # ONLY way to become a CRM super-admin — the per-org `role` field is
    # insufficient (it's auto-assigned by org ownership).
    pf_beta_features = pf_user.get("beta_features") or []
    is_crm_admin = "crm_admin" in pf_beta_features

    # CRM is team-only: block anyone not currently in a team context.
    # Frontend detects the "team_required" code and shows a "create team" modal.
    if not pf_org_id:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={"code": "team_required", "message": "CRM доступен только для команд. Создайте команду в METRA AI."},
        )

    import hashlib
    effective_org_id = str(pf_org_id)
    import logging
    logging.info(f"[SSO] user={pf_user_id} pf_org_id={pf_org_id} effective_org_id={effective_org_id} pf_org_role={pf_org_role}")
    # Unique tg_user_id per org context (hash of user_id + org_id)
    synthetic_tg_id = int(hashlib.sha256(f"{pf_user_id}:{effective_org_id}".encode()).hexdigest()[:15], 16)

    # Determine CRM role based on PostForge team context:
    # - Team owner → super_admin
    # - Team admin → admin
    # - Everyone else → operator
    if pf_is_org_owner:
        crm_role = "super_admin"
    elif pf_org_role in ("owner", "admin"):
        crm_role = "admin"
    else:
        crm_role = "operator"

    # Find existing Staff by (postforge_user_id, postforge_org_id) pair.
    # FOR UPDATE prevents a race where two concurrent SSO requests both see
    # "no Staff exists" and both INSERT a duplicate — which would create two
    # Staff records with the same synthetic tg_user_id and cause one user to
    # see another's Telegram chats.
    result = await db.execute(
        select(Staff).where(
            Staff.postforge_user_id == pf_user_id,
            Staff.postforge_org_id == effective_org_id,
            Staff.is_active.is_(True),
        ).with_for_update()
    )
    user = result.scalar_one_or_none()

    # Auto-create staff for SSO users
    if not user:
        # Check if user has a personal_ Staff that should be migrated to org
        if pf_org_id:
            personal_org = f"personal_{pf_user_id}"
            personal_result = await db.execute(
                select(Staff).where(
                    Staff.postforge_user_id == pf_user_id,
                    Staff.postforge_org_id == personal_org,
                    Staff.is_active.is_(True),
                )
            )
            personal_user = personal_result.scalar_one_or_none()
            if personal_user:
                # Deactivate personal Staff — user moved to org
                personal_user.is_active = False

        user = Staff(
            tg_user_id=synthetic_tg_id,
            tg_username=pf_email.split("@")[0] if pf_email else None,
            role=crm_role,
            name=pf_nickname or pf_email.split("@")[0],
            postforge_user_id=pf_user_id,
            postforge_org_id=effective_org_id,
            real_tg_id=pf_tg_id,  # Store real TG ID for Mini App auth
            is_crm_admin=is_crm_admin,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
    else:
        # Sync role, name, real_tg_id, and is_crm_admin from PostForge on each login
        changed = False
        if user.role != crm_role:
            user.role = crm_role
            changed = True
        if pf_nickname and user.name != pf_nickname:
            user.name = pf_nickname
            changed = True
        if pf_tg_id and user.real_tg_id != pf_tg_id:
            user.real_tg_id = pf_tg_id
            changed = True
        if user.is_crm_admin != is_crm_admin:
            user.is_crm_admin = is_crm_admin
            changed = True
        if changed:
            await db.commit()

    print(f"[SSO] user={pf_user_id} org={effective_org_id} role={crm_role} staff={user.id} name={user.name}", flush=True)

    _audit(db, user, "sso_login", metadata={"pf_user_id": pf_user_id, "role": crm_role})
    await db.commit()

    # Carry PostForge session id into CRM JWT so the parent-revoke webhook
    # can kill this token in O(1).
    pf_sid = pf_user.get("session_id")

    return TokenResponse(
        access_token=create_token(user.id, "access", pf_sid=pf_sid),
        refresh_token=create_token(user.id, "refresh", pf_sid=pf_sid),
        role=user.role,
    )


# ============================================================
# Telegram Accounts
# ============================================================

MAX_TG_ACCOUNTS = 50

@app.post("/api/tg/connect")
async def tg_connect(req: TgConnectRequest, request: Request, user: AdminUser, db: DB):
    check_rate_limit(request)
    # Check account limit
    count = await db.execute(
        select(func.count(TgAccount.id)).where(TgAccount.org_id == _org_id(user), TgAccount.is_active.is_(True))
    )
    if count.scalar() >= MAX_TG_ACCOUNTS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Максимум {MAX_TG_ACCOUNTS} аккаунтов")

    # Check if user can afford the monthly CRM fee before connecting
    billing_check = await _postforge_crm_billing_check(user)
    if billing_check and not billing_check.get("can_afford"):
        cost = billing_check.get("cost", "45")
        balance = billing_check.get("balance", "0")
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Недостаточно средств на балансе ({balance} коинов). "
            f"Стоимость подключения номера: {cost} коинов/месяц. "
            f"Пополните баланс в разделе Баланс."
        )

    try:
        result = await start_connect(req.phone)
        return {
            "status": "code_sent",
            "debug": result,
            "billing": billing_check,
        }
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Ошибка подключения: {str(e)[:200]}")


@app.post("/api/tg/verify", response_model=TgAccountOut)
async def tg_verify(req: TgVerifyRequest, user: CurrentUser, db: DB):
    try:
        account = await verify_code(req.phone, req.code, req.password_2fa)
    except Exception as e:
        msg = str(e)
        if "PasswordHashInvalid" in msg or "password" in msg.lower():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный 2FA пароль")
        if "PhoneCodeInvalid" in msg or "code" in msg.lower():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код подтверждения")
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Ошибка верификации: {msg[:200]}")
    # Set org_id on the newly connected account
    result = await db.execute(select(TgAccount).where(TgAccount.id == account.id))
    tg_acc = result.scalar_one_or_none()
    if tg_acc:
        tg_acc.org_id = _org_id(user)
        _audit(db, user, "tg_connect", target_id=str(account.id), target_type="tg_account",
               metadata={"phone": account.phone[:4] + "****"})  # Mask phone in logs
        await db.commit()

    # Charge the user for the CRM account (45 coins first month).
    # Non-blocking: if PostForge is unreachable or billing_enabled=0,
    # the account connects anyway — billing catches up on the next
    # monthly tick or when the user tops up.
    try:
        await _postforge_crm_billing_charge(
            user,
            crm_account_id=str(account.id),
            phone_number=account.phone,
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"CRM billing charge failed (non-blocking): {e}")

    # Auto-sync ALL dialogs in background after connecting (no limit)
    asyncio.create_task(_do_sync_dialogs(account.id, None))
    return account


@app.get("/api/tg/status", response_model=list[TgAccountOut])
async def tg_status(user: CurrentUser, db: DB):
    from telegram import _clients, _account_status, _account_errors
    query = select(TgAccount).where(TgAccount.org_id == _org_id(user), TgAccount.is_active.is_(True))
    # Non-admin users only see accounts assigned to them
    if user.role not in ("super_admin", "admin"):
        assigned = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(TgAccount.id.in_(assigned))
    result = await db.execute(query)
    accounts = result.scalars().all()
    is_operator = user.role not in ("super_admin", "admin")
    out = []
    for acc in accounts:
        d = TgAccountOut.model_validate(acc)
        client = _clients.get(acc.id)
        d.connected = bool(client and client.is_connected())
        # Operators must not see phone numbers
        if is_operator:
            d.phone = "••••" + (acc.phone[-4:] if acc.phone and len(acc.phone) >= 4 else "")
        out.append(d)
    return out


@app.get("/api/tg/billing")
async def tg_billing_info(user: CurrentUser):
    """
    Billing info for all CRM TG accounts of this user. Returns per-account
    data: connected_at, next_charge_at, cost per month. Used by the
    frontend settings page to show billing details next to each number.
    """
    accounts = await _postforge_crm_billing_accounts(user)
    billing_check = await _postforge_crm_billing_check(user)
    return {
        "accounts": accounts,
        "cost_per_month": billing_check.get("cost", "45") if billing_check else "45",
        "billing_enabled": billing_check.get("billing_enabled", False) if billing_check else False,
        "can_afford_new": billing_check.get("can_afford", True) if billing_check else True,
        "balance": billing_check.get("balance", "0") if billing_check else "0",
    }


@app.delete("/api/tg/disconnect/{account_id}")
async def tg_disconnect(account_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(TgAccount).where(TgAccount.id == account_id, TgAccount.org_id == _org_id(user)))
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Mark as inactive with timestamp (data kept for 30 days)
    account.is_active = False
    account.disconnected_at = datetime.utcnow()

    # Unlink from staff
    await db.execute(sa_delete(StaffTgAccount).where(StaffTgAccount.tg_account_id == account_id))

    _audit(db, user, "tg_disconnect", target_id=str(account_id), target_type="tg_account",
           metadata={"phone": account.phone[:4] + "****" if account.phone else None})
    await db.commit()
    try:
        await disconnect_account(account_id)
    except Exception:
        pass  # Session may be expired — DB cleanup already done

    # Notify PostForge to stop future billing for this account
    try:
        await _postforge_crm_billing_disconnect(str(account_id))
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"CRM billing disconnect failed (non-blocking): {e}")

    return {"status": "disconnected"}


@app.get("/api/tg/{account_id}/auto-settings")
async def get_auto_settings(account_id: UUID, user: CurrentUser, db: DB):
    """Get auto-tag + auto-greeting settings for a TG account."""
    result = await db.execute(
        select(TgAccount).where(TgAccount.id == account_id, TgAccount.org_id == _org_id(user))
    )
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return {
        "auto_tags": account.auto_tags or [],
        "auto_greeting_template_id": str(account.auto_greeting_template_id) if account.auto_greeting_template_id else None,
    }


class AutoSettingsUpdate(PydanticBaseModel):
    auto_tags: list[str] | None = None
    auto_greeting_template_id: str | None = None  # UUID string or null


@app.patch("/api/tg/{account_id}/auto-settings")
async def update_auto_settings(account_id: UUID, req: AutoSettingsUpdate, user: CurrentUser, db: DB):
    """Update auto-tag + auto-greeting settings for a TG account."""
    result = await db.execute(
        select(TgAccount).where(TgAccount.id == account_id, TgAccount.org_id == _org_id(user))
    )
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    if req.auto_tags is not None:
        account.auto_tags = req.auto_tags
    if req.auto_greeting_template_id is not None:
        if req.auto_greeting_template_id == "" or req.auto_greeting_template_id == "null":
            account.auto_greeting_template_id = None
        else:
            account.auto_greeting_template_id = UUID(req.auto_greeting_template_id)

    await db.commit()
    return {
        "auto_tags": account.auto_tags or [],
        "auto_greeting_template_id": str(account.auto_greeting_template_id) if account.auto_greeting_template_id else None,
    }


# ============================================================
# Contacts
# ============================================================

@app.get("/api/drafts")
async def get_drafts_endpoint(user: CurrentUser, db: DB, tg_account_id: UUID | None = None):
    """Get Telegram drafts for user's accounts, matched to CRM contacts."""
    from telegram import get_drafts
    # Get accounts for this user's org
    acct_result = await db.execute(
        select(TgAccount.id).where(TgAccount.org_id == _org_id(user), TgAccount.is_active.is_(True))
    )
    account_ids = [r[0] for r in acct_result.all()]
    if tg_account_id and tg_account_id in account_ids:
        account_ids = [tg_account_id]

    # Collect all drafts from all accounts
    raw_drafts = []
    for aid in account_ids:
        drafts = await get_drafts(aid)
        for d in drafts:
            d["_account_id"] = aid
            raw_drafts.append(d)

    if not raw_drafts:
        return []

    # Batch lookup: match all peer_ids to contacts in one query
    peer_ids = [d["peer_id"] for d in raw_drafts]
    contact_result = await db.execute(
        select(Contact.id, Contact.alias, Contact.real_tg_id, Contact.tg_account_id).where(
            Contact.real_tg_id.in_(peer_ids),
            Contact.tg_account_id.in_(account_ids),
        )
    )
    # Build lookup: (account_id, peer_id) -> (contact_id, alias)
    contact_map = {}
    for row in contact_result.all():
        contact_map[(row[3], row[2])] = (str(row[0]), row[1])

    all_drafts = []
    for d in raw_drafts:
        key = (d["_account_id"], d["peer_id"])
        match = contact_map.get(key)
        if match:
            d["contact_id"] = match[0]
            d["contact_alias"] = match[1]
            d["tg_account_id"] = str(d.pop("_account_id"))
            all_drafts.append(d)
        else:
            d.pop("_account_id", None)
    return all_drafts


@app.get("/api/contacts", response_model=list[ContactOut])
async def list_contacts(
    user: CurrentUser,
    db: DB,
    status_filter: str | None = Query(None, alias="status"),
    assigned_to: UUID | None = None,
    tag: str | None = None,
    tg_account_id: UUID | None = None,
    archived: bool = Query(False),
    search: str | None = Query(None, description="Search by alias or phone"),
    # Date range — filters contacts whose FIRST incoming message landed
    # inside the window. Matches the semantic used by /api/reports/new-chats
    # ("new chats in this period"). Contact.last_message_at would be wrong
    # here: an old contact who replied today would falsely appear as "new
    # today". Uses the same MIN(created_at) WHERE direction='incoming'
    # subquery pattern.
    from_date: str | None = Query(None, description="YYYY-MM-DD or YYYY-MM-DDTHH:MM"),
    to_date: str | None = Query(None, description="YYYY-MM-DD or YYYY-MM-DDTHH:MM"),
    # The denormalization (last_message_* columns) makes 2000-row responses
    # cheap at the DB layer. Frontend still pays for payload size + JSON
    # parse, but that's a network/client concern — add explicit pagination
    # later if needed. Keep the default permissive so existing clients
    # don't silently truncate their lists.
    limit: int = Query(2000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    # Redis cache disabled — caused empty contacts for some users
    cache_key = None

    query = select(Contact)

    # Org scoping: only contacts from this org's TG accounts
    query = query.where(Contact.tg_account_id.in_(_org_accounts_subq(user)))

    # Filter out Telegram service account
    query = query.where(Contact.real_tg_id != 777000)

    # Archive filter
    query = query.where(Contact.is_archived == archived)

    # Filter by specific TG account
    if tg_account_id:
        query = query.where(Contact.tg_account_id == tg_account_id)

    # Operators see contacts from their assigned TG accounts only
    if user.role == "operator":
        sub = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(Contact.tg_account_id.in_(sub))
    # Filter blocked contacts unless explicitly requested
    if status_filter:
        query = query.where(Contact.status == status_filter)
    else:
        query = query.where(Contact.status != "blocked")

    if assigned_to:
        query = query.where(Contact.assigned_to == assigned_to)
    if tag:
        query = query.where(Contact.tags.any(tag))
    if search:
        search_pattern = f"%{search}%"
        # Contact has no `phone` column (only TgAccount does). Search by
        # alias only — previously the code referenced Contact.phone and
        # raised AttributeError on every search call.
        query = query.where(Contact.alias.ilike(search_pattern))

    # Date-range filter over first-incoming-message timestamp.
    if from_date or to_date:
        def _parse_contact_dt(s: str, end_of_day: bool) -> datetime:
            try:
                if "T" in s or " " in s:
                    return datetime.fromisoformat(s.replace(" ", "T"))
                d = datetime.fromisoformat(s)
                return d.replace(hour=23, minute=59, second=59) if end_of_day else d
            except ValueError:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid date format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM.")

        first_msg_sub = (
            select(
                Message.contact_id.label("contact_id"),
                func.min(Message.created_at).label("first_at"),
            )
            .where(Message.direction == "incoming")
            .group_by(Message.contact_id)
            .subquery()
        )
        query = query.join(first_msg_sub, first_msg_sub.c.contact_id == Contact.id)
        if from_date:
            query = query.where(first_msg_sub.c.first_at >= _parse_contact_dt(from_date, end_of_day=False))
        if to_date:
            query = query.where(first_msg_sub.c.first_at <= _parse_contact_dt(to_date, end_of_day=True))

    query = query.order_by(Contact.is_pinned.desc(), Contact.last_message_at.desc().nullslast())
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    contacts = list(result.scalars().all())

    # Build per-account show_real_names map
    acct_result = await db.execute(
        select(TgAccount.id, TgAccount.show_real_names).where(TgAccount.org_id == _org_id(user))
    )
    show_real_map = {row[0]: row[1] for row in acct_result.all()}

    # The last_message_* fields are now denormalized columns on Contact,
    # populated by the listener / send API / read-event handlers. The old
    # O(messages) subquery that computed them on every call is gone.
    for c in contacts:
        if c.chat_type != "private":
            title = decrypt(c.group_title_encrypted) if c.group_title_encrypted else None
            if title:
                c.alias = title
        elif show_real_map.get(c.tg_account_id, False):
            real_name = decrypt(c.real_name_encrypted) if c.real_name_encrypted else None
            if real_name:
                c.alias = real_name
        # Pre-compute signed avatar URL so the frontend doesn't need a
        # separate round-trip per contact. Signature binds both contact_id
        # and tg_account_id to prevent URL tampering.
        c.avatar_url = _build_avatar_signed_url(c.id, c.tg_account_id)

    # Cache result for 15s
    if cache_key:
        try:
            from pydantic import TypeAdapter
            ta = TypeAdapter(list[ContactOut])
            serialized = ta.dump_json(contacts).decode()
            await cache_set(cache_key, serialized, ttl=15)
        except Exception:
            pass

    return contacts


@app.get("/api/contacts/{contact_id}", response_model=ContactOut)
async def get_contact(contact_id: UUID, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)

    # Always show real title for groups/channels/supergroups
    if contact.chat_type != "private":
        title = decrypt(contact.group_title_encrypted) if contact.group_title_encrypted else None
        if title:
            contact.alias = title
    elif contact.tg_account_id:
        acct = await db.execute(select(TgAccount.show_real_names).where(TgAccount.id == contact.tg_account_id))
        show_real = acct.scalar_one_or_none()
        if show_real:
            real_name = decrypt(contact.real_name_encrypted) if contact.real_name_encrypted else None
            if real_name:
                contact.alias = real_name

    contact.avatar_url = _build_avatar_signed_url(contact.id, contact.tg_account_id)
    return contact


@app.patch("/api/contacts/{contact_id}", response_model=ContactOut)
async def update_contact(contact_id: UUID, req: ContactUpdate, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)

    # Snapshot old tags BEFORE assignment so we can detect newly-added tags
    # for the PostForge funnel webhook below.
    old_tags = set(contact.tags or [])

    if req.alias is not None:
        contact.alias = req.alias
    if req.tags is not None:
        contact.tags = req.tags
    if req.notes is not None:
        contact.notes = req.notes
    if req.assigned_to is not None and user.role in ("super_admin", "admin"):
        contact.assigned_to = req.assigned_to
    if req.is_archived is not None:
        contact.is_archived = req.is_archived

    await db.commit()
    await db.refresh(contact)

    # PostForge funnel webhook: if the operator added a tag that maps to a
    # PostForge funnel stage (e.g. "qualified", "купил") AND this contact is
    # linked to a PostForge campaign — push the stage update so the pixel
    # learns about QualifiedLead / Application / Purchase events.
    #
    # Best-effort: any failure is logged inside the helper and doesn't
    # affect this PATCH response. Skip silently if Contact has no
    # postforge_campaign_id (legacy contacts created before integration).
    if (
        req.tags is not None
        and contact.postforge_campaign_id
        and contact.real_tg_id
        and _TAG_TO_STAGE
    ):
        # Diff against the OLD tags case-sensitively (preserves operator's
        # capitalization in DB), but lookup in TAG_TO_STAGE case-INsensitively.
        new_tags = set(req.tags or [])
        added = new_tags - old_tags
        # Pick the FIRST mapped tag from the diff (deterministic order via
        # original list iteration). If multiple stage-mapping tags were
        # added in one PATCH, only the first one fires — that's enough.
        stage_to_push: str | None = None
        for tag in (req.tags or []):
            if tag in added:
                # Normalize for lookup so "Qualified" / "QUALIFIED" /
                # " qualified " all match the "qualified" map key.
                stage = _TAG_TO_STAGE.get(str(tag).strip().lower())
                if stage:
                    stage_to_push = stage
                    break
        if stage_to_push:
            import asyncio as _asyncio
            _asyncio.create_task(
                _postforge_funnel_stage_update(
                    campaign_id=str(contact.postforge_campaign_id),
                    telegram_user_id=int(contact.real_tg_id),
                    stage=stage_to_push,
                )
            )

    # Apply show_real_names / group title resolution ONLY if alias wasn't explicitly changed
    if req.alias is None:
        if contact.chat_type != "private":
            title = decrypt(contact.group_title_encrypted) if contact.group_title_encrypted else None
            if title:
                contact.alias = title
        elif contact.tg_account_id:
            acct = await db.execute(select(TgAccount.show_real_names).where(TgAccount.id == contact.tg_account_id))
            show_real = acct.scalar_one_or_none()
            if show_real:
                real_name = decrypt(contact.real_name_encrypted) if contact.real_name_encrypted else None
                if real_name:
                    contact.alias = real_name

    return contact


@app.post("/api/contacts/{contact_id}/approve", response_model=ContactOut)
async def approve_contact(contact_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    contact.status = "approved"
    contact.approved_at = func.now()
    await db.commit()
    await db.refresh(contact)

    await ws_manager.broadcast_to_admins({
        "type": "contact_approved",
        "contact_id": str(contact.id),
    }, org_id=_org_id(user))
    return contact


@app.post("/api/contacts/{contact_id}/block", response_model=ContactOut)
async def block_contact(contact_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    contact.status = "blocked"
    await db.commit()
    await db.refresh(contact)

    await ws_manager.broadcast_to_admins({
        "type": "contact_blocked",
        "contact_id": str(contact.id),
    }, org_id=_org_id(user))
    return contact


@app.delete("/api/contacts/{contact_id}", status_code=204)
async def delete_contact(contact_id: UUID, user: AdminUser, db: DB):
    """Delete a contact and its messages from CRM (does not affect Telegram)."""
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Delete related records first (FK constraints)
    from sqlalchemy import delete as sa_delete
    await db.execute(sa_delete(BroadcastRecipient).where(BroadcastRecipient.contact_id == contact_id))
    await db.execute(sa_delete(AuditLog).where(AuditLog.target_contact_id == contact_id))
    await db.execute(sa_delete(PinnedChat).where(PinnedChat.contact_id == contact_id))
    await db.execute(sa_delete(Message).where(Message.contact_id == contact_id))
    await db.delete(contact)
    await db.commit()

    await ws_manager.broadcast_to_admins({
        "type": "contact_deleted",
        "contact_id": str(contact_id),
    }, org_id=_org_id(user))
    return


# ---- Pinned chats (per-user) ----

@app.get("/api/pinned")
async def get_pinned(user: Annotated[Staff, Depends(get_current_user)], db: DB):
    """Return pinned contact IDs, filtered to LIVE non-archived contacts
    that still belong to one of the user's accessible TG accounts.

    Without this filter the frontend would see ghost pins: a PinnedChat
    row survives archive/delete/account-disconnect and kept surfacing as
    "pinned" in the Set, causing stale/archived chats to appear right
    below the pinned block and then disappear after the next refetch.
    """
    org = _org_id(user)
    query = (
        select(PinnedChat.contact_id)
        .join(Contact, Contact.id == PinnedChat.contact_id)
        .where(
            PinnedChat.org_id == org,
            Contact.is_archived.is_(False),
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    # Operators see contacts from their assigned TG accounts only —
    # mirror the same restriction list_contacts applies so the pinned
    # set can't leak contact UUIDs from accounts the operator isn't
    # authorized to see.
    if user.role == "operator":
        sub = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(Contact.tg_account_id.in_(sub))
    result = await db.execute(query)
    return [str(row[0]) for row in result.all()]


async def _set_contact_pin(contact_id: UUID, user: Staff, db, pinned: bool) -> None:
    """Shared body for pin/unpin. Mirrors `pinned` to Contact.is_pinned
    and keeps the legacy PinnedChat table in sync. CRM-LOCAL ONLY — does
    NOT touch Telegram.

    History: this used to push the pin state to Telegram via Telethon
    (`telegram.set_chat_pin`, which calls ToggleDialogPinRequest). That
    forced CRM users to live within the TG hard cap (5 pinned dialogs
    for non-Premium accounts) and surfaced `PinnedDialogsTooMuchError`
    as HTTP 502 in the CRM UI — operators got a Cloudflare 502 page on
    a routine "pin chat" click once they had ≥5 pinned. Operator's call
    on 27-Apr: pin is a CRM-local concept, decoupled from TG. CRM users
    can pin as many chats as they want; TG side stays untouched.

    `telegram.set_chat_pin` is now unreferenced — left in place rather
    than deleted, in case we want to re-enable per-account TG sync later.
    """
    from sqlalchemy import delete as sa_delete

    org = _org_id(user)
    result = await db.execute(select(Contact).where(
        Contact.id == contact_id,
        Contact.tg_account_id.in_(_org_accounts_subq(user)),
    ))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    contact.is_pinned = pinned

    # Keep legacy per-org PinnedChat in sync so existing list-sort / UI bits
    # still work without a second round-trip. Idempotent: double-pin or
    # double-unpin is a no-op.
    if pinned:
        existing = await db.execute(
            select(PinnedChat).where(PinnedChat.org_id == org, PinnedChat.contact_id == contact_id)
        )
        if not existing.scalar_one_or_none():
            db.add(PinnedChat(staff_id=user.id, contact_id=contact_id, org_id=org))
    else:
        await db.execute(
            sa_delete(PinnedChat).where(PinnedChat.org_id == org, PinnedChat.contact_id == contact_id)
        )

    await db.commit()
    await cache_invalidate(f"contacts:{org}:*")


@app.post("/api/pinned/{contact_id}", status_code=204)
async def pin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    """Pin chat in CRM only. Does NOT touch Telegram (decoupled 27-Apr —
    see `_set_contact_pin` docstring). CRM has no pin cap; pinning is
    independent of TG's 5-dialog hard limit."""
    await _set_contact_pin(contact_id, user, db, pinned=True)


@app.delete("/api/pinned/{contact_id}", status_code=204)
async def unpin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    """Unpin chat in CRM only (no TG side-effect)."""
    await _set_contact_pin(contact_id, user, db, pinned=False)


@app.get("/api/contacts/{contact_id}/reveal", response_model=ContactReveal)
async def reveal_contact(contact_id: UUID, user: AdminUser, db: DB):
    """Reveal real client data. Logged in audit."""
    contact = await _get_contact_with_access(contact_id, user, db)

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

    # Verify TG account belongs to user's org
    r = await db.execute(select(TgAccount).where(TgAccount.id == req.tg_account_id, TgAccount.org_id == _org_id(user)))
    if not r.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TG account not found in your workspace")

    # Batch-load CRM contacts to resolve real TG IDs (avoid N+1)
    r = await db.execute(select(Contact).where(
        Contact.id.in_(req.member_contact_ids),
        Contact.tg_account_id.in_(_org_accounts_subq(user)),
    ))
    member_tg_ids = [c.real_tg_id for c in r.scalars().all() if c.real_tg_id]

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

    # Verify group contact belongs to user's org
    group = await _get_contact_with_access(contact_id, user, db)
    if group.chat_type not in ("group", "channel"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Not a group chat")

    member_contact_id = req.get("member_contact_id")
    if not member_contact_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "member_contact_id required")

    # Verify member contact belongs to user's org
    member = await _get_contact_with_access(UUID(member_contact_id), user, db)

    try:
        await add_group_member(group.tg_account_id, group.real_tg_id, str(member.real_tg_id))
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to add group member: {e}")
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Failed to add member to group")

    return {"ok": True}


async def _get_contact_with_access(contact_id: UUID, user: Staff, db: AsyncSession) -> Contact:
    result = await db.execute(
        select(Contact).where(
            Contact.id == contact_id,
            Contact.tg_account_id.in_(_org_accounts_subq(user)),  # org scoping
        )
    )
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

@app.get("/api/messages/{contact_id}/topics")
async def get_contact_topics(contact_id: UUID, user: CurrentUser, db: DB):
    """Get list of forum topics directly from Telegram API."""
    contact = await _get_contact_with_access(contact_id, user, db)
    if not contact.is_forum or not contact.tg_account_id:
        return []

    from telegram import _clients
    client = _clients.get(contact.tg_account_id)
    if not client:
        # Fallback to DB
        result = await db.execute(
            select(Message.topic_id, Message.topic_name)
            .where(Message.contact_id == contact_id, Message.topic_id.isnot(None))
            .group_by(Message.topic_id, Message.topic_name)
            .order_by(Message.topic_id)
        )
        return [{"id": row[0], "name": row[1] or ("General" if row[0] == 1 else f"Topic #{row[0]}")} for row in result.all()]

    # Fetch topics from Telegram
    topics = [{"id": 1, "name": "General"}]
    try:
        from telethon.tl.functions.channels import GetForumTopicsByIDRequest, GetForumTopicsRequest
        entity = await client.get_input_entity(contact.real_tg_id)
        result = await client(GetForumTopicsRequest(
            channel=entity, offset_date=None, offset_id=0, offset_topic=0, limit=100,
        ))
        for t in result.topics:
            if t.id != 1:
                topics.append({"id": t.id, "name": t.title})
    except ImportError:
        # Telethon version without forum support — fallback to DB
        result = await db.execute(
            select(Message.topic_id, Message.topic_name)
            .where(Message.contact_id == contact_id, Message.topic_id.isnot(None))
            .group_by(Message.topic_id, Message.topic_name)
            .order_by(Message.topic_id)
        )
        topics = [{"id": row[0], "name": row[1] or ("General" if row[0] == 1 else f"Topic #{row[0]}")} for row in result.all()]
    except Exception as e:
        print(f"[TOPICS] Failed to fetch from TG: {e}")
        # Fallback to DB
        result = await db.execute(
            select(Message.topic_id, Message.topic_name)
            .where(Message.contact_id == contact_id, Message.topic_id.isnot(None))
            .group_by(Message.topic_id, Message.topic_name)
            .order_by(Message.topic_id)
        )
        topics = [{"id": row[0], "name": row[1] or ("General" if row[0] == 1 else f"Topic #{row[0]}")} for row in result.all()]

    return topics


@app.post("/api/messages/{contact_id}/download-missing-media")
async def download_missing_media_endpoint(contact_id: UUID, user: CurrentUser, db: DB):
    """Check for missing media and download in background. Returns immediately."""
    contact = await _get_contact_with_access(contact_id, user, db)

    # Quick check: any media messages that might be missing files?
    result = await db.execute(
        select(Message.id, Message.tg_message_id, Message.media_path).where(
            Message.contact_id == contact.id,
            Message.media_type.isnot(None),
            Message.media_type != "sticker",
            Message.tg_message_id.isnot(None),
        ).order_by(Message.created_at.desc()).limit(100)
    )
    rows = result.all()
    missing_ids = []
    for msg_id, tg_msg_id, media_path in rows:
        if not media_path or not os.path.isfile(os.path.join(MEDIA_DIR, media_path)):
            missing_ids.append((msg_id, tg_msg_id))

    if not missing_ids:
        return {"status": "ok", "missing": 0}

    # Download in background — don't block the response
    async def _bg_download(account_id, chat_tg_id, contact_id, missing):
        from telegram import download_missing_media as _dl_media
        from models import async_session as _async_session
        async with _async_session() as bg_db:
            downloaded = 0
            for msg_id, tg_msg_id in missing[:50]:
                path = await _dl_media(account_id, chat_tg_id, tg_msg_id, contact_id)
                if path:
                    result = await bg_db.execute(select(Message).where(Message.id == msg_id))
                    msg = result.scalar_one_or_none()
                    if msg:
                        msg.media_path = path
                        downloaded += 1
            if downloaded > 0:
                await bg_db.commit()
                print(f"[BG-DOWNLOAD] Downloaded {downloaded} missing media for contact {contact_id}")

    asyncio.create_task(_bg_download(contact.tg_account_id, contact.real_tg_id, contact.id, missing_ids))
    return {"status": "downloading", "missing": len(missing_ids)}


@app.get("/api/messages/{contact_id}", response_model=list[MessageOut])
async def get_messages(
    contact_id: UUID,
    user: CurrentUser,
    db: DB,
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    topic_id: int | None = Query(None),
):
    contact = await _get_contact_with_access(contact_id, user, db)

    # On-demand fetch from Telegram for forum topics with few messages in DB
    if contact.is_forum and contact.tg_account_id and topic_id is not None:
        from telegram import _resolve_topic_name, _clients, _extract_media, sanitize_text
        client = _clients.get(contact.tg_account_id)
        if client:
            # Check how many messages we have for this topic
            count_q = select(func.count(Message.id)).where(
                Message.contact_id == contact_id, Message.topic_id == topic_id
            )
            db_count = (await db.execute(count_q)).scalar() or 0

            if db_count < limit:
                # Fetch more from Telegram for this topic
                try:
                    existing_tg_ids_q = select(Message.tg_message_id).where(
                        Message.contact_id == contact_id,
                        Message.tg_message_id.isnot(None),
                    )
                    existing_result = await db.execute(existing_tg_ids_q)
                    existing_tg_ids = {r[0] for r in existing_result.all()}

                    me = await client.get_me()
                    tg_msgs = await client.get_messages(
                        contact.real_tg_id, reply_to=topic_id, limit=200
                    )
                    added = 0
                    for msg_obj in tg_msgs:
                        if not msg_obj or msg_obj.id in existing_tg_ids:
                            continue
                        if not (msg_obj.text or msg_obj.media):
                            continue

                        sender_id = getattr(msg_obj, "sender_id", None) or getattr(msg_obj, "from_id", None)
                        if hasattr(sender_id, "user_id"):
                            sender_id = sender_id.user_id
                        direction = "outgoing" if sender_id == me.id else "incoming"

                        media_type, ext = _extract_media(msg_obj)
                        media_path = None
                        if media_type and msg_obj.media:
                            try:
                                fname = f"{contact.id}_{msg_obj.id}{ext or ''}"
                                dl_path = os.path.join(MEDIA_DIR, fname)
                                await client.download_media(msg_obj, file=dl_path)
                                media_path = fname
                            except Exception:
                                pass

                        sender_tg_id_val = None
                        sender_alias_val = None
                        if contact.chat_type != "private" and direction == "incoming":
                            sender = getattr(msg_obj, "sender", None)
                            if sender:
                                sender_tg_id_val = getattr(sender, "id", None)
                                fn = getattr(sender, "first_name", "") or ""
                                ln = getattr(sender, "last_name", "") or ""
                                tt = getattr(sender, "title", "") or ""
                                sender_alias_val = (f"{fn} {ln}".strip() or tt or "User")

                        topic_name_val = await _resolve_topic_name(client, contact.real_tg_id, topic_id, contact.tg_account_id)

                        fwd_alias = None
                        if msg_obj.forward:
                            fwd_sender = msg_obj.forward.sender
                            if fwd_sender:
                                fn = getattr(fwd_sender, "first_name", "") or ""
                                ln = getattr(fwd_sender, "last_name", "") or ""
                                tt = getattr(fwd_sender, "title", "") or ""
                                fwd_alias = (f"{fn} {ln}".strip() or tt or "User")

                        msg_date = msg_obj.date.replace(tzinfo=None) if msg_obj.date else datetime.utcnow()
                        db_msg = Message(
                            contact_id=contact.id,
                            tg_message_id=msg_obj.id,
                            direction=direction,
                            content=sanitize_text(msg_obj.text),
                            media_type=media_type,
                            media_path=media_path,
                            is_read=True,
                            sender_tg_id=sender_tg_id_val,
                            sender_alias=sender_alias_val,
                            topic_id=topic_id,
                            topic_name=topic_name_val,
                            forwarded_from_alias=fwd_alias,
                            created_at=msg_date,
                        )
                        db.add(db_msg)
                        added += 1

                    if added:
                        await db.commit()
                        print(f"[TOPIC-FETCH] Loaded {added} messages for topic {topic_id} in contact {contact_id}")
                except Exception as e:
                    await db.rollback()
                    print(f"[TOPIC-FETCH] Error: {e}")

    query = (
        select(Message)
        .where(Message.contact_id == contact_id)
        .order_by(Message.tg_message_id.desc().nullslast(), Message.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    if topic_id is not None:
        query = query.where(Message.topic_id == topic_id)
    result = await db.execute(query)
    messages = list(reversed(result.scalars().all()))

    # Backfill topic_id and topic_name for forum supergroups (for "all topics" view)
    if contact.is_forum and contact.tg_account_id and topic_id is None:
        from telegram import _resolve_topic_name, _clients
        client = _clients.get(contact.tg_account_id)
        if client:
            updated = False
            needs_topic = [m for m in messages if m.topic_id is None and m.tg_message_id]
            if needs_topic:
                try:
                    tg_ids = [m.tg_message_id for m in needs_topic]
                    tg_msgs = await client.get_messages(contact.real_tg_id, ids=tg_ids)
                    tg_map = {}
                    for tm in tg_msgs:
                        if tm:
                            tg_map[tm.id] = tm
                    for m in needs_topic:
                        tm = tg_map.get(m.tg_message_id)
                        if tm and tm.reply_to:
                            rt = tm.reply_to
                            if getattr(rt, "forum_topic", False):
                                m.topic_id = getattr(rt, "reply_to_msg_id", None)
                            else:
                                m.topic_id = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
                        elif tm:
                            m.topic_id = 1
                        if m.topic_id:
                            updated = True
                except Exception as e:
                    print(f"[TOPIC-BACKFILL] Error: {e}")

            for m in messages:
                if m.topic_id and not m.topic_name:
                    name = await _resolve_topic_name(client, contact.real_tg_id, m.topic_id, contact.tg_account_id)
                    if name:
                        m.topic_name = name
                        updated = True
            if updated:
                await db.commit()

    for m in messages:
        _attach_media_url(m)
    return messages


@app.post("/api/messages/{contact_id}/send", response_model=MessageOut)
async def send_msg(contact_id: UUID, req: SendMessage, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)

    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

    check_tg_send_limit(str(contact.tg_account_id), contact.real_tg_id)

    # Resolve reply-to. Filter by contact_id so a user can't reference a
    # message from a different contact (or from another tenant entirely).
    # Without this filter, sending POST /api/messages/{contact_id}/send
    # with reply_to_msg_id set to ANY message UUID would happily resolve
    # the row, exfiltrating its tg_message_id and content preview into
    # the response — a cross-tenant message disclosure vector.
    reply_to_tg_msg_id = None
    reply_to_msg_id = None
    reply_to_content_preview = None
    if req.reply_to_msg_id:
        rr = await db.execute(
            select(Message).where(
                Message.id == req.reply_to_msg_id,
                Message.contact_id == contact.id,
            )
        )
        ref_msg = rr.scalar_one_or_none()
        if ref_msg:
            reply_to_tg_msg_id = ref_msg.tg_message_id
            reply_to_msg_id = ref_msg.id
            preview = ref_msg.content or (f"[{ref_msg.media_type}]" if ref_msg.media_type else "...")
            reply_to_content_preview = preview[:200]

    # Send via Telethon
    try:
        tg_msg_id = await send_message(
            contact.tg_account_id, contact.real_tg_id, req.content,
            reply_to_tg_msg_id=reply_to_tg_msg_id,
        )
    except ValueError as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e))

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
    _touch_contact_preview(contact, req.content, None, "outgoing")
    await db.commit()
    await db.refresh(msg)

    _attach_media_url(msg)
    return msg


# ---------- Scheduled messages ----------

class ScheduleMessageRequest(PydanticBaseModel):
    content: str | None = None
    scheduled_at: str  # ISO format: "2026-03-26T14:30:00"
    timezone: str = "UTC"

class ScheduleMessageUpdate(PydanticBaseModel):
    content: str | None = None
    scheduled_at: str | None = None
    timezone: str | None = None

class ScheduledMessageOut(PydanticBaseModel):
    id: UUID
    contact_id: UUID
    content: str | None
    media_path: str | None
    media_type: str | None
    scheduled_at: datetime
    timezone: str
    status: str
    created_at: datetime
    contact_alias: str | None = None
    model_config = {"from_attributes": True}

def _parse_schedule_dt(scheduled_at: str, tz_name: str) -> datetime:
    import zoneinfo
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    local_dt = datetime.fromisoformat(scheduled_at).replace(tzinfo=tz)
    utc_dt = local_dt.astimezone(timezone.utc)
    return utc_dt.replace(tzinfo=None)  # Store as naive UTC for PostgreSQL

@app.post("/api/messages/{contact_id}/schedule", response_model=ScheduledMessageOut)
async def schedule_message(contact_id: UUID, body: ScheduleMessageRequest, user: CurrentUser, db: DB):
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")
    utc_dt = _parse_schedule_dt(body.scheduled_at, body.timezone)
    if utc_dt <= datetime.utcnow():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Время должно быть в будущем")
    sm = ScheduledMessage(
        contact_id=contact_id, content=body.content, scheduled_at=utc_dt,
        timezone=body.timezone, created_by=user.id, org_id=_org_id(user),
    )
    db.add(sm)
    await db.commit()
    await db.refresh(sm)
    out = ScheduledMessageOut.model_validate(sm)
    out.contact_alias = contact.alias
    return out

@app.get("/api/scheduled", response_model=list[ScheduledMessageOut])
async def list_scheduled(user: CurrentUser, db: DB):
    query = select(ScheduledMessage).where(
        ScheduledMessage.org_id == _org_id(user),
        ScheduledMessage.status == "pending",
    ).order_by(ScheduledMessage.scheduled_at)
    result = await db.execute(query)
    items = list(result.scalars().all())
    # Resolve contact aliases
    cids = {s.contact_id for s in items}
    alias_map: dict = {}
    if cids:
        cr = await db.execute(select(Contact.id, Contact.alias).where(Contact.id.in_(cids)))
        alias_map = {r[0]: r[1] for r in cr.all()}
    out = []
    for s in items:
        d = ScheduledMessageOut.model_validate(s)
        d.contact_alias = alias_map.get(s.contact_id, "—")
        out.append(d)
    return out

@app.patch("/api/scheduled/{scheduled_id}", response_model=ScheduledMessageOut)
async def update_scheduled(scheduled_id: UUID, body: ScheduleMessageUpdate, user: CurrentUser, db: DB):
    result = await db.execute(select(ScheduledMessage).where(
        ScheduledMessage.id == scheduled_id, ScheduledMessage.org_id == _org_id(user), ScheduledMessage.status == "pending",
    ))
    sm = result.scalar_one_or_none()
    if not sm:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if body.content is not None:
        sm.content = body.content
    if body.scheduled_at:
        tz_name = body.timezone or sm.timezone
        sm.scheduled_at = _parse_schedule_dt(body.scheduled_at, tz_name)
        if body.timezone:
            sm.timezone = body.timezone
    await db.commit()
    await db.refresh(sm)
    return ScheduledMessageOut.model_validate(sm)

@app.delete("/api/scheduled/{scheduled_id}", status_code=204)
async def cancel_scheduled(scheduled_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(ScheduledMessage).where(
        ScheduledMessage.id == scheduled_id, ScheduledMessage.org_id == _org_id(user), ScheduledMessage.status == "pending",
    ))
    sm = result.scalar_one_or_none()
    if not sm:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    sm.status = "cancelled"
    await db.commit()


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

    check_tg_send_limit(str(contact.tg_account_id), contact.real_tg_id)
    await _validate_upload(file)

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

    # Save file locally. Strip any path components from the user-supplied
    # extension (defence in depth — uuid filename should be safe but the
    # ext could still contain '../' if a client crafted a weird filename).
    ext = os.path.splitext(os.path.basename(file.filename or ""))[1] or ""
    filename = f"{uuid_mod.uuid4()}{ext}"
    media_root = os.path.realpath(MEDIA_DIR)
    filepath = os.path.realpath(os.path.join(media_root, filename))
    if not filepath.startswith(media_root + os.sep):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")
    data = await file.read()
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
    if len(data) > MAX_UPLOAD_SIZE:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "File too large (max 50MB)")
    await asyncio.to_thread(lambda: open(filepath, "wb").write(data))

    # Send via Telethon
    try:
        tg_msg_id = await send_message(
            contact.tg_account_id, contact.real_tg_id,
            text=caption, file_path=filepath,
        )
    except ValueError as e:
        # Clean up saved file on send failure
        if os.path.exists(filepath):
            os.remove(filepath)
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e))

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
    _touch_contact_preview(contact, caption, media_type, "outgoing")
    await db.commit()
    await db.refresh(msg)
    _attach_media_url(msg)
    return msg


@app.post("/api/messages/{contact_id}/send-template-media", response_model=MessageOut)
async def send_template_media(contact_id: UUID, user: CurrentUser, db: DB, template_id: UUID = Query(...)):
    """Send a message using a template's media and text (legacy single-block)."""
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

    check_tg_send_limit(str(contact.tg_account_id), contact.real_tg_id)

    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Template not found")

    file_path = os.path.join(MEDIA_DIR, tpl.media_path) if tpl.media_path else None
    try:
        tg_msg_id = await send_message(
            contact.tg_account_id, contact.real_tg_id,
            text=tpl.content or None,
            file_path=file_path,
            media_type=tpl.media_type,
        )
    except ValueError as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e))

    msg = Message(
        contact_id=contact.id,
        tg_message_id=tg_msg_id,
        direction="outgoing",
        content=tpl.content,
        media_type=tpl.media_type,
        media_path=tpl.media_path,
        sent_by=user.id,
    )
    db.add(msg)
    contact.last_message_at = func.now()
    _touch_contact_preview(contact, tpl.content, tpl.media_type, "outgoing")
    await db.commit()
    await db.refresh(msg)
    _attach_media_url(msg)
    return msg


@app.post("/api/messages/{contact_id}/send-template-block")
async def send_template_single_block(
    contact_id: UUID, user: CurrentUser, db: DB,
    template_id: UUID = Query(...),
    block_index: int = Query(..., description="Index of the block to send"),
):
    """Send a single block from a template."""
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")
    check_tg_send_limit(str(contact.tg_account_id), contact.real_tg_id)
    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id))
    tpl = result.scalar_one_or_none()
    if not tpl or not tpl.blocks_json:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if block_index < 0 or block_index >= len(tpl.blocks_json):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid block index")
    block = tpl.blocks_json[block_index]
    text = block.get("content") or None
    media_type = block.get("media_type") or block.get("type")
    if media_type == "text":
        media_type = None

    # Media group: multiple files in one block
    media_files = block.get("media_files")
    if media_files and len(media_files) > 1:
        from telegram import send_media_group
        # Validate every media_path against MEDIA_DIR to block path traversal
        # via crafted template JSON (e.g. media_path="../../etc/passwd").
        file_paths = [p for p in (_safe_media_path(f.get("path")) for f in media_files) if p]
        if not file_paths:
            return {"status": "skipped"}
        tg_msg_ids = None
        for attempt in range(5):
            try:
                tg_msg_ids = await send_media_group(
                    contact.tg_account_id, contact.real_tg_id,
                    file_paths=file_paths, caption=text,
                )
                break
            except Exception as e:
                if "database is locked" in str(e) and attempt < 4:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e))
        # Save each media as a separate message with same grouped_id
        import random
        album_grouped_id = random.randint(10**15, 10**18)
        msgs = []
        for i, tg_id in enumerate(tg_msg_ids or []):
            mf = media_files[i] if i < len(media_files) else {}
            msg = Message(
                contact_id=contact.id, tg_message_id=tg_id, direction="outgoing",
                content=text if i == 0 else None,
                media_type=mf.get("type", "photo"),
                media_path=mf.get("path"), sent_by=user.id,
                grouped_id=album_grouped_id,
            )
            db.add(msg)
            msgs.append(msg)
        contact.last_message_at = func.now()
        _touch_contact_preview(contact, text, "photo", "outgoing")
        await db.commit()
        for m in msgs:
            await db.refresh(m)
            _attach_media_url(m)
        return msgs[0] if msgs else {"status": "sent"}

    # Single media — validated against MEDIA_DIR via _safe_media_path so
    # a crafted template with media_path="../../etc/passwd" can't be used
    # to ship arbitrary files via Telethon's file= parameter.
    block_media_path = None
    if block.get("media_path"):
        block_media_path = _safe_media_path(block["media_path"])
    elif media_files and len(media_files) == 1:
        block_media_path = _safe_media_path(media_files[0].get("path"))
        media_type = media_files[0].get("type", media_type)

    if not text and not block_media_path:
        return {"status": "skipped"}
    # Retry on SQLite lock
    tg_msg_id = None
    for attempt in range(5):
        try:
            tg_msg_id = await send_message(
                contact.tg_account_id, contact.real_tg_id,
                text=text, file_path=block_media_path,
                media_type=media_type if block_media_path else None,
            )
            break
        except Exception as e:
            if "database is locked" in str(e) and attempt < 4:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e))
    msg = Message(
        contact_id=contact.id, tg_message_id=tg_msg_id, direction="outgoing",
        content=text, media_type=media_type if block_media_path else None,
        media_path=block.get("media_path") or (media_files[0]["path"] if media_files else None),
        sent_by=user.id,
    )
    db.add(msg)
    contact.last_message_at = func.now()
    _touch_contact_preview(contact, text, media_type if block_media_path else None, "outgoing")
    await db.commit()
    await db.refresh(msg)
    _attach_media_url(msg)
    return msg


@app.post("/api/messages/{contact_id}/send-template-blocks")
async def send_template_blocks(contact_id: UUID, user: CurrentUser, db: DB, template_id: UUID = Query(...)):
    """Send all template blocks as a background task.

    Returns immediately with status=sending. The backend handles inter-
    block delays server-side and broadcasts a WS event per block, so the
    CRM UI updates in real-time even if the user navigates away.

    Previous design ran delays in the browser (setTimeout). When the user
    switched chats mid-script, the browser throttled the timers and the
    remaining blocks never sent.
    """
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

    check_tg_send_limit(str(contact.tg_account_id), contact.real_tg_id)

    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Template not found")

    blocks = tpl.blocks_json
    if not blocks:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Template has no blocks")

    # Pre-validate all media paths before spawning the background task.
    media_root = os.path.realpath(MEDIA_DIR)
    validated_blocks: list[dict] = []
    for i, block in enumerate(blocks):
        raw_media = block.get("media_path")
        resolved = None
        if raw_media:
            candidate = os.path.realpath(os.path.join(media_root, raw_media))
            if not candidate.startswith(media_root + os.sep) and candidate != media_root:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Block {i+1}: invalid media path")
            if not os.path.isfile(candidate):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Block {i+1}: media file not found")
            resolved = candidate
        text = block.get("content") or None
        media_type = block.get("media_type") or block.get("type")
        if media_type == "text":
            media_type = None
        if text or resolved:
            validated_blocks.append({
                "text": text,
                "media_type": media_type,
                "media_path_raw": raw_media,
                "media_path_resolved": resolved,
                "delay_after": block.get("delay_after", 0),
            })

    if not validated_blocks:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Template has no sendable blocks")

    # Snapshot values needed by the background task. The DB session will
    # be closed by the time the task runs, so we copy everything.
    contact_id_val = contact.id
    tg_account_id = contact.tg_account_id
    real_tg_id = contact.real_tg_id
    user_id = user.id
    org_id = _org_id(user)

    async def _bg_send():
        """Background: send blocks one by one with delays + WS per block."""
        for i, vb in enumerate(validated_blocks):
            # Inter-block delay (from template JSON)
            if i > 0:
                delay = validated_blocks[i - 1].get("delay_after", 0) or 0
                if delay > 0:
                    await asyncio.sleep(min(delay, 60))  # cap at 60s

            try:
                tg_msg_id = await send_message(
                    tg_account_id, real_tg_id,
                    text=vb["text"],
                    file_path=vb["media_path_resolved"],
                    media_type=vb["media_type"] if vb["media_path_resolved"] else None,
                )
            except Exception as e:
                print(f"[TEMPLATE-BG] Block {i+1} failed for contact {contact_id_val}: {e}")
                continue  # Don't abort the whole chain on one failure

            # Save to DB + broadcast
            async with async_session() as bg_db:
                try:
                    msg = Message(
                        contact_id=contact_id_val,
                        tg_message_id=tg_msg_id,
                        direction="outgoing",
                        content=vb["text"],
                        media_type=vb["media_type"] if vb["media_path_resolved"] else None,
                        media_path=vb["media_path_raw"],
                        sent_by=user_id,
                    )
                    bg_db.add(msg)
                    # Update contact preview
                    c_row = await bg_db.get(Contact, contact_id_val)
                    if c_row:
                        c_row.last_message_at = func.now()
                        _touch_contact_preview(c_row, vb["text"], vb["media_type"], "outgoing")
                    await bg_db.commit()
                    await bg_db.refresh(msg)

                    # WS broadcast so open CRM tabs see the message appear
                    signed_media = _build_media_signed_url(msg.media_path) if msg.media_path else None
                    await ws_manager.broadcast_to_admins({
                        "type": "new_message",
                        "contact_id": str(contact_id_val),
                        "message": {
                            "id": str(msg.id),
                            "tg_message_id": msg.tg_message_id,
                            "direction": "outgoing",
                            "content": msg.content,
                            "media_type": msg.media_type,
                            "media_path": msg.media_path,
                            "media_url": signed_media,
                            "is_deleted": False,
                            "created_at": msg.created_at.isoformat() if msg.created_at else None,
                        },
                    }, org_id=org_id)
                except Exception as e:
                    print(f"[TEMPLATE-BG] DB/WS error block {i+1}: {e}")
                    await bg_db.rollback()

    asyncio.create_task(_bg_send())
    return {"status": "sending", "blocks": len(validated_blocks)}


@app.patch("/api/messages/{contact_id}/read")
async def mark_read(contact_id: UUID, user: CurrentUser, db: DB):
    """Mark all unread incoming messages in a contact chat as read."""
    # Verify contact belongs to user's org
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    from sqlalchemy import update
    await db.execute(
        update(Message)
        .where(Message.contact_id == contact_id, Message.direction == "incoming", Message.is_read.is_(False))
        .values(is_read=True)
    )
    # Flip the denormalized preview flag so the chat-list badge clears
    # on the next /api/contacts fetch.
    if contact.last_message_direction == "incoming":
        contact.last_message_is_read = True
    await db.commit()
    return {"status": "ok"}


@app.get("/api/unread")
async def get_unread_counts(user: CurrentUser, db: DB, tg_account_id: UUID | None = None):
    """Get unread message counts per contact for approved contacts."""
    from sqlalchemy import case
    query = (
        select(
            Message.contact_id,
            func.count(Message.id).label("count"),
        )
        .join(Contact, Contact.id == Message.contact_id)
        .where(
            Message.direction == "incoming",
            Message.is_read.is_(False),
            Contact.status == "approved",
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    if tg_account_id:
        query = query.where(Contact.tg_account_id == tg_account_id)
    query = query.group_by(Message.contact_id)
    result = await db.execute(query)
    return {str(row.contact_id): row.count for row in result.all()}


@app.post("/api/messages/{contact_id}/forward")
async def forward_msg(contact_id: UUID, req: ForwardMessage, user: CurrentUser, db: DB):
    """Forward messages from one contact chat to another."""
    source = await _get_contact_with_access(contact_id, user, db)
    target = await _get_contact_with_access(req.to_contact_id, user, db)

    if target.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Target contact not approved")

    check_tg_send_limit(str(target.tg_account_id), target.real_tg_id)

    # Batch-load source messages (avoid N+1 queries)
    result = await db.execute(
        select(Message).where(Message.id.in_(req.message_ids), Message.contact_id == contact_id)
    )
    all_msgs = {str(m.id): m for m in result.scalars().all()}
    # Preserve original order from request
    src_messages = []
    tg_msg_ids = []
    for msg_id in req.message_ids:
        msg = all_msgs.get(str(msg_id))
        if msg and msg.tg_message_id:
            tg_msg_ids.append(msg.tg_message_id)
            src_messages.append(msg)

    if not tg_msg_ids:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No valid messages to forward")

    fwd_ids = await forward_message(
        source.tg_account_id, source.real_tg_id, tg_msg_ids, target.real_tg_id,
        media_only=req.media_only,
    )

    # Save forwarded messages in CRM — carry over content + media from source
    saved = []
    for i, fwd_tg_id in enumerate(fwd_ids):
        src = src_messages[i] if i < len(src_messages) else None
        fwd_msg = Message(
            contact_id=target.id,
            tg_message_id=fwd_tg_id,
            direction="outgoing",
            content=None if req.media_only else (src.content if src else None),
            media_type=src.media_type if src else None,
            media_path=src.media_path if src else None,
            sent_by=user.id,
            forwarded_from_alias=source.alias,
        )
        db.add(fwd_msg)
        saved.append(fwd_msg)

    target.last_message_at = func.now()
    if saved:
        last = saved[-1]
        _touch_contact_preview(target, last.content, last.media_type, "outgoing")
    await db.commit()
    return {"status": "ok", "forwarded_count": len(fwd_ids)}


@app.delete("/api/messages/{contact_id}/delete/{message_id}")
async def delete_message(contact_id: UUID, message_id: UUID, user: CurrentUser, db: DB):
    """Delete own outgoing message from Telegram and mark as deleted in DB."""
    contact = await _get_contact_with_access(contact_id, user, db)
    result = await db.execute(
        select(Message).where(Message.id == message_id, Message.contact_id == contact_id, Message.direction == "outgoing")
    )
    msg = result.scalar_one_or_none()
    if not msg:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Message not found")
    # Delete from Telegram if possible
    if msg.tg_message_id:
        try:
            from telegram import delete_messages
            await delete_messages(contact.tg_account_id, contact.real_tg_id, [msg.tg_message_id])
        except Exception:
            pass  # best-effort deletion from TG
    msg.is_deleted = True
    await db.commit()
    return {"status": "deleted"}


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


# ============================================================
# Staff
# ============================================================

@app.get("/api/staff/me", response_model=StaffOut)
async def get_me(user: CurrentUser):
    """Return current authenticated staff member."""
    return user


@app.get("/api/chats/bootstrap")
async def chats_bootstrap(
    user: CurrentUser,
    db: Annotated[AsyncSession, Depends(get_db)],
    tg_account_id: UUID | None = Query(None),
):
    """Single round-trip bootstrap for the /chats page.

    Replaces nine separate requests (staff/me, tg/status, pinned, tags,
    templates, unread, scheduled, and the two contacts fetches) with one
    response. Cuts cold-load TTI from ~800-1500ms to ~150-300ms on
    mobile/high-latency links and removes a user-visible waterfall.

    Scoping/filters MUST match the per-endpoint versions exactly: same
    operator phone masking, same tag/template visibility rules, same
    unread approved-contact filter. Discrepancies cause badge flicker
    and — worse — accidental data leaks.
    """
    from schemas import StaffOut, TagOut, TemplateOut
    from sqlalchemy import or_ as _or
    from telegram import _clients as _tg_clients

    org = _org_id(user)
    is_operator = user.role not in ("super_admin", "admin")
    assigned_subq = None
    if is_operator:
        assigned_subq = select(StaffTgAccount.tg_account_id).where(
            StaffTgAccount.staff_id == user.id
        )

    # --- TG accounts (mirror /api/tg/status behaviour) ---
    acct_query = select(TgAccount).where(
        TgAccount.is_active.is_(True),
        TgAccount.org_id == org,
    )
    if is_operator:
        acct_query = acct_query.where(TgAccount.id.in_(assigned_subq))
    acct_query = acct_query.order_by(TgAccount.connected_at)
    acct_rows = await db.execute(acct_query)
    tg_accounts = list(acct_rows.scalars().all())
    accounts_out = []
    for a in tg_accounts:
        # Operators must not see the raw phone — show the masked form
        # `••••1234` identical to /api/tg/status.
        phone = a.phone
        if is_operator and phone:
            phone = "••••" + (phone[-4:] if len(phone) >= 4 else "")
        client = _tg_clients.get(a.id)
        accounts_out.append({
            "id": str(a.id),
            "phone": phone,
            "display_name": a.display_name,
            "connected_at": a.connected_at.isoformat() if a.connected_at else None,
            "show_real_names": a.show_real_names,
            "is_active": a.is_active,
            "connected": bool(client and client.is_connected()),
        })

    # --- Pinned chat ids ---
    pin_query = (
        select(PinnedChat.contact_id)
        .join(Contact, Contact.id == PinnedChat.contact_id)
        .where(
            PinnedChat.org_id == org,
            Contact.is_archived.is_(False),
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    if is_operator:
        pin_query = pin_query.where(Contact.tg_account_id.in_(assigned_subq))
    pin_rows = await db.execute(pin_query)
    pinned = [str(row[0]) for row in pin_rows.all()]

    # --- Tags (mirror /api/tags exactly) ---
    tag_query = select(Tag).where(Tag.org_id == org)
    if tg_account_id:
        tag_query = tag_query.where(_or(
            Tag.tg_account_id == tg_account_id,
            Tag.tg_account_id.is_(None),
        ))
    if is_operator:
        tag_query = tag_query.where(_or(
            Tag.tg_account_id.in_(assigned_subq),
            Tag.tg_account_id.is_(None),
        ))
    tag_rows = await db.execute(tag_query.order_by(Tag.name))
    tags = list(tag_rows.scalars().all())

    # --- Templates (mirror /api/templates exactly) ---
    tpl_query = select(MessageTemplate).where(MessageTemplate.org_id == org)
    if tg_account_id:
        tpl_query = tpl_query.where(_or(
            MessageTemplate.tg_account_id == tg_account_id,
            MessageTemplate.tg_account_id.is_(None),
        ))
    if is_operator:
        tpl_query = tpl_query.where(_or(
            MessageTemplate.tg_account_id.in_(assigned_subq),
            MessageTemplate.tg_account_id.is_(None),
        ))
    tpl_rows = await db.execute(tpl_query.order_by(MessageTemplate.created_at))
    templates = list(tpl_rows.scalars().all())
    # Resolve created_by_name (same as /api/templates)
    creator_ids = {t.created_by for t in templates if t.created_by}
    creator_names: dict = {}
    if creator_ids:
        staff_rows = await db.execute(select(Staff.id, Staff.name).where(Staff.id.in_(creator_ids)))
        creator_names = {r[0]: r[1] for r in staff_rows.all()}

    # --- Unread counts (mirror /api/unread — status='approved' filter!) ---
    unread_q = (
        select(Message.contact_id, func.count(Message.id))
        .join(Contact, Contact.id == Message.contact_id)
        .where(
            Message.direction == "incoming",
            Message.is_read.is_(False),
            Contact.status == "approved",
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
        .group_by(Message.contact_id)
    )
    if tg_account_id:
        unread_q = unread_q.where(Contact.tg_account_id == tg_account_id)
    if is_operator:
        unread_q = unread_q.where(Contact.tg_account_id.in_(assigned_subq))
    unread_rows = await db.execute(unread_q)
    unread = {str(row[0]): row[1] for row in unread_rows.all()}

    # --- Scheduled messages (pending only) ---
    sched_q = (
        select(ScheduledMessage)
        .where(
            ScheduledMessage.org_id == org,
            ScheduledMessage.status == "pending",
        )
        .order_by(ScheduledMessage.scheduled_at)
    )
    sched_rows = await db.execute(sched_q)
    sched_list = list(sched_rows.scalars().all())
    alias_map: dict = {}
    if sched_list:
        cids = {s.contact_id for s in sched_list}
        alias_rows = await db.execute(select(Contact.id, Contact.alias).where(Contact.id.in_(cids)))
        alias_map = {r[0]: r[1] for r in alias_rows.all()}

    from pydantic import TypeAdapter
    tag_ta = TypeAdapter(list[TagOut])

    # Templates: serialize via TemplateOut + attach created_by_name so
    # the response is indistinguishable from /api/templates.
    templates_out = []
    for t in templates:
        data = TemplateOut.model_validate(t)
        data.created_by_name = creator_names.get(t.created_by)
        templates_out.append(data.model_dump(mode="json"))

    return {
        "staff": StaffOut.model_validate(user).model_dump(mode="json"),
        "accounts": accounts_out,
        "pinned": pinned,
        "tags": tag_ta.dump_python(tags, mode="json"),
        "templates": templates_out,
        "unread": unread,
        "scheduled": [
            {
                "id": str(s.id),
                "contact_id": str(s.contact_id),
                "content": s.content,
                "media_path": s.media_path,
                "media_type": s.media_type,
                "scheduled_at": s.scheduled_at.isoformat() if s.scheduled_at else None,
                "timezone": s.timezone,
                "status": s.status,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "contact_alias": alias_map.get(s.contact_id, "—"),
            }
            for s in sched_list
        ],
    }


@app.get("/api/staff", response_model=list[StaffOut])
async def list_staff(user: CurrentUser, db: DB):
    result = await db.execute(
        select(Staff).where(Staff.postforge_org_id == _org_id(user)).order_by(Staff.created_at)
    )
    return result.scalars().all()


@app.post("/api/staff/invite", response_model=BotInviteOut)
async def create_invite(req: BotInviteCreate, user: AdminUser, db: DB):
    code = secrets.token_urlsafe(16)[:20]
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
    result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.postforge_org_id == _org_id(user)))
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
    if req.signature_mode is not None:
        if req.signature_mode not in ("named", "anonymous"):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "signature_mode must be 'named' or 'anonymous'")
        target.signature_mode = req.signature_mode

    await db.commit()
    await db.refresh(target)
    return target


@app.get("/api/staff/{staff_id}/accounts")
async def get_staff_accounts(staff_id: UUID, user: AdminUser, db: DB):
    """Get TG accounts assigned to a staff member."""
    # Verify staff belongs to same org
    result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.postforge_org_id == _org_id(user)))
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    result = await db.execute(
        select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == staff_id)
    )
    return [str(row[0]) for row in result.all()]


@app.put("/api/staff/{staff_id}/accounts")
async def set_staff_accounts(staff_id: UUID, account_ids: list[UUID], user: AdminUser, db: DB):
    """Set TG accounts for a staff member (replace all)."""
    org = _org_id(user)
    # Deduplicate to prevent false count mismatches and IntegrityError on insert.
    account_ids = list(set(account_ids))

    # Verify staff exists in same org
    result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.postforge_org_id == org))
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # CRITICAL: verify ALL accounts belong to the SAME org.
    # Without this check an admin from Org A could assign Org B's TG accounts
    # to their own staff and gain access to Org B's chats — a full data breach.
    if account_ids:
        from sqlalchemy import func as sa_func
        valid_count = (await db.execute(
            select(sa_func.count()).select_from(TgAccount).where(
                TgAccount.id.in_(account_ids),
                TgAccount.org_id == org,
            )
        )).scalar() or 0
        if valid_count != len(account_ids):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "One or more accounts do not belong to your organization",
            )

    # Delete existing assignments
    from sqlalchemy import delete
    await db.execute(delete(StaffTgAccount).where(StaffTgAccount.staff_id == staff_id))

    # Create new assignments
    for acc_id in account_ids:
        db.add(StaffTgAccount(staff_id=staff_id, tg_account_id=acc_id))

    _audit(db, user, "assign_accounts", target_id=str(staff_id), target_type="staff",
           metadata={"account_ids": [str(a) for a in account_ids]})
    await db.commit()
    return {"status": "ok", "account_ids": [str(a) for a in account_ids]}


@app.delete("/api/staff/{staff_id}")
async def deactivate_staff(staff_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.postforge_org_id == _org_id(user)))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if target.role == "super_admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Cannot deactivate super admin")
    target.is_active = False
    _audit(db, user, "deactivate_staff", target_id=str(staff_id), target_type="staff",
           metadata={"name": target.name, "role": target.role})
    await db.commit()
    return {"status": "deactivated"}


# ============================================================
# Tags
# ============================================================

@app.get("/api/tags", response_model=list[TagOut])
async def list_tags(user: CurrentUser, db: DB, tg_account_id: UUID | None = None):
    from sqlalchemy import or_
    query = select(Tag).where(Tag.org_id == _org_id(user))
    if tg_account_id:
        query = query.where(or_(Tag.tg_account_id == tg_account_id, Tag.tg_account_id.is_(None)))
    # Operators only see tags for their assigned accounts
    if user.role not in ("super_admin", "admin"):
        assigned = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(or_(Tag.tg_account_id.in_(assigned), Tag.tg_account_id.is_(None)))
    query = query.order_by(Tag.name)
    result = await db.execute(query)
    return result.scalars().all()


@app.post("/api/tags", response_model=TagOut)
async def create_tag(req: TagCreate, user: ContentManager, db: DB):
    tag = Tag(name=req.name, color=req.color, created_by=user.id, org_id=_org_id(user), tg_account_id=req.tg_account_id)
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return tag


@app.delete("/api/tags/{tag_id}")
async def delete_tag(tag_id: UUID, user: ContentManager, db: DB):
    result = await db.execute(select(Tag).where(Tag.id == tag_id, Tag.org_id == _org_id(user)))
    tag = result.scalar_one_or_none()
    if not tag:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    # Remove tag from all contacts in one SQL statement.
    # contacts.tags is VARCHAR[]; asyncpg infers the array literal as TEXT[]
    # and PG has no VARCHAR[] @> TEXT[] operator, so we cast explicitly.
    # Using `= ANY(tags)` instead of `@> ARRAY[...]` avoids the issue
    # entirely (ANY() unwraps the array element per-row).
    tag_name = tag.name
    from sqlalchemy import text as _text
    await db.execute(_text(
        "UPDATE contacts SET tags = array_remove(tags, CAST(:tag_name AS varchar)) "
        "WHERE CAST(:tag_name AS varchar) = ANY(tags)"
    ), {"tag_name": tag_name})
    await db.delete(tag)
    await db.commit()
    return {"status": "deleted"}


# ============================================================
# Message Templates
# ============================================================

@app.get("/api/templates", response_model=list[TemplateOut])
async def list_templates(user: CurrentUser, db: DB, tg_account_id: UUID | None = None):
    from sqlalchemy import or_
    query = select(MessageTemplate).where(MessageTemplate.org_id == _org_id(user))
    if tg_account_id:
        query = query.where(or_(MessageTemplate.tg_account_id == tg_account_id, MessageTemplate.tg_account_id.is_(None)))
    # Operators only see templates for their assigned accounts
    if user.role not in ("super_admin", "admin"):
        assigned = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        query = query.where(or_(MessageTemplate.tg_account_id.in_(assigned), MessageTemplate.tg_account_id.is_(None)))
    query = query.order_by(MessageTemplate.created_at)
    result = await db.execute(query)
    templates = result.scalars().all()
    # Resolve creator names
    creator_ids = {t.created_by for t in templates if t.created_by}
    staff_names: dict = {}
    if creator_ids:
        staff_result = await db.execute(select(Staff).where(Staff.id.in_(creator_ids)))
        staff_names = {s.id: s.name for s in staff_result.scalars().all()}
    out = []
    for t in templates:
        data = TemplateOut.model_validate(t)
        data.created_by_name = staff_names.get(t.created_by)
        out.append(data)
    return out


@app.post("/api/templates", response_model=TemplateOut)
async def create_template(req: TemplateCreate, user: ContentManager, db: DB):
    blocks = [b.model_dump() for b in req.blocks_json] if req.blocks_json else None
    # Build legacy content from blocks for backward compat
    content = req.content or ""
    if blocks and not content:
        text_parts = [b["content"] for b in blocks if b.get("content")]
        content = "\n---\n".join(text_parts) if text_parts else "(media)"
    tpl = MessageTemplate(
        title=req.title,
        content=content,
        category=req.category,
        shortcut=req.shortcut,
        tg_account_id=req.tg_account_id,
        blocks_json=blocks,
        created_by=user.id,
        org_id=_org_id(user),
    )
    db.add(tpl)
    await db.commit()
    await db.refresh(tpl)
    return tpl


@app.patch("/api/templates/{template_id}", response_model=TemplateOut)
async def update_template(template_id: UUID, req: TemplateUpdate, user: ContentManager, db: DB):
    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    data = req.model_dump(exclude_unset=True)
    if "blocks_json" in data and data["blocks_json"] is not None:
        data["blocks_json"] = [b.model_dump() if hasattr(b, "model_dump") else b for b in data["blocks_json"]]
        # Update legacy content from blocks
        text_parts = [b["content"] for b in data["blocks_json"] if b.get("content")]
        data["content"] = "\n---\n".join(text_parts) if text_parts else "(media)"
    for field, val in data.items():
        setattr(tpl, field, val)
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(tpl, "blocks_json")
    await db.commit()
    await db.refresh(tpl)
    return tpl


@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: UUID, user: ContentManager, db: DB):
    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    await db.delete(tpl)
    await db.commit()
    return {"status": "deleted"}


@app.post("/api/templates/{template_id}/upload-media")
async def template_upload_media(
    template_id: UUID,
    user: ContentManager,
    db: DB,
    file: UploadFile = File(...),
    send_as: str = Query("auto", description="auto|photo|video|video_note|voice|document"),
):
    await _validate_upload(file)

    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    ext = os.path.splitext(os.path.basename(file.filename or ""))[1].lower()
    filename = f"template_{template_id}{ext}"
    filepath = os.path.join(MEDIA_DIR, filename)
    filepath = os.path.abspath(filepath)
    if not filepath.startswith(os.path.abspath(MEDIA_DIR)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")

    content = await file.read()
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "File too large (max 50MB)")
    await asyncio.to_thread(lambda: open(filepath, "wb").write(content))

    if send_as != "auto":
        media_type = send_as
    else:
        ct = (file.content_type or "").lower()
        if "image" in ct:
            media_type = "photo"
        elif "video" in ct:
            media_type = "video"
        elif "audio" in ct or "ogg" in ct:
            media_type = "voice"
        else:
            media_type = "document"

    # Convert video to circle
    if media_type == "video_note":
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"template_{template_id}_circle.mp4")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-t", "60", "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=640:640",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-c:a", "aac", "-b:a", "128k",
                "-f", "mp4", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"template_{template_id}_circle.mp4"
        except Exception:
            media_type = "video"

    # Convert audio to voice
    if media_type == "voice" and ext not in (".ogg", ".oga"):
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"template_{template_id}_voice.ogg")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-c:a", "libopus", "-b:a", "64k", "-f", "ogg", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"template_{template_id}_voice.ogg"
        except Exception:
            media_type = "document"

    tpl.media_type = media_type
    tpl.media_path = filename
    await db.commit()
    return {"media_path": filename, "media_type": media_type}


@app.post("/api/templates/{template_id}/upload-block-media")
async def template_upload_block_media(
    template_id: UUID,
    user: ContentManager,
    db: DB,
    file: UploadFile = File(...),
    block_id: str = Query(..., description="Block ID within the template"),
    send_as: str = Query("auto", description="auto|photo|video|video_note|voice|document"),
):
    """Upload media for a specific block within a template."""
    _validate_block_id(block_id)
    await _validate_upload(file)

    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    ext = os.path.splitext(os.path.basename(file.filename or ""))[1].lower()
    filename = f"tplblock_{template_id}_{block_id}{ext}"
    filepath = os.path.join(MEDIA_DIR, filename)
    filepath = os.path.abspath(filepath)
    if not filepath.startswith(os.path.abspath(MEDIA_DIR)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")

    content = await file.read()
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "File too large (max 50MB)")
    await asyncio.to_thread(lambda: open(filepath, "wb").write(content))

    if send_as != "auto":
        media_type = send_as
    else:
        ct = (file.content_type or "").lower()
        if "image" in ct:
            media_type = "photo"
        elif "video" in ct:
            media_type = "video"
        elif "audio" in ct or "ogg" in ct:
            media_type = "voice"
        else:
            media_type = "document"

    # Convert video to circle
    if media_type == "video_note":
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"tplblock_{template_id}_{block_id}_circle.mp4")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-t", "60", "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=640:640",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-c:a", "aac", "-b:a", "128k",
                "-f", "mp4", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"tplblock_{template_id}_{block_id}_circle.mp4"
        except Exception:
            media_type = "video"

    # Convert audio to voice (OGG opus)
    if media_type == "voice" and ext not in (".ogg", ".oga"):
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"tplblock_{template_id}_{block_id}_voice.ogg")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-c:a", "libopus", "-b:a", "64k", "-f", "ogg", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"tplblock_{template_id}_{block_id}_voice.ogg"
        except Exception:
            media_type = "document"

    return {"media_path": filename, "media_type": media_type, "block_id": block_id}


@app.delete("/api/templates/{template_id}/block-media/{block_id}")
async def template_delete_block_media(template_id: UUID, block_id: str, user: ContentManager, db: DB):
    """Delete media file for a specific block."""
    _validate_block_id(block_id)
    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    # Remove matching files
    import glob as _glob
    pattern = os.path.join(MEDIA_DIR, f"tplblock_{template_id}_{block_id}*")
    for f in _glob.glob(pattern):
        try:
            os.remove(f)
        except Exception:
            pass
    return {"status": "deleted"}


# ============================================================
# Archive
# ============================================================

@app.post("/api/contacts/{contact_id}/archive")
async def archive_contact(contact_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    contact.is_archived = True
    await db.commit()
    await cache_invalidate(f"contacts:{_org_id(user)}:*")
    return {"status": "archived"}


@app.post("/api/contacts/{contact_id}/unarchive")
async def unarchive_contact(contact_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    contact.is_archived = False
    await db.commit()
    await cache_invalidate(f"contacts:{_org_id(user)}:*")
    return {"status": "unarchived"}


async def _set_contact_mute(contact_id: UUID, user: Staff, db, muted: bool) -> dict:
    """Shared body for /mute and /unmute. Resolves the contact, pushes the
    new state to Telegram via Telethon, then mirrors it to `Contact.is_muted`.

    Raises 404 if the contact isn't in the user's org, 400 if the Telegram
    call fails (e.g. account disconnected). `crm_muted` is kept in sync with
    `is_muted` to avoid two-source drift — effectively a single-layer mute.
    """
    from telegram import set_chat_mute
    result = await db.execute(select(Contact).where(
        Contact.id == contact_id,
        Contact.tg_account_id.in_(_org_accounts_subq(user)),
    ))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    try:
        await set_chat_mute(contact.tg_account_id, contact.real_tg_id, muted)
    except ValueError as e:
        # Account not connected
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))
    except Exception as e:
        # Telethon / network / flood
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Telegram rejected the mute change: {e}")

    contact.is_muted = muted
    contact.crm_muted = muted
    await db.commit()
    await cache_invalidate(f"contacts:{_org_id(user)}:*")
    return {"status": "muted" if muted else "unmuted"}


@app.post("/api/contacts/{contact_id}/mute")
async def mute_contact(contact_id: UUID, user: CurrentUser, db: DB):
    """Mute a chat in the user's native Telegram client (via Telethon)
    and mirror the state to Contact.is_muted. Silences in-CRM toasts.
    """
    return await _set_contact_mute(contact_id, user, db, muted=True)


@app.post("/api/contacts/{contact_id}/unmute")
async def unmute_contact(contact_id: UUID, user: CurrentUser, db: DB):
    """Unmute a chat in native Telegram + clear Contact.is_muted.
    Works regardless of whether the chat was originally muted from CRM
    or from the Telegram app — the CRM now drives the real TG state.
    """
    return await _set_contact_mute(contact_id, user, db, muted=False)


# ============================================================
# Avatars (proxy Telegram profile photos)
# ============================================================

def _build_avatar_signed_url(contact_id: UUID, tg_account_id: UUID, ttl_seconds: int = 86400) -> str:
    """Build an HMAC-signed avatar URL with a 24h expiry by default.

    The HMAC payload binds `contact_id`, `tg_account_id`, and `expires`.
    Binding to tg_account_id means a URL forged/tampered to point at a
    different contact UUID or the same UUID under a different account
    won't verify — defence in depth against UUID-leak scenarios.
    """
    expires = int(time.time()) + ttl_seconds
    payload = f"{contact_id}:{tg_account_id}:{expires}"
    sig = hmac.new(settings.JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    return f"/api/contacts/{contact_id}/avatar?expires={expires}&sig={sig}"


@app.post("/api/contacts/{contact_id}/refresh-avatar", status_code=204)
async def refresh_contact_avatar(
    contact_id: UUID,
    user: CurrentUser,
    db: Annotated[AsyncSession, Depends(get_db)],
):
    """Manually invalidate the cached avatar file for a contact. The next
    /avatar request re-downloads from Telegram. Use this when a user wants
    to see an updated profile photo without waiting for the periodic sync.
    """
    result = await db.execute(
        select(Contact.id).where(
            Contact.id == contact_id,
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    avatar_path = os.path.join(MEDIA_DIR, "avatars", f"{contact_id}.jpg")
    try:
        if os.path.exists(avatar_path):
            os.remove(avatar_path)
    except Exception:
        pass
    return


@app.get("/api/contacts/{contact_id}/avatar-url")
async def get_avatar_signed_url(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    """Return a signed, time-limited URL for a contact's avatar.

    Frontend calls this once and uses the signed URL in <img src>.
    The signed URL doesn't contain the JWT — it uses an HMAC signature
    with a 1-hour expiry, so even if leaked it's short-lived and
    can't be used for anything except viewing this specific avatar.
    """
    # Verify contact belongs to user's org and load its tg_account_id
    # so the signature payload matches what the serve endpoint expects.
    result = await db.execute(
        select(Contact.id, Contact.tg_account_id).where(
            Contact.id == contact_id,
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    row = result.first()
    if not row:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    return {"url": _build_avatar_signed_url(contact_id, row[1])}


@app.get("/api/contacts/{contact_id}/avatar")
async def get_contact_avatar(
    contact_id: UUID,
    request: Request,
    db: DB,
    expires: int = Query(0),
    sig: str = Query(""),
    token: str = Query(""),  # Legacy compat — remove after frontend deploys
):
    """Serve avatar image. Accepts either HMAC signed URL or legacy JWT token."""
    from fastapi.responses import FileResponse, Response

    if sig and expires:
        # Signed URL auth (preferred — no JWT in URL)
        if time.time() > expires:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Link expired")
        # Load contact FIRST so we can bind the HMAC payload to the
        # contact's tg_account_id. This prevents URL tampering (changing
        # contact_id in the URL): the resulting HMAC wouldn't match
        # because the loaded contact's tg_account_id differs.
        result = await db.execute(select(Contact).where(Contact.id == contact_id))
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status.HTTP_404_NOT_FOUND)
        payload = f"{contact_id}:{contact.tg_account_id}:{expires}"
        expected = hmac.new(settings.JWT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(sig, expected):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid signature")
    elif token:
        # Legacy: JWT in query param (backward compat during frontend rollout)
        payload_jwt = decode_token(token)
        if payload_jwt.get("type") != "access":
            raise HTTPException(status.HTTP_401_UNAUTHORIZED)
        staff_id = payload_jwt.get("sub")
        staff_result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True)))
        staff_user = staff_result.scalar_one_or_none()
        if not staff_user:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED)
        result = await db.execute(
            select(Contact).where(
                Contact.id == contact_id,
                Contact.tg_account_id.in_(_org_accounts_subq(staff_user)),
            )
        )
        contact = result.scalar_one_or_none()
        if not contact:
            raise HTTPException(status.HTTP_404_NOT_FOUND)
    else:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED)

    avatar_dir = os.path.join(MEDIA_DIR, "avatars")
    os.makedirs(avatar_dir, exist_ok=True)
    avatar_path = os.path.join(avatar_dir, f"{contact_id}.jpg")

    # Avatar caching TTL. Short enough that a profile-photo change
    # propagates within a few hours even without sync invalidation, long
    # enough that we don't hammer Telegram's API. Proactive invalidation
    # in _do_sync_dialogs handles the fast path (~2h worst case after
    # photo change), this is the fallback.
    _AVATAR_DISK_TTL = 6 * 3600       # 6 hours
    _AVATAR_BROWSER_TTL = 2 * 3600    # 2 hours — browsers refetch after this

    def _serve(path: str) -> Response:
        """Serve cached avatar with HTTP caching headers + ETag negotiation."""
        mtime = int(os.path.getmtime(path))
        etag = f'W/"{contact_id}-{mtime}"'
        cache_control = f"private, max-age={_AVATAR_BROWSER_TTL}"
        # 304 Not Modified shortcut — browser already has a fresh copy
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={
                "ETag": etag,
                "Cache-Control": cache_control,
            })
        return FileResponse(
            path,
            media_type="image/jpeg",
            headers={"ETag": etag, "Cache-Control": cache_control},
        )

    # Check server-side cache; refresh from Telegram if stale.
    if os.path.exists(avatar_path):
        age = time.time() - os.path.getmtime(avatar_path)
        if age < _AVATAR_DISK_TTL:
            return _serve(avatar_path)

    # Download from Telegram — use SMALL version (160x160, ~8-15KB) instead of
    # the default big version (640x640, ~80KB). Avatars are displayed at 32x32
    # in the chat list, so the small variant is more than enough and ~10x
    # faster to download from Telegram's CDN.
    from telegram import _clients
    client = _clients.get(contact.tg_account_id)
    if not client:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not connected")

    try:
        photo = await client.download_profile_photo(
            contact.real_tg_id, file=avatar_path, download_big=False,
        )
        if photo:
            return _serve(avatar_path)
    except Exception:
        pass

    raise HTTPException(status.HTTP_404_NOT_FOUND, "No avatar")


# ============================================================
# Message Editing
# ============================================================

@app.patch("/api/messages/{contact_id}/{message_id}/edit")
async def edit_message(contact_id: UUID, message_id: UUID, req: SendMessage, user: CurrentUser, db: DB):
    """Edit a previously sent outgoing message."""
    # Verify contact belongs to user's org
    contact = await _get_contact_with_access(contact_id, user, db)

    result = await db.execute(select(Message).where(Message.id == message_id, Message.contact_id == contact_id))
    msg = result.scalar_one_or_none()
    if not msg:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if msg.direction != "outgoing":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Can only edit outgoing messages")
    if not msg.tg_message_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No Telegram message ID")

    # Edit on Telegram
    from telegram import _clients
    client = _clients.get(contact.tg_account_id)
    if not client:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Account not connected")

    try:
        await client.edit_message(contact.real_tg_id, msg.tg_message_id, req.content)
    except Exception as e:
        err = str(e).lower()
        if "not modified" in err:
            pass  # Content unchanged — save locally without error
        elif "too much time" in err or "time has passed" in err:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Сообщение слишком старое для редактирования (лимит Telegram — 48 часов)")
        else:
            import logging
            logging.getLogger(__name__).error(f"Edit failed: {e}")
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Ошибка редактирования: {str(e)[:100]}")

    # Save edit history before updating
    old_content = msg.content
    history = MessageEditHistory(
        message_id=msg.id,
        old_content=old_content,
        new_content=req.content,
    )
    db.add(history)

    msg.content = req.content
    msg.is_edited = True
    await db.commit()
    return {"status": "edited"}


@app.get("/api/messages/{contact_id}/{message_id}/edit-history", response_model=list[MessageEditHistoryOut])
async def get_edit_history(contact_id: UUID, message_id: UUID, user: CurrentUser, db: DB):
    """Get edit history for a message."""
    # Verify contact belongs to user's org
    await _get_contact_with_access(contact_id, user, db)

    # Verify message belongs to contact
    result = await db.execute(select(Message).where(Message.id == message_id, Message.contact_id == contact_id))
    msg = result.scalar_one_or_none()
    if not msg:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    result = await db.execute(
        select(MessageEditHistory)
        .where(MessageEditHistory.message_id == message_id)
        .order_by(MessageEditHistory.edited_at)
    )
    return result.scalars().all()


# ============================================================
# Translation
# ============================================================

# Per-user translate rate limit state. In-memory — fine for single-worker
# deploys, must migrate to Redis when we scale out.
_translate_limits: dict[str, list[float]] = defaultdict(list)
TRANSLATE_MAX_CHARS = 4000
TRANSLATE_RATE_WINDOW = 300  # 5 minutes
TRANSLATE_RATE_MAX = 30
ALLOWED_TRANSLATE_LANGS = {
    "en", "ru", "uk", "es", "fr", "de", "it", "pt", "zh", "ja", "ko",
    "ar", "tr", "pl", "nl", "sv", "cs", "hu", "ro", "bg", "el", "he",
    "hi", "th", "vi", "id", "ms", "fa", "da", "fi", "no",
}


@app.post("/api/translate")
async def translate_text(req: TranslateRequest, user: CurrentUser):
    """Translate text using free Google Translate (via googletrans-like API).

    Hardened against abuse: length capped, per-user rate-limited, target
    language whitelisted. Without these, an authenticated user could bulk-
    exfiltrate chat content through Google and get the CRM IP banned.
    """
    # Length cap: 4000 chars is below Google's free-API 5k limit and well
    # above any legitimate chat message.
    if not req.text or not req.text.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Text is empty")
    if len(req.text) > TRANSLATE_MAX_CHARS:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Text too long (max {TRANSLATE_MAX_CHARS} chars)",
        )
    if req.target_lang not in ALLOWED_TRANSLATE_LANGS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unsupported target language")

    # Per-user sliding-window rate limit (30 translations / 5 min).
    now = time.time()
    key = str(user.id)
    _translate_limits[key] = [t for t in _translate_limits[key] if now - t < TRANSLATE_RATE_WINDOW]
    if len(_translate_limits[key]) >= TRANSLATE_RATE_MAX:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Слишком много запросов на перевод. Попробуйте через несколько минут.",
        )
    _translate_limits[key].append(now)

    # Graceful degradation: the unofficial Google Translate endpoint
    # rate-limits / IP-blocks by region. When it fails we return the
    # original text with a `failed: true` flag instead of 502, so the
    # UI can show a subtle "перевод недоступен" badge without a red toast.
    import httpx
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://translate.googleapis.com/translate_a/single",
                params={
                    "client": "gtx",
                    "sl": "auto",
                    "tl": req.target_lang,
                    "dt": "t",
                    "q": req.text,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                translated = "".join(part[0] for part in data[0] if part[0])
                detected_lang = data[2] if len(data) > 2 else "unknown"
                # If detected language matches target, flip to English or Russian
                if isinstance(detected_lang, str) and detected_lang == req.target_lang:
                    alt_lang = "en" if req.target_lang != "en" else "ru"
                    resp2 = await client.get(
                        "https://translate.googleapis.com/translate_a/single",
                        params={"client": "gtx", "sl": "auto", "tl": alt_lang, "dt": "t", "q": req.text},
                    )
                    if resp2.status_code == 200:
                        data2 = resp2.json()
                        translated = "".join(part[0] for part in data2[0] if part[0])
                        return {"translated": translated, "detected_lang": detected_lang}
                return {"translated": translated, "detected_lang": detected_lang}
            _log.warning(f"Translation upstream {resp.status_code}: {resp.text[:120]}")
    except Exception as e:
        _log.error(f"Translation upstream error: {e}")
    return {
        "translated": req.text,
        "detected_lang": "unknown",
        "failed": True,
        "reason": "upstream_unavailable",
    }


# ============================================================
# Broadcasts
# ============================================================

async def _broadcast_to_out(bc: Broadcast, db) -> BroadcastOut:
    """Convert a Broadcast row to BroadcastOut, resolving the human-
    readable account label and creator name in one shot. Used by every
    endpoint that returns a single broadcast or a list — keeps the UI
    free of UUID-only displays.

    Looks up TgAccount.phone/display_name and Staff.name via two simple
    PK fetches (cached via session identity-map on a list call where the
    same staff/account is referenced repeatedly). Returns None for the
    label fields when the FK target is missing — UI falls back to "—".
    """
    tg_phone = None
    tg_display = None
    if bc.tg_account_id:
        acc = await db.get(TgAccount, bc.tg_account_id)
        if acc is not None:
            tg_phone = acc.phone
            tg_display = acc.display_name
    creator_name = None
    if bc.created_by:
        st = await db.get(Staff, bc.created_by)
        if st is not None:
            creator_name = st.name
    out = BroadcastOut.model_validate(bc)
    out.tg_account_phone = tg_phone
    out.tg_account_display_name = tg_display
    out.created_by_name = creator_name
    return out


@app.get("/api/broadcasts", response_model=list[BroadcastOut])
async def list_broadcasts(user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.org_id == _org_id(user)).order_by(Broadcast.created_at.desc()))
    rows = result.scalars().all()
    return [await _broadcast_to_out(bc, db) for bc in rows]


@app.post("/api/broadcasts", response_model=BroadcastOut)
async def create_broadcast(req: BroadcastCreate, user: CurrentUser, db: DB):
    bc = Broadcast(
        title=req.title,
        content=req.content,
        tg_account_id=req.tg_account_id,
        tag_filter=req.tag_filter,
        tag_exclude=req.tag_exclude or [],
        include_archived=bool(req.include_archived),
        # Floor raised from 1s → 5s: with the per-account system throttle
        # already capped at ~1 msg/sec, a 1-second user delay meant every
        # broadcast ran right at the Telegram flood threshold. 5 s gives
        # breathing room and halves the flood-ban risk. Older drafts in
        # the DB (delay=1..4) are grandfathered — they keep their value
        # until the user opens them in the editor, which clamps up.
        delay_seconds=max(5, min(3600, req.delay_seconds)),
        max_recipients=req.max_recipients,
        contact_ids=req.contact_ids or [],
        created_by=user.id,
        org_id=_org_id(user),
    )
    db.add(bc)
    await db.commit()
    await db.refresh(bc)
    return await _broadcast_to_out(bc, db)


@app.post("/api/broadcasts/{broadcast_id}/upload-media")
async def broadcast_upload_media(
    broadcast_id: UUID,
    user: CurrentUser,
    db: DB,
    file: UploadFile = File(...),
    send_as: str = Query("auto", description="auto|photo|video|video_note|voice|document"),
):
    await _validate_upload(file)

    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    ext = os.path.splitext(os.path.basename(file.filename or ""))[1].lower()
    filename = f"broadcast_{broadcast_id}{ext}"
    filepath = os.path.join(MEDIA_DIR, filename)
    filepath = os.path.abspath(filepath)
    if not filepath.startswith(os.path.abspath(MEDIA_DIR)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")

    content = await file.read()
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "File too large (max 50MB)")
    await asyncio.to_thread(lambda: open(filepath, "wb").write(content))

    # Auto-detect or use explicit type
    if send_as != "auto":
        media_type = send_as
    else:
        ct = (file.content_type or "").lower()
        if "image" in ct:
            media_type = "photo"
        elif "video" in ct:
            media_type = "video"
        elif "audio" in ct or "ogg" in ct:
            media_type = "voice"
        else:
            media_type = "document"

    # Convert video to circle (video_note) — crop to square, max 60s, no audio
    if media_type == "video_note":
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"broadcast_{broadcast_id}_circle.mp4")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-t", "60",
                "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=640:640",
                "-c:v", "libx264", "-preset", "fast", "-crf", "28",
                "-an",  # no audio for video notes
                "-f", "mp4", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"broadcast_{broadcast_id}_circle.mp4"
        except Exception as e:
            print(f"[FFMPEG] video_note conversion failed: {e}")
            # fallback to regular video
            media_type = "video"

    # Convert audio to voice (ogg opus)
    if media_type == "voice" and ext not in (".ogg", ".oga"):
        import subprocess
        out_path = os.path.join(MEDIA_DIR, f"broadcast_{broadcast_id}_voice.ogg")
        try:
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-c:a", "libopus", "-b:a", "64k",
                "-f", "ogg", out_path,
            ], check=True, timeout=120, capture_output=True)
            filename = f"broadcast_{broadcast_id}_voice.ogg"
        except Exception as e:
            print(f"[FFMPEG] voice conversion failed: {e}")
            media_type = "document"

    bc.media_type = media_type
    bc.media_path = filename
    await db.commit()
    return {"media_path": filename, "media_type": media_type}


@app.post("/api/broadcasts/{broadcast_id}/start")
async def start_broadcast(broadcast_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(
        select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)).with_for_update()
    )
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if bc.status not in ("draft", "paused"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Cannot start from status {bc.status}")

    # Per-account lock: at most one running broadcast per tg_account at any time.
    # Two parallel broadcasts on the same TG account double the send rate and
    # significantly raise the risk of a flood ban. Run them sequentially.
    # Held inside the same transaction (with_for_update on the row above) so
    # two concurrent start requests can't both pass this check.
    running_q = await db.execute(
        select(Broadcast.id, Broadcast.title).where(
            Broadcast.tg_account_id == bc.tg_account_id,
            Broadcast.status == "running",
            Broadcast.id != bc.id,
        )
    )
    running = running_q.first()
    if running:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f'На этом TG-аккаунте уже идёт другая рассылка: "{running[1] or running[0]}". '
            f"Дождитесь её завершения или поставьте на паузу.",
        )

    # Build recipient list with diagnostic counts so the user understands WHY
    # it's empty when it's empty.
    import random as _random
    base_filter = (
        Contact.tg_account_id == bc.tg_account_id,
        Contact.chat_type == "private",
    )

    # tag_exclude is applied in EVERY branch below (manual + tags + all) as
    # a secondary filter — defends against the "I excluded tag X but still
    # checked a contact who has tag X" case by dropping them server-side.
    exclude_clause = (
        (~Contact.tags.overlap(bc.tag_exclude),)
        if getattr(bc, "tag_exclude", None)
        else ()
    )

    # Archive cut. Off by default (excludes archived contacts) so the historical
    # behavior is preserved. When include_archived=True the equality clause is
    # dropped entirely, so the recipient set covers archived AND non-archived
    # contacts that match the rest of the filters. Tags exist on contacts
    # regardless of archive state, so this is the correct knob: opting in
    # means "these tag-holders count even if they're archived".
    archive_clause = (
        () if getattr(bc, "include_archived", False)
        else (Contact.is_archived.is_(False),)
    )

    if bc.contact_ids:
        # Manual selection — use specified contacts (private only).
        # Validate all contact_ids belong to user's org AND don't carry
        # a tag that was put in the exclude list.
        result = await db.execute(
            select(Contact).where(
                Contact.id.in_(bc.contact_ids),
                Contact.tg_account_id.in_(_org_accounts_subq(user)),
                *base_filter,
                Contact.status == "approved",
                *archive_clause,
                *exclude_clause,
            )
        )
        contacts = list(result.scalars().all())
    else:
        q = select(Contact).where(
            *base_filter,
            Contact.status == "approved",
            *archive_clause,
            *exclude_clause,
        )
        if bc.tag_filter:
            q = q.where(Contact.tags.overlap(bc.tag_filter))
        result = await db.execute(q)
        contacts = list(result.scalars().all())

    # Random N from filtered set
    if bc.max_recipients and len(contacts) > bc.max_recipients:
        contacts = _random.sample(contacts, bc.max_recipients)

    if not contacts:
        # Diagnose why it's empty so the user gets actionable feedback,
        # not just "No matching recipients".
        total_in_account = (await db.execute(
            select(func.count()).select_from(Contact).where(*base_filter)
        )).scalar_one() or 0
        # The "eligible pool" the user is actually drawing from. Reflects
        # the include_archived choice so the wording matches reality:
        # without the toggle, "active" excludes archived; with it, they're
        # included. Otherwise the message reads "ни один из 50 активных"
        # while archived contacts with the tag exist, which is misleading.
        eligible_count = (await db.execute(
            select(func.count()).select_from(Contact).where(
                *base_filter,
                Contact.status == "approved",
                *archive_clause,
            )
        )).scalar_one() or 0
        # When the user picked tags AND didn't opt into archived contacts,
        # tell them how many archived holders they would have reached if
        # they flipped the toggle. This is the most common reason for "0
        # подходит" right after the tag → archive UX confusion the tester
        # hit. Skip the count when include_archived is already on so we
        # don't waste a query.
        archived_with_tag_count = 0
        if (
            bc.tag_filter
            and not getattr(bc, "include_archived", False)
            and not bc.contact_ids
        ):
            archived_with_tag_count = (await db.execute(
                select(func.count()).select_from(Contact).where(
                    *base_filter,
                    Contact.status == "approved",
                    Contact.is_archived.is_(True),
                    Contact.tags.overlap(bc.tag_filter),
                    *exclude_clause,
                )
            )).scalar_one() or 0

        def _ru_plural_contacts(n: int) -> str:
            # Russian plural forms: 1 контакт, 2-4 контакта, 5+ контактов.
            mod10 = n % 10
            mod100 = n % 100
            if mod10 == 1 and mod100 != 11:
                return "контакт"
            if 2 <= mod10 <= 4 and not (12 <= mod100 <= 14):
                return "контакта"
            return "контактов"

        exclude_note = (
            f" Исключающие теги: {', '.join(bc.tag_exclude)}."
            if getattr(bc, "tag_exclude", None)
            else ""
        )
        archive_hint = (
            f" В архиве есть ещё {archived_with_tag_count} {_ru_plural_contacts(archived_with_tag_count)} с этими тегами — "
            f"включите «Включая архивные контакты», чтобы охватить их."
            if archived_with_tag_count > 0
            else ""
        )
        eligible_label = "активных и архивных" if getattr(bc, "include_archived", False) else "активных"
        if bc.contact_ids:
            reason = (
                f"Из {len(bc.contact_ids)} выбранных вручную {_ru_plural_contacts(len(bc.contact_ids))} "
                f"ни один не подходит (не approved, не private, удалён или попал под исключающий тег"
                f"{'' if getattr(bc, 'include_archived', False) else ', либо в архиве'}).{exclude_note}"
            )
        elif bc.tag_filter:
            reason = (
                f"Ни один из {eligible_count} {eligible_label} {_ru_plural_contacts(eligible_count)} аккаунта "
                f"не имеет нужных тегов ({', '.join(bc.tag_filter)}).{exclude_note}{archive_hint}"
            )
        elif total_in_account == 0:
            reason = "У этого TG-аккаунта вообще нет контактов. Сначала синхронизируйте диалоги."
        elif eligible_count == 0:
            reason = (
                f"В аккаунте {total_in_account} {_ru_plural_contacts(total_in_account)}, "
                f"но ни один не помечен как approved (все в архиве/blocked/pending)."
            )
        else:
            reason = (
                f"В аккаунте {eligible_count} {eligible_label} {_ru_plural_contacts(eligible_count)}, "
                f"но фильтр их отсеял."
            )

        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Получателей не найдено. {reason}")

    # Create recipients (clear old if restarting)
    if bc.status == "draft":
        await db.execute(
            sa_delete(BroadcastRecipient).where(BroadcastRecipient.broadcast_id == bc.id)
        )
        for c in contacts:
            db.add(BroadcastRecipient(broadcast_id=bc.id, contact_id=c.id))

    bc.status = "running"
    bc.total_recipients = len(contacts)
    bc.sent_count = 0
    bc.failed_count = 0
    bc.started_at = datetime.utcnow()
    await db.commit()

    # Run broadcast in background
    asyncio.create_task(_run_broadcast(bc.id))
    return {"status": "started", "recipients": len(contacts)}


@app.post("/api/broadcasts/{broadcast_id}/pause")
async def pause_broadcast(broadcast_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if bc.status != "running":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Not running")
    bc.status = "paused"
    await db.commit()
    return {"status": "paused"}


@app.post("/api/broadcasts/{broadcast_id}/cancel")
async def cancel_broadcast(broadcast_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    bc.status = "cancelled"
    await db.commit()
    return {"status": "cancelled"}


@app.patch("/api/broadcasts/{broadcast_id}", response_model=BroadcastOut)
async def update_broadcast(broadcast_id: UUID, req: BroadcastCreate, user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if bc.status not in ("draft",):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Can only edit draft broadcasts")
    bc.title = req.title
    bc.content = req.content
    bc.tg_account_id = req.tg_account_id
    bc.tag_filter = req.tag_filter
    bc.tag_exclude = req.tag_exclude or []
    bc.include_archived = bool(req.include_archived)
    bc.delay_seconds = max(5, min(3600, req.delay_seconds))
    bc.max_recipients = req.max_recipients
    bc.contact_ids = req.contact_ids or []
    await db.commit()
    await db.refresh(bc)
    return await _broadcast_to_out(bc, db)


@app.delete("/api/broadcasts/{broadcast_id}")
async def delete_broadcast(broadcast_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if bc.status in ("running",):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot delete a running broadcast")
    # Delete recipients first
    from sqlalchemy import delete
    await db.execute(delete(BroadcastRecipient).where(BroadcastRecipient.broadcast_id == broadcast_id))
    await db.delete(bc)
    await db.commit()
    return {"status": "deleted"}


@app.get("/api/broadcasts/{broadcast_id}/recipients", response_model=list[BroadcastRecipientOut])
async def list_broadcast_recipients(broadcast_id: UUID, user: CurrentUser, db: DB):
    """Per-recipient breakdown for the "Статистика" panel on the
    broadcasts page. Operator wants to see who got the message and who
    failed, with the error message right next to the failed row.

    Org-scoping: the broadcast itself is filtered by org, and recipients
    are children of that broadcast. We don't need a second org check on
    each contact — the FK chain guarantees they belong to the same org.

    Sort order matches the operator's debugging flow: failed rows first
    (the ones they need to act on), then sent (in reverse-chronological
    so the latest landed message floats up), then still-pending.

    Real-name display follows the existing org contract: decrypted only
    for TgAccounts whose `show_real_names` flag is on. Otherwise the
    panel falls back to the alias (e.g. "да-4719"). Same gate as the
    main chat list — see app.py around line 1851. `real_tg_id` is plain
    text in DB but still gated, since the bare ID lets an operator look
    up the user externally.
    """
    bc_result = await db.execute(select(Broadcast).where(
        Broadcast.id == broadcast_id,
        Broadcast.org_id == _org_id(user),
    ))
    bc = bc_result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Per-account show_real_names map for the org. Same shape as the
    # /api/contacts handler. Cheap one-shot query — typical org has
    # 1-3 accounts.
    show_real_q = await db.execute(
        select(TgAccount.id, TgAccount.show_real_names).where(TgAccount.org_id == _org_id(user))
    )
    show_real_map = {row[0]: row[1] for row in show_real_q.all()}

    from sqlalchemy import case
    rows = await db.execute(
        select(BroadcastRecipient, Contact)
        .join(Contact, Contact.id == BroadcastRecipient.contact_id)
        .where(BroadcastRecipient.broadcast_id == broadcast_id)
        .order_by(
            # failed → sent → pending (operator's debug order)
            case(
                (BroadcastRecipient.status == "failed", 1),
                (BroadcastRecipient.status == "sent", 2),
                else_=3,
            ),
            BroadcastRecipient.sent_at.desc().nullslast(),
        )
    )

    out: list[BroadcastRecipientOut] = []
    for r, contact in rows.all():
        real_name: str | None = None
        real_username: str | None = None
        real_tg_id: int | None = None
        if show_real_map.get(contact.tg_account_id, False):
            try:
                real_name = decrypt(contact.real_name_encrypted) if contact.real_name_encrypted else None
            except Exception:
                real_name = None
            try:
                real_username = decrypt(contact.real_username_encrypted) if contact.real_username_encrypted else None
            except Exception:
                real_username = None
            real_tg_id = contact.real_tg_id
        out.append(BroadcastRecipientOut(
            contact_id=r.contact_id,
            contact_alias=contact.alias,
            real_name=real_name,
            real_username=real_username,
            real_tg_id=real_tg_id,
            status=r.status,
            sent_at=r.sent_at,
            error=r.error,
        ))
    return out


async def _run_broadcast(broadcast_id: UUID):
    """Background task to send broadcast messages with delay.

    Wrapped in a top-level try/except: catastrophic failures (lost DB
    connection, Telethon client gone, etc.) mark the broadcast as
    `failed` with the error stored in `last_error`, instead of leaving
    it stuck in `running` forever with no diagnostic.
    """
    from models import async_session
    try:
        async with async_session() as db:
            result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id))
            bc = result.scalar_one_or_none()
            if not bc:
                return

            result = await db.execute(
                select(BroadcastRecipient)
                .where(BroadcastRecipient.broadcast_id == broadcast_id, BroadcastRecipient.status == "pending")
            )
            recipients = result.scalars().all()

            # Warm up Telethon entity cache by iterating dialogs once.
            # Without this, send_message(user_id) fails with "Could not find the input entity for
            # PeerUser(...)" when the user is in the contacts table but their access_hash is not in
            # the current SQLite session (happens after re-login or when the contact was added via
            # a different code path). iter_dialogs() forces Telethon to fetch and cache access_hash
            # for every dialog the account currently has.
            from telegram import _clients as _tg_clients
            _client = _tg_clients.get(bc.tg_account_id)
            if not _client:
                bc.status = "failed"
                bc.last_error = (
                    "TG-аккаунт не подключён к серверу. "
                    "Откройте Настройки → TG аккаунты и переподключите."
                )
                await db.commit()
                await ws_manager.broadcast_to_admins({
                    "type": "broadcast_status",
                    "broadcast_id": str(broadcast_id),
                    "status": "failed",
                    "last_error": bc.last_error,
                }, org_id=bc.org_id)
                return

            try:
                _warmed = 0
                async for _ in _client.iter_dialogs(limit=None):
                    _warmed += 1
                print(f"[BROADCAST {broadcast_id}] Warmed entity cache: {_warmed} dialogs", flush=True)
            except Exception as _warm_err:
                print(f"[BROADCAST {broadcast_id}] Warm-up failed: {_warm_err}", flush=True)

            for recip in recipients:
                # Check if paused/cancelled
                await db.refresh(bc)
                if bc.status != "running":
                    return

                # Get contact
                result = await db.execute(select(Contact).where(Contact.id == recip.contact_id))
                contact = result.scalar_one_or_none()
                if not contact:
                    recip.status = "failed"
                    recip.error = "Контакт не найден"
                    bc.failed_count += 1
                    bc.last_error = "Контакт не найден (был удалён после создания рассылки)"
                    await db.commit()
                    continue

                try:
                    # Throttle broadcast sends to stay below Telegram flood
                    # thresholds. Unlike manual send endpoints (which raise
                    # 429), broadcasts sleep until a slot frees up.
                    await wait_tg_send_slot(str(bc.tg_account_id), contact.real_tg_id)

                    file_path = os.path.join(MEDIA_DIR, bc.media_path) if bc.media_path else None
                    tg_msg_id = await send_message(
                        bc.tg_account_id,
                        contact.real_tg_id,
                        text=bc.content,
                        file_path=file_path,
                        media_type=bc.media_type,
                    )

                    # Save message to DB so it shows in chat
                    msg = Message(
                        contact_id=contact.id,
                        tg_message_id=tg_msg_id,
                        direction="outgoing",
                        content=bc.content,
                        media_type=bc.media_type,
                        media_path=bc.media_path,
                        sent_by=bc.created_by,
                    )
                    db.add(msg)
                    contact.last_message_at = datetime.utcnow()
                    _touch_contact_preview(contact, bc.content, bc.media_type, "outgoing")

                    recip.status = "sent"
                    recip.sent_at = datetime.utcnow()
                    bc.sent_count += 1
                except Exception as e:
                    err_str = str(e)[:500]
                    recip.status = "failed"
                    recip.error = err_str
                    bc.failed_count += 1
                    # Surface most-recent error on the broadcast itself so the
                    # user sees what's going wrong without opening every recipient.
                    bc.last_error = f"{contact.alias or contact.real_tg_id}: {err_str}"
                    print(f"[BROADCAST {broadcast_id}] Send failed for {contact.real_tg_id}: {err_str}", flush=True)

                await db.commit()

                # Broadcast progress via WS
                await ws_manager.broadcast_to_admins({
                    "type": "broadcast_progress",
                    "broadcast_id": str(broadcast_id),
                    "sent": bc.sent_count,
                    "failed": bc.failed_count,
                    "total": bc.total_recipients,
                    "last_error": bc.last_error,
                }, org_id=bc.org_id)

                # Delay between sends
                await asyncio.sleep(bc.delay_seconds)

            # Mark completed
            await db.refresh(bc)
            if bc.status == "running":
                bc.status = "completed"
                bc.completed_at = datetime.utcnow()
                await db.commit()
                await ws_manager.broadcast_to_admins({
                    "type": "broadcast_status",
                    "broadcast_id": str(broadcast_id),
                    "status": "completed",
                }, org_id=bc.org_id)
    except Exception as e:
        # Catastrophic failure: lost DB, telethon disconnect, etc.
        # Mark broadcast as failed and surface the error so it doesn't
        # stay stuck in "running" with no diagnostic.
        import traceback
        tb = traceback.format_exc()
        print(f"[BROADCAST {broadcast_id}] Catastrophic failure: {e}\n{tb}", flush=True)
        try:
            from models import async_session as _ses
            async with _ses() as _db:
                _r = await _db.execute(select(Broadcast).where(Broadcast.id == broadcast_id))
                _bc = _r.scalar_one_or_none()
                if _bc:
                    _bc.status = "failed"
                    _bc.last_error = f"Сбой фоновой задачи: {str(e)[:480]}"
                    await _db.commit()
                    await ws_manager.broadcast_to_admins({
                        "type": "broadcast_status",
                        "broadcast_id": str(broadcast_id),
                        "status": "failed",
                        "last_error": _bc.last_error,
                    }, org_id=_bc.org_id)
        except Exception as _post_err:
            print(f"[BROADCAST {broadcast_id}] Failed to mark broadcast failed: {_post_err}", flush=True)


# ============================================================
# Background Tasks (legacy — now also in tasks.py, these remain for compatibility)
# ============================================================

async def _auto_sync_on_startup():
    """Auto-sync ALL dialogs for all connected accounts on every startup."""
    await asyncio.sleep(3)  # Wait for Telethon clients to fully connect
    from telegram import _clients
    async with async_session() as db:
        result = await db.execute(select(TgAccount).where(TgAccount.is_active.is_(True)))
        accounts = result.scalars().all()
    for account in accounts:
        if account.id not in _clients:
            continue
        print(f"[AUTO-SYNC] Syncing all dialogs for {account.phone}...")
        try:
            imported = await _do_sync_dialogs(account.id, None)  # None = no limit
            print(f"[AUTO-SYNC] {account.phone}: imported {imported} new dialogs")
        except Exception as e:
            print(f"[AUTO-SYNC] {account.phone}: error: {e}")
        await asyncio.sleep(1)  # Pace between accounts


async def _process_scheduled_messages():
    """Check for due scheduled messages every 30 seconds and send them."""
    await asyncio.sleep(5)  # Wait for startup
    while True:
        try:
            async with async_session() as db:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                result = await db.execute(
                    select(ScheduledMessage).where(
                        ScheduledMessage.status == "pending",
                        ScheduledMessage.scheduled_at <= now,
                    )
                )
                due = list(result.scalars().all())
                for sm in due:
                    try:
                        contact = await db.get(Contact, sm.contact_id)
                        if not contact or contact.status != "approved":
                            sm.status = "cancelled"
                            continue
                        tg_msg_id = await send_message(
                            contact.tg_account_id, contact.real_tg_id,
                            text=sm.content,
                            file_path=os.path.join(MEDIA_DIR, sm.media_path) if sm.media_path else None,
                            media_type=sm.media_type,
                        )
                        msg = Message(
                            contact_id=sm.contact_id,
                            tg_message_id=tg_msg_id,
                            direction="outgoing",
                            content=sm.content,
                            media_type=sm.media_type,
                            media_path=sm.media_path,
                            sent_by=sm.created_by,
                        )
                        db.add(msg)
                        contact.last_message_at = func.now()
                        sm.status = "sent"
                        sm.sent_at = func.now()
                        print(f"[SCHEDULED] Sent message {sm.id} to {contact.alias}")
                    except Exception as e:
                        print(f"[SCHEDULED] Failed to send {sm.id}: {e}")
                        sm.status = "failed"
                if due:
                    await db.commit()
        except Exception as e:
            print(f"[SCHEDULED] Loop error: {e}")
        await asyncio.sleep(30)


async def _telethon_health_monitor():
    """Check Telethon client connections every 60s, reconnect if needed."""
    from telegram import _clients, _try_reconnect
    await asyncio.sleep(30)  # Wait for startup
    while True:
        try:
            for account_id, client in list(_clients.items()):
                if not client.is_connected():
                    async with async_session() as db:
                        result = await db.execute(
                            select(TgAccount).where(TgAccount.id == account_id, TgAccount.is_active.is_(True))
                        )
                        account = result.scalar_one_or_none()
                        if not account:
                            continue
                        print(f"[HEALTH] {account.phone} disconnected, attempting reconnect...")
                        try:
                            new_client = await _try_reconnect(account_id)
                            if new_client:
                                print(f"[HEALTH] {account.phone} reconnected successfully")
                                await ws_manager.broadcast_to_org(account.org_id, {
                                    "type": "account_status", "account_id": str(account_id), "connected": True,
                                })
                            else:
                                print(f"[HEALTH] {account.phone} reconnect failed")
                        except Exception as e:
                            print(f"[HEALTH] {account.phone} reconnect error: {e}")
        except Exception as e:
            print(f"[HEALTH] Monitor error: {e}")
        await asyncio.sleep(60)


async def _cleanup_old_media():
    """Delete media files older than 60 days that are no longer needed. Runs daily."""
    await asyncio.sleep(300)  # Wait 5 min after startup
    while True:
        try:
            cutoff = datetime.utcnow() - timedelta(days=60)
            async with async_session() as db:
                # Find media_paths referenced by recent messages
                result = await db.execute(
                    select(Message.media_path).where(
                        Message.media_path.isnot(None),
                        Message.created_at >= cutoff,
                    )
                )
                recent_paths = {r[0] for r in result.all()}

            # Scan media directory
            import glob
            media_files = glob.glob(os.path.join(MEDIA_DIR, "*"))
            deleted = 0
            for filepath in media_files:
                filename = os.path.basename(filepath)
                if filename in recent_paths:
                    continue
                # Check file age
                try:
                    file_age = datetime.utcnow() - datetime.fromtimestamp(os.path.getmtime(filepath))
                    if file_age > timedelta(days=60):
                        os.remove(filepath)
                        deleted += 1
                except Exception:
                    pass
            if deleted:
                print(f"[MEDIA-CLEANUP] Deleted {deleted} old media files")
        except Exception as e:
            print(f"[MEDIA-CLEANUP] Error: {e}")
        await asyncio.sleep(86400)  # Daily


async def _cleanup_disconnected_accounts():
    """Delete data for accounts disconnected more than 30 days ago. Runs daily."""
    while True:
        try:
            async with async_session() as db:
                cutoff = datetime.utcnow() - timedelta(days=30)
                # Find accounts disconnected > 30 days
                result = await db.execute(
                    select(TgAccount).where(
                        TgAccount.is_active.is_(False),
                        TgAccount.disconnected_at.isnot(None),
                        TgAccount.disconnected_at < cutoff,
                    )
                )
                expired = list(result.scalars().all())
                for acc in expired:
                    aid = acc.id
                    # Delete contacts + cascade data
                    contact_rows = await db.execute(select(Contact.id).where(Contact.tg_account_id == aid))
                    cids = [r[0] for r in contact_rows.all()]
                    if cids:
                        await db.execute(sa_delete(Message).where(Message.contact_id.in_(cids)))
                        await db.execute(sa_delete(PinnedChat).where(PinnedChat.contact_id.in_(cids)))
                        await db.execute(sa_delete(AuditLog).where(AuditLog.target_contact_id.in_(cids)))
                        await db.execute(sa_delete(BroadcastRecipient).where(BroadcastRecipient.contact_id.in_(cids)))
                        await db.execute(sa_delete(Contact).where(Contact.tg_account_id == aid))
                    # Delete templates, tags
                    await db.execute(sa_delete(MessageTemplate).where(MessageTemplate.tg_account_id == aid))
                    await db.execute(sa_delete(Tag).where(Tag.tg_account_id == aid))
                    # Delete account
                    await db.execute(sa_delete(TgAccount).where(TgAccount.id == aid))
                    print(f"[CLEANUP] Deleted expired disconnected account {acc.phone} ({aid})")
                if expired:
                    await db.commit()
        except Exception as e:
            print(f"[CLEANUP] Error: {e}")
        await asyncio.sleep(86400)  # Run daily


async def _do_sync_dialogs(account_id: UUID, limit: int | None = None) -> int:
    """Background-safe: import dialogs for a TG account. Returns count imported.

    Structural rewrite focused on DB pool pressure:
      - One-shot alias sequence counter instead of SELECT count(*) per new
        dialog (was O(N²), dominated long syncs at 1000+ contacts).
      - Short-lived DB sessions per dialog — the previous version held one
        pool slot for 5-15 minutes during a full sync, starving the pool
        and blocking /api/contacts requests behind connection checkout.
      - Telegram network I/O (iter_dialogs, get_messages, download_media)
        happens OUTSIDE any open session, so connections are only held
        while there's actual SQL to run.

    limit=None means fetch ALL dialogs (both main folder and archive).
    """
    from telegram import _clients, generate_alias, _extract_media, sanitize_text, extract_stripped_thumb
    from crypto import encrypt

    client = _clients.get(account_id)
    if not client:
        print(f"[SYNC] Account {account_id} not connected, skipping")
        return 0

    me = await client.get_me()
    imported = 0
    existing_updates = 0

    # One-time: verify account + read starting alias sequence. This cold
    # query replaces the per-new-dialog SELECT count(*) that used to run
    # on every iteration.
    async with async_session() as db:
        account_row = await db.execute(select(TgAccount).where(TgAccount.id == account_id))
        account = account_row.scalar_one_or_none()
        if not account:
            print(f"[SYNC] Account {account_id} not found in DB")
            return 0
        seq_row = await db.execute(select(func.count(Contact.id)))
        next_seq = (seq_row.scalar() or 0) + 1

    def _decode_mute(raw_dialog) -> bool:
        ns = getattr(raw_dialog, "notify_settings", None) if raw_dialog else None
        mute_until = getattr(ns, "mute_until", None) if ns else None
        if mute_until is None:
            return False
        try:
            ts = mute_until.timestamp() if hasattr(mute_until, "timestamp") else float(mute_until)
            return ts > time.time()
        except (TypeError, ValueError, OverflowError):
            # "forever" sentinel may overflow on some platforms — treat as muted.
            return True

    # archived=None iterates BOTH main folder AND archive folder. Default
    # (False) skips the archive folder entirely.
    async for dialog in client.iter_dialogs(limit=limit, archived=None):
        peer_id = dialog.id
        if not peer_id or peer_id == 777000:
            continue

        is_tg_archived = bool(getattr(dialog, "archived", False))
        is_tg_pinned = bool(getattr(dialog, "pinned", False))
        is_tg_muted = _decode_mute(getattr(dialog, "dialog", None))
        tg_thumb = extract_stripped_thumb(dialog.entity)
        dialog_date = getattr(dialog, "date", None)
        tg_last_msg_at = None
        if dialog_date:
            tg_last_msg_at = dialog_date.replace(tzinfo=None) if dialog_date.tzinfo else dialog_date
        # Native-Telegram read state — every Dialog carries the max
        # message_id the user has acknowledged as read in their TG app.
        # Anything <= read_inbox_max_id is already read. We use this
        # to backfill CRM's is_read flag so a chat read on the phone
        # doesn't keep a stale unread badge after opening CRM.
        tg_read_inbox_max_id = getattr(getattr(dialog, "dialog", None), "read_inbox_max_id", None) or 0

        # --- short-lived session: update-existing-or-create path ---
        async with async_session() as db:
            result = await db.execute(
                select(Contact).where(Contact.tg_account_id == account_id, Contact.real_tg_id == peer_id)
            )
            existing = result.scalars().first()

            if existing:
                dirty = False
                if existing.is_pinned != is_tg_pinned:
                    existing.is_pinned = is_tg_pinned
                    dirty = True
                if existing.is_muted != is_tg_muted:
                    existing.is_muted = is_tg_muted
                    dirty = True
                # One-way archive sync: TG archived -> CRM archived, never
                # un-archive (prevents the re-archive loop from 89bf3a4).
                if is_tg_archived and not existing.is_archived:
                    existing.is_archived = True
                    dirty = True
                # Stripped thumb diff = profile photo changed → invalidate
                # the on-disk full-res cache so next /avatar re-downloads.
                if existing.avatar_thumb != tg_thumb:
                    existing.avatar_thumb = tg_thumb
                    dirty = True
                    try:
                        _stale_path = os.path.join(MEDIA_DIR, "avatars", f"{existing.id}.jpg")
                        if os.path.exists(_stale_path):
                            os.remove(_stale_path)
                    except Exception as _e:
                        print(f"[SYNC] avatar cache invalidate failed: {_e}")
                if tg_last_msg_at and (not existing.last_message_at or tg_last_msg_at > existing.last_message_at):
                    existing.last_message_at = tg_last_msg_at
                    dirty = True

                # Sync native-TG read state in bulk. One UPDATE per contact
                # that actually had unread messages below the TG high-water
                # mark. Clears the stale "90 unread" badge the user sees
                # after reading a chat on their phone while CRM was off.
                if tg_read_inbox_max_id:
                    from sqlalchemy import update as sa_update
                    read_result = await db.execute(
                        sa_update(Message)
                        .where(
                            Message.contact_id == existing.id,
                            Message.direction == "incoming",
                            Message.tg_message_id <= tg_read_inbox_max_id,
                            Message.is_read.is_(False),
                        )
                        .values(is_read=True)
                    )
                    if read_result.rowcount:
                        dirty = True
                        if existing.last_message_direction == "incoming":
                            existing.last_message_is_read = True

                if dirty:
                    await db.commit()
                    existing_updates += 1
                continue

            # Skip creating rows for archived dialogs.
            if is_tg_archived:
                continue

            # --- new contact ---
            entity = dialog.entity
            is_forum = getattr(entity, "forum", False)
            if getattr(entity, "megagroup", False) or is_forum:
                chat_type = "supergroup"
            elif dialog.is_group:
                chat_type = "group"
            elif dialog.is_channel:
                chat_type = "channel"
            else:
                chat_type = "private"
            name = dialog.name or ""
            username = getattr(entity, "username", None)

            # Alias generation: start from in-memory counter, fall back to
            # per-alias lookup only on collision (rare).
            alias_base = generate_alias(name, next_seq)
            next_seq += 1
            for _ in range(10):
                check = await db.execute(select(Contact.id).where(Contact.alias == alias_base))
                if not check.scalar_one_or_none():
                    break
                alias_base = generate_alias(name, next_seq)
                next_seq += 1

            contact = Contact(
                tg_account_id=account_id,
                real_tg_id=peer_id,
                real_name_encrypted=encrypt(name) if name else None,
                real_username_encrypted=encrypt(username) if username else None,
                group_title_encrypted=encrypt(dialog.name) if chat_type != "private" and dialog.name else None,
                chat_type=chat_type,
                is_forum=is_forum,
                alias=alias_base,
                status="approved",
                approved_at=datetime.utcnow(),
                last_message_at=tg_last_msg_at,
                is_pinned=is_tg_pinned,
                is_muted=is_tg_muted,
                avatar_thumb=tg_thumb,
            )
            db.add(contact)
            try:
                await db.commit()
                await db.refresh(contact)
            except Exception as e:
                await db.rollback()
                print(f"[SYNC] Failed to insert contact {peer_id}: {e}")
                continue
            contact_id_new = contact.id
            contact_is_forum = is_forum
            contact_chat_type = chat_type

        # --- Telegram I/O happens OUTSIDE any open session ---
        msg_limit = 200 if contact_is_forum else 30
        try:
            msgs = await client.get_messages(peer_id, limit=msg_limit)
        except Exception as e:
            print(f"[SYNC] Failed to fetch messages for {peer_id}: {e}")
            imported += 1
            continue

        # Prepare all message data (including media downloads) without
        # holding a DB connection. We only reopen the session to insert.
        pending_rows: list[dict] = []
        for msg_obj in reversed(msgs):
            if not msg_obj or not (msg_obj.text or msg_obj.media):
                continue
            sender_id = getattr(msg_obj, "sender_id", None) or getattr(msg_obj, "from_id", None)
            if hasattr(sender_id, "user_id"):
                sender_id = sender_id.user_id
            direction = "outgoing" if sender_id == me.id else "incoming"

            media_type, ext = _extract_media(msg_obj)
            media_path = None
            if media_type and msg_obj.media:
                try:
                    fname = f"{contact_id_new}_{msg_obj.id}{ext or ''}"
                    dl_path = os.path.join(MEDIA_DIR, fname)
                    await client.download_media(msg_obj, file=dl_path)
                    media_path = fname
                except Exception:
                    pass

            sender_tg_id_val = None
            sender_alias_val = None
            if contact_chat_type != "private" and direction == "incoming":
                sender = getattr(msg_obj, "sender", None)
                if sender:
                    sender_tg_id_val = getattr(sender, "id", None)
                    fname = getattr(sender, "first_name", "") or ""
                    lname = getattr(sender, "last_name", "") or ""
                    title = getattr(sender, "title", "") or ""
                    sender_alias_val = (f"{fname} {lname}".strip() or title or "User")

            topic_id_val = None
            topic_name_val = None
            if contact_is_forum and hasattr(msg_obj, "reply_to") and msg_obj.reply_to:
                rt = msg_obj.reply_to
                if getattr(rt, "forum_topic", False):
                    topic_id_val = getattr(rt, "reply_to_msg_id", None)
                else:
                    topic_id_val = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
            elif contact_is_forum:
                topic_id_val = 1
            if topic_id_val is not None:
                from telegram import _resolve_topic_name
                topic_name_val = await _resolve_topic_name(client, peer_id, topic_id_val, account_id)

            fwd_alias = None
            if msg_obj.forward:
                fwd_sender = msg_obj.forward.sender
                if fwd_sender:
                    fn = getattr(fwd_sender, "first_name", "") or ""
                    ln = getattr(fwd_sender, "last_name", "") or ""
                    tt = getattr(fwd_sender, "title", "") or ""
                    fwd_alias = (f"{fn} {ln}".strip() or tt or "User")

            msg_date = msg_obj.date.replace(tzinfo=None) if msg_obj.date else datetime.utcnow()
            pending_rows.append({
                "contact_id": contact_id_new,
                "tg_message_id": msg_obj.id,
                "direction": direction,
                "content": sanitize_text(msg_obj.text),
                "media_type": media_type,
                "media_path": media_path,
                "is_read": True,
                "sender_tg_id": sender_tg_id_val,
                "sender_alias": sender_alias_val,
                "topic_id": topic_id_val,
                "topic_name": topic_name_val,
                "forwarded_from_alias": fwd_alias,
                "created_at": msg_date,
            })

        if pending_rows:
            async with async_session() as db:
                try:
                    for row in pending_rows:
                        db.add(Message(**row))
                    await db.commit()
                except Exception as e:
                    await db.rollback()
                    print(f"[SYNC] Failed to persist messages for {peer_id}: {e}")

        imported += 1
        if imported % 10 == 0:
            await asyncio.sleep(0.5)

    print(f"[SYNC] Finished: imported {imported} new, updated {existing_updates} existing for account {account_id}")
    return imported


@app.post("/api/tg/{account_id}/sync-dialogs")
async def sync_old_dialogs(account_id: UUID, user: AdminUser, db: DB):
    """Trigger full dialog sync as a background task. Syncs ALL dialogs."""
    from telegram import _clients
    client = _clients.get(account_id)
    if not client:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Account not connected")
    asyncio.create_task(_do_sync_dialogs(account_id, None))  # Sync ALL dialogs
    return {"status": "sync_started"}


# ============================================================
# Staff Signature Mode
# ============================================================

@app.patch("/api/staff/me/signature")
async def update_signature_mode(user: CurrentUser, db: DB, mode: str = Query(...)):
    if mode not in ("named", "anonymous"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Mode must be 'named' or 'anonymous'")
    result = await db.execute(select(Staff).where(Staff.id == user.id))
    staff = result.scalar_one_or_none()
    if staff:
        staff.signature_mode = mode
        await db.commit()
    return {"signature_mode": mode}


# ============================================================
# Staff Timezone
# ============================================================

VALID_TIMEZONES = {
    "UTC",
    "Europe/Moscow", "Europe/Berlin", "Europe/London", "Europe/Paris",
    "Europe/Istanbul", "Europe/Kiev",
    "Asia/Dubai", "Asia/Bangkok", "Asia/Singapore", "Asia/Tokyo",
    "Asia/Shanghai", "Asia/Kolkata",
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Sao_Paulo",
    "Pacific/Auckland",
}


@app.patch("/api/staff/me/timezone")
async def update_timezone(user: CurrentUser, db: DB, timezone: str = Query(...)):
    """Update current user's timezone."""
    if timezone not in VALID_TIMEZONES:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid timezone: {timezone}")
    result = await db.execute(select(Staff).where(Staff.id == user.id))
    staff = result.scalar_one_or_none()
    if staff:
        staff.timezone = timezone
        await db.commit()
    return {"timezone": timezone}


# ============================================================
# Reports
# ============================================================

@app.get("/api/reports/new-chats")
async def report_new_chats(
    user: CurrentUser,
    db: DB,
    from_date: str = Query(..., description="ISO date or datetime, e.g. 2026-03-20 or 2026-03-20T14:30"),
    to_date: str = Query(..., description="ISO date or datetime, e.g. 2026-03-23 or 2026-03-23T17:45"),
    tg_account_id: UUID | None = Query(None),
    timezone: str = Query("UTC"),
):
    """Report: chats where the FIRST message landed inside the date range.

    Accepts both plain dates (whole-day semantics) and datetime-local values
    (hour/minute precision). When the range spans <= 48 hours, the
    per-period buckets are hourly; otherwise daily.

    The previous implementation counted `Contact.created_at BETWEEN from AND to`,
    which is the database insert timestamp — a sync run dumped every existing
    Telegram chat with created_at=now(), producing huge false counts like
    "371 new chats today". The correct signal is the earliest real message
    in the chat: `MIN(messages.created_at) WHERE contact_id=X`. If that
    minimum falls in the period, the chat really became active then.
    """
    # Validate timezone
    if timezone not in VALID_TIMEZONES:
        timezone = "UTC"

    # Accept both `YYYY-MM-DD` and `YYYY-MM-DDTHH:MM[:SS]`. For plain dates,
    # `start` is 00:00 and `end` is 23:59:59 to keep the old whole-day
    # semantics. For datetime values, we take them literally so the user
    # can drill down to e.g. "2026-04-11T09:00 — 2026-04-11T18:00".
    def _parse_dt(s: str, end_of_day: bool) -> datetime:
        try:
            if "T" in s or " " in s:
                return datetime.fromisoformat(s.replace(" ", "T"))
            d = datetime.fromisoformat(s)
            if end_of_day:
                return d.replace(hour=23, minute=59, second=59)
            return d
        except ValueError:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid date format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM.")

    start = _parse_dt(from_date, end_of_day=False)
    end = _parse_dt(to_date, end_of_day=True)
    if end < start:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "`to_date` must be >= `from_date`")

    range_seconds = (end - start).total_seconds()
    bucket = "hour" if range_seconds <= 48 * 3600 else "day"

    org = _org_id(user)
    org_accounts = select(TgAccount.id).where(TgAccount.org_id == org)

    # Subquery: first INCOMING message per contact. "New chat" means someone
    # wrote TO YOU — not an old contact whose history was imported during
    # dialog sync. Without the direction filter, sync-imported contacts
    # whose oldest messages happen to fall in the date range (because only
    # the last ~200 messages are synced, not the full history) produce
    # inflated counts like "371 new chats today".
    first_msg_sub = (
        select(
            Message.contact_id.label("contact_id"),
            func.min(Message.created_at).label("first_at"),
        )
        .where(Message.direction == "incoming")
        .group_by(Message.contact_id)
        .subquery()
    )

    base_where = [
        Contact.tg_account_id.in_(org_accounts),
        first_msg_sub.c.first_at >= start,
        first_msg_sub.c.first_at <= end,
    ]
    if tg_account_id:
        base_where.append(Contact.tg_account_id == tg_account_id)
    # Operator scope — same as list_contacts.
    if user.role == "operator":
        op_sub = select(StaffTgAccount.tg_account_id).where(StaffTgAccount.staff_id == user.id)
        base_where.append(Contact.tg_account_id.in_(op_sub))

    # --- total ---
    total_q = (
        select(func.count(Contact.id))
        .join(first_msg_sub, first_msg_sub.c.contact_id == Contact.id)
        .where(*base_where)
    )
    total = (await db.execute(total_q)).scalar() or 0

    # --- by bucket (hour for tight ranges, day otherwise), in target tz ---
    tz_first_at = first_msg_sub.c.first_at.op("AT TIME ZONE")("UTC").op("AT TIME ZONE")(timezone)
    if bucket == "hour":
        bucket_expr = func.date_trunc("hour", tz_first_at)
    else:
        bucket_expr = func.date_trunc("day", tz_first_at)
    by_bucket_q = (
        select(bucket_expr.label("bucket"), func.count(Contact.id).label("cnt"))
        .join(first_msg_sub, first_msg_sub.c.contact_id == Contact.id)
        .where(*base_where)
        .group_by(bucket_expr)
        .order_by(bucket_expr)
    )
    by_day = []
    for row in (await db.execute(by_bucket_q)).all():
        if bucket == "hour" and row.bucket is not None:
            # Label as "YYYY-MM-DD HH:00" so the frontend can render it verbatim.
            label = row.bucket.strftime("%Y-%m-%d %H:00")
        else:
            label = str(row.bucket)[:10] if row.bucket else ""
        by_day.append({"date": label, "count": row.cnt})

    # --- by account ---
    by_account_q = (
        select(
            TgAccount.id.label("account_id"),
            TgAccount.phone,
            TgAccount.display_name,
            func.count(Contact.id).label("cnt"),
        )
        .join(first_msg_sub, first_msg_sub.c.contact_id == Contact.id)
        .join(TgAccount, Contact.tg_account_id == TgAccount.id)
        .where(*base_where)
        .group_by(TgAccount.id, TgAccount.phone, TgAccount.display_name)
        .order_by(func.count(Contact.id).desc())
    )
    by_account = [
        {
            "account_id": str(row.account_id),
            "phone": row.phone,
            "display_name": row.display_name,
            "count": row.cnt,
        }
        for row in (await db.execute(by_account_q)).all()
    ]

    return {"total": total, "by_day": by_day, "by_account": by_account}


# ============================================================
# CRM Settings (global, managed by super_admin)
# ============================================================

@app.get("/api/settings/crm")
async def get_crm_settings(user: CurrentUser, db: DB):
    """Get CRM settings for current org — true if ANY staff in this org has it enabled."""
    result = await db.execute(
        select(Staff).where(Staff.postforge_org_id == _org_id(user), Staff.show_real_names.is_(True)).limit(1)
    )
    has_real = result.scalar_one_or_none() is not None
    return {
        "show_real_names": has_real,
    }


@app.patch("/api/settings/crm")
async def update_crm_settings(
    user: AdminUser, db: DB,
    show_real_names: bool = Query(...),
    tg_account_id: UUID | None = Query(None),
):
    """Update CRM settings. If tg_account_id given, set per-account; else set for all staff globally."""
    if tg_account_id:
        await db.execute(
            sa_update(TgAccount).where(
                TgAccount.id == tg_account_id, TgAccount.org_id == _org_id(user)
            ).values(show_real_names=show_real_names)
        )
    else:
        await db.execute(
            sa_update(Staff).where(Staff.postforge_org_id == _org_id(user)).values(show_real_names=show_real_names)
        )
        await db.execute(
            sa_update(TgAccount).where(TgAccount.org_id == _org_id(user)).values(show_real_names=show_real_names)
        )
    await db.commit()
    return {"show_real_names": show_real_names}


# ============================================================
# WebSocket
# ============================================================

# ============================================================
# WebSocket ticket-based auth (Slack-style)
#
# Why: passing JWT in ws://...?token=<jwt> leaks the token into
# browser history, nginx access logs, and proxy logs. Any of those
# is a direct account takeover vector.
#
# Pattern: client calls POST /api/ws/ticket with JWT in Authorization
# header (safe, not logged). Server returns a single-use UUID ticket
# stored in Redis with 30-second TTL. Client connects to ws://?ticket=<uuid>.
# Server redeems ticket from Redis (delete-on-read) and proceeds.
# The real JWT never appears in a URL.
# ============================================================

import secrets as _secrets

# In-memory fallback if Redis is unavailable. Dict of {ticket: staff_id, org_id, expires}
_ws_tickets: dict[str, tuple[UUID, str | None, float]] = {}
_WS_TICKET_TTL = 30  # seconds


@app.post("/api/ws/ticket")
async def create_ws_ticket(user: Annotated[Staff, Depends(get_current_user)]):
    """Issue a single-use, short-lived ticket for WebSocket connection.

    The ticket replaces the JWT in the WS URL, keeping the real token out of
    browser history and server logs (Slack uses the same pattern).
    """
    ticket = _secrets.token_urlsafe(32)
    expires = time.time() + _WS_TICKET_TTL

    r = await _get_redis()
    if r:
        try:
            await r.set(f"ws_ticket:{ticket}", f"{user.id}:{_org_id(user) or ''}", ex=_WS_TICKET_TTL)
        except Exception:
            # Redis down — use in-memory fallback
            _ws_tickets[ticket] = (user.id, _org_id(user), expires)
    else:
        _ws_tickets[ticket] = (user.id, _org_id(user), expires)

    return {"ticket": ticket}


async def _redeem_ws_ticket(ticket: str) -> tuple[UUID, str | None] | None:
    """Redeem a WS ticket (single-use, delete on read). Returns (staff_id, org_id) or None."""
    r = await _get_redis()
    if r:
        try:
            # GETDEL is atomic: reads and deletes in one command (Redis 6.2+).
            # Prevents two concurrent WS connections from both redeeming the same ticket.
            val = await r.getdel(f"ws_ticket:{ticket}")
            if val:
                raw = val if isinstance(val, str) else val.decode()
                parts = raw.split(":", 1)
                org_id = parts[1] if (len(parts) > 1 and parts[1]) else None
                return UUID(parts[0]), org_id
        except Exception:
            pass

    # In-memory fallback
    entry = _ws_tickets.pop(ticket, None)
    if entry:
        staff_id, org_id, expires = entry
        if time.time() < expires:
            return staff_id, org_id
    return None


async def _handle_ws(ws: WebSocket, ticket: str | None = None, token: str | None = None):
    """Shared WebSocket handler. Accepts ticket (preferred) or legacy token."""
    staff_id: UUID | None = None
    org_id: str | None = None

    if ticket:
        # Ticket-based auth (secure — JWT never in URL)
        result = await _redeem_ws_ticket(ticket)
        if not result:
            await ws.close(code=4001)
            return
        staff_id, org_id = result
    elif token:
        # Legacy token-based auth (kept for backward compat during rollout)
        payload = decode_token(token)
        if payload.get("type") != "access":
            await ws.close(code=4001)
            return
        staff_id = UUID(payload["sub"])
        # Need to look up org_id
        from models import async_session as get_session
        async with get_session() as db:
            result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True)))
            user = result.scalar_one_or_none()
            if not user:
                await ws.close(code=4003)
                return
            org_id = user.postforge_org_id
    else:
        await ws.close(code=4001)
        return

    # For ticket-based auth, verify user still exists and is active
    if ticket:
        from models import async_session as get_session
        async with get_session() as db:
            result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True)))
            user = result.scalar_one_or_none()
            if not user:
                await ws.close(code=4003)
                return

    await ws_manager.connect(staff_id, ws, org_id=org_id)

    # Periodic token/session revalidation (Fix #3 from audit).
    # Every 5 minutes, verify the user is still active. If deactivated
    # (e.g., admin revoked access), close the WS instead of letting it
    # receive messages indefinitely on a stale session.
    _last_revalidation = time.time()

    try:
        while True:
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=45)
                if data == "ping":
                    await ws.send_text("pong")

                # Revalidate every 5 minutes
                now = time.time()
                if now - _last_revalidation > 300:
                    _last_revalidation = now
                    from models import async_session as get_session
                    async with get_session() as db:
                        still_active = (await db.execute(
                            select(Staff.is_active).where(Staff.id == staff_id)
                        )).scalar()
                        if not still_active:
                            await ws.close(code=4003, reason="Session revoked")
                            return

            except asyncio.TimeoutError:
                try:
                    await ws.send_text('{"type":"ping"}')
                except Exception:
                    break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        ws_manager.disconnect(staff_id, ws)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, ticket: str = Query(None), token: str = Query(None)):
    # ws.accept() is called inside ws_manager.connect() — do NOT call it here.
    await _handle_ws(ws, ticket=ticket, token=token)


@app.websocket("/crm/ws")
async def websocket_endpoint_crm(ws: WebSocket, ticket: str = Query(None), token: str = Query(None)):
    await _handle_ws(ws, ticket=ticket, token=token)


# ============================================================
# CRM Admin panel — protected by is_crm_admin (PostForge beta flag)
# ============================================================

@app.get("/api/admin/me")
async def admin_me(user: CrmAdminUser):
    """Check if current user has CRM admin access. Used by frontend
    to show/hide the admin menu item."""
    return {
        "id": str(user.id),
        "name": user.name,
        "is_crm_admin": user.is_crm_admin,
        "postforge_user_id": user.postforge_user_id,
    }


@app.get("/api/admin/audit")
async def admin_audit_log(
    user: CrmAdminUser,
    db: DB,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    action: str | None = Query(None, description="Filter by action name"),
    staff_id: UUID | None = Query(None, description="Filter by actor staff"),
    target_type: str | None = Query(None, description="Filter by target type"),
    since: str | None = Query(None, description="ISO timestamp — entries after this"),
):
    """
    Paginated audit log. Returns entries newest-first with actor info.

    Admin sees audit logs for ALL orgs — this is by design (CRM admin
    is a global role, not org-scoped).
    """
    from sqlalchemy import desc as sa_desc

    query = select(AuditLog, Staff.name, Staff.postforge_org_id).join(
        Staff, Staff.id == AuditLog.staff_id
    )

    if action:
        query = query.where(AuditLog.action == action)
    if staff_id:
        query = query.where(AuditLog.staff_id == staff_id)
    if target_type:
        query = query.where(AuditLog.target_type == target_type)
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00")).replace(tzinfo=None)
            query = query.where(AuditLog.created_at >= since_dt)
        except ValueError:
            pass

    query = query.order_by(sa_desc(AuditLog.created_at)).limit(limit).offset(offset)
    result = await db.execute(query)

    entries = []
    for log, actor_name, actor_org in result.all():
        entries.append({
            "id": str(log.id),
            "action": log.action,
            "actor_id": str(log.staff_id),
            "actor_name": actor_name,
            "actor_org": actor_org,
            "target_id": log.target_id,
            "target_type": log.target_type,
            "target_contact_id": str(log.target_contact_id) if log.target_contact_id else None,
            "metadata": log.metadata_json,
            "ip_address": log.ip_address,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        })

    # Total count for pagination
    count_q = select(func.count(AuditLog.id))
    if action:
        count_q = count_q.where(AuditLog.action == action)
    if staff_id:
        count_q = count_q.where(AuditLog.staff_id == staff_id)
    if target_type:
        count_q = count_q.where(AuditLog.target_type == target_type)
    total = (await db.execute(count_q)).scalar() or 0

    return {"entries": entries, "total": total, "limit": limit, "offset": offset}


@app.get("/api/admin/audit/actions")
async def admin_audit_actions(user: CrmAdminUser, db: DB):
    """Distinct action names from audit log — for filter dropdown."""
    result = await db.execute(select(AuditLog.action).distinct())
    return {"actions": sorted([row[0] for row in result.all() if row[0]])}


@app.get("/api/admin/stats")
async def admin_stats(user: CrmAdminUser, db: DB):
    """
    Critical CRM metrics across all orgs. Used by the admin dashboard
    to spot problems (stuck broadcasts, dead accounts, flood patterns).
    """
    from datetime import timedelta

    now = datetime.utcnow()
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)

    # Counts
    total_staff = (await db.execute(select(func.count(Staff.id)).where(Staff.is_active.is_(True)))).scalar() or 0
    total_accounts = (await db.execute(select(func.count(TgAccount.id)).where(TgAccount.is_active.is_(True)))).scalar() or 0
    total_contacts = (await db.execute(select(func.count(Contact.id)))).scalar() or 0
    total_messages = (await db.execute(select(func.count(Message.id)))).scalar() or 0

    messages_24h = (await db.execute(
        select(func.count(Message.id)).where(Message.created_at >= day_ago)
    )).scalar() or 0
    messages_7d = (await db.execute(
        select(func.count(Message.id)).where(Message.created_at >= week_ago)
    )).scalar() or 0

    # Broadcasts
    broadcasts_running = (await db.execute(
        select(func.count(Broadcast.id)).where(Broadcast.status == "running")
    )).scalar() or 0
    broadcasts_completed_24h = (await db.execute(
        select(func.count(Broadcast.id)).where(
            Broadcast.status == "completed",
            Broadcast.completed_at >= day_ago,
        )
    )).scalar() or 0

    # Audit events in last 24h
    audit_24h = (await db.execute(
        select(func.count(AuditLog.id)).where(AuditLog.created_at >= day_ago)
    )).scalar() or 0

    # Telethon clients currently connected (in-memory)
    from telegram import _clients as _tg_clients
    connected_clients = sum(1 for c in _tg_clients.values() if c.is_connected())

    return {
        "staff": {"total_active": total_staff, "crm_admins": await _count_crm_admins(db)},
        "accounts": {"total_active": total_accounts, "currently_connected": connected_clients},
        "contacts": {"total": total_contacts},
        "messages": {
            "total": total_messages,
            "last_24h": messages_24h,
            "last_7d": messages_7d,
        },
        "broadcasts": {
            "running": broadcasts_running,
            "completed_24h": broadcasts_completed_24h,
        },
        "audit": {"events_24h": audit_24h},
    }


async def _count_crm_admins(db) -> int:
    """Count all staff with is_crm_admin = True."""
    result = await db.execute(
        select(func.count(Staff.id)).where(
            Staff.is_crm_admin.is_(True),
            Staff.is_active.is_(True),
        )
    )
    return result.scalar() or 0


@app.get("/api/admin/accounts")
async def admin_all_accounts(user: CrmAdminUser, db: DB):
    """All TG accounts across all orgs — for debugging connection issues."""
    from telegram import _clients as _tg_clients

    result = await db.execute(
        select(TgAccount, Staff.name, Staff.postforge_org_id).outerjoin(
            Staff, Staff.postforge_org_id == TgAccount.org_id
        ).where(TgAccount.is_active.is_(True))
    )

    # Deduplicate by account.id (the join can produce duplicates
    # when multiple staff share the same org)
    seen = set()
    accounts = []
    for acc, staff_name, staff_org in result.all():
        if acc.id in seen:
            continue
        seen.add(acc.id)
        client = _tg_clients.get(acc.id)
        accounts.append({
            "id": str(acc.id),
            "phone": acc.phone,
            "display_name": acc.display_name,
            "org_id": acc.org_id,
            "connected": bool(client and client.is_connected()),
            "connected_at": acc.connected_at.isoformat() if acc.connected_at else None,
            "disconnected_at": acc.disconnected_at.isoformat() if acc.disconnected_at else None,
        })

    return {"accounts": accounts, "total": len(accounts)}


@app.get("/api/admin/staff")
async def admin_all_staff(user: CrmAdminUser, db: DB):
    """All active staff across orgs with their roles + admin flags."""
    result = await db.execute(
        select(Staff).where(Staff.is_active.is_(True)).order_by(
            Staff.is_crm_admin.desc(), Staff.name
        )
    )
    staff_list = []
    for s in result.scalars().all():
        staff_list.append({
            "id": str(s.id),
            "name": s.name,
            "role": s.role,
            "org_id": s.postforge_org_id,
            "postforge_user_id": s.postforge_user_id,
            "is_crm_admin": s.is_crm_admin,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        })
    return {"staff": staff_list, "total": len(staff_list)}


# ============================================================
# Internal — called by PostForge backend
# ============================================================
# Authenticated by the shared POSTFORGE_BOT_SECRET (same value as
# PostForge's BACKEND_BOT_SECRET — both sides have it as a secret env var).

def _verify_bot_secret(authorization: str = Header(default="")) -> None:
    """FastAPI dependency: require Authorization: Bot <POSTFORGE_BOT_SECRET>."""
    expected = settings.POSTFORGE_BOT_SECRET or ""
    if not expected:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "POSTFORGE_BOT_SECRET not configured")
    if not authorization.startswith("Bot "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bot token")
    token = authorization[4:].strip()
    import hmac as _hmac
    if not _hmac.compare_digest(token, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid bot token")


class _InvalidateSessionsRequest(PydanticBaseModel):
    sids: list[str]  # PostForge user_sessions.id values


@app.post("/api/internal/invalidate-sessions")
async def internal_invalidate_sessions(
    req: _InvalidateSessionsRequest,
    _: None = Depends(_verify_bot_secret),
):
    """Receive PostForge's revoke webhook.

    PostForge calls this whenever a user_sessions row is revoked for ANY
    reason: explicit "завершить сессию" from the UI, password change,
    logout, revoke-others, cookie-theft detection (fingerprint/country
    mismatch mid-request). We write a Redis blacklist entry for each sid;
    the CRM auth dependency (``get_current_user`` in backend/auth.py)
    checks it on every request and immediately 401s any CRM JWT whose
    pf_sid is flagged.

    TTL is 35 days — comfortably longer than any CRM JWT lifetime, so
    a flag set today can never be outlived by a token issued before it.
    """
    if not req.sids:
        return {"ok": True, "invalidated": 0}
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.REDIS_URL, socket_connect_timeout=3)
        try:
            for sid in req.sids:
                await r.set(f"pf_sess_revoked:{sid}", "1", ex=35 * 24 * 3600)
        finally:
            await r.aclose()
    except Exception as e:
        # Failing here is not catastrophic — the CRM JWT's short TTL is
        # the ultimate backstop. But it IS bad, so surface as 500 so
        # PostForge can retry.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"redis write failed: {e}")
    return {"ok": True, "invalidated": len(req.sids)}


class _StaffSyncRoleRequest(PydanticBaseModel):
    postforge_user_id: str
    postforge_org_id: str
    new_role: str  # 'super_admin' | 'admin' | 'operator'


@app.post("/api/internal/staff/sync-role")
async def internal_sync_staff_role(
    req: _StaffSyncRoleRequest,
    db: DB,
    _: None = Depends(_verify_bot_secret),
):
    """Synchronize a Staff record's role from PostForge.

    Called by PostForge after `transfer_ownership` so the CRM staff role
    matches the new PostForge role without waiting for the affected user
    to re-login. Idempotent — if the staff doesn't exist yet (user never
    opened CRM), this is a no-op.
    """
    valid_roles = {"super_admin", "admin", "operator"}
    if req.new_role not in valid_roles:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid role: {req.new_role}")

    result = await db.execute(
        select(Staff).where(
            Staff.postforge_user_id == req.postforge_user_id,
            Staff.postforge_org_id == req.postforge_org_id,
            Staff.is_active.is_(True),
        )
    )
    staff = result.scalar_one_or_none()
    if not staff:
        # User never opened CRM in this org context — nothing to sync.
        # On first SSO login the role will be set correctly from PostForge.
        return {"ok": True, "synced": False, "reason": "no staff row"}

    if staff.role == req.new_role:
        return {"ok": True, "synced": False, "reason": "already up to date"}

    old_role = staff.role
    staff.role = req.new_role
    await db.commit()

    print(f"[INTERNAL] Synced staff role for pf_user={req.postforge_user_id} org={req.postforge_org_id}: {old_role} -> {req.new_role}", flush=True)
    return {"ok": True, "synced": True, "old_role": old_role, "new_role": req.new_role}


class _ForceDisconnectRequest(PydanticBaseModel):
    crm_account_id: str  # UUID of TgAccount row
    reason: str | None = None


@app.post("/api/internal/force-disconnect-account")
async def internal_force_disconnect(
    req: _ForceDisconnectRequest,
    db: DB,
    _: None = Depends(_verify_bot_secret),
):
    """Forcibly disconnect a TG account on behalf of PostForge.

    Called by the PostForge worker when the monthly CRM charge fails
    (insufficient balance). Mirrors the cleanup the owner would do via
    DELETE /api/tg/disconnect/{account_id}, but driven from the billing
    side without requiring a user session:

    - Telethon client disconnected (removed from _clients map)
    - TgAccount.is_active = False, disconnected_at = now
    - StaffTgAccount links removed (so operators lose access)
    - PostForge billing row flipped to is_active=false so we don't keep
      trying to charge a frozen account

    Idempotent — if the account is already inactive, this is a no-op.
    """
    from uuid import UUID as _UUID
    try:
        account_uuid = _UUID(req.crm_account_id)
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid crm_account_id")

    result = await db.execute(select(TgAccount).where(TgAccount.id == account_uuid))
    account = result.scalar_one_or_none()
    if not account:
        return {"status": "not_found"}

    if not account.is_active:
        return {"status": "already_inactive"}

    account.is_active = False
    account.disconnected_at = datetime.utcnow()
    await db.execute(sa_delete(StaffTgAccount).where(StaffTgAccount.tg_account_id == account_uuid))
    await db.commit()

    # Kill the Telethon session if it's still running in-process.
    try:
        from .telegram import disconnect_account
        await disconnect_account(account_uuid)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"telethon disconnect failed (non-blocking): {e}")

    # Mark PostForge billing row inactive too (same endpoint as the regular
    # user-initiated disconnect flow).
    try:
        await _postforge_crm_billing_disconnect(str(account_uuid))
    except Exception:
        pass

    return {"status": "disconnected", "reason": req.reason}


class _PostForgeContactUpsertRequest(PydanticBaseModel):
    """Body for /api/internal/postforge-contact-upsert.

    Sent by PostForge worker when a user does /start in a campaign bot, so
    that an operator immediately sees the new lead in CRM without waiting
    for the user to write first.
    """
    telegram_user_id: int
    campaign_id: str  # PostForge AdCampaign.id (UUID as str)
    postforge_user_id: str  # owner of the campaign — used to find Staff/org
    postforge_org_id: str | None = None  # explicit org if PostForge has one
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None


@app.post("/api/internal/postforge-contact-upsert")
async def internal_postforge_contact_upsert(
    req: _PostForgeContactUpsertRequest,
    db: DB,
    _: None = Depends(_verify_bot_secret),
):
    """Auto-create or auto-link a Contact when PostForge sees a /start.

    Lookup chain:
      1. If postforge_org_id provided → find Staff with both pf_user_id+org_id
         and pull TgAccount(s) in that org via org_id
      2. Otherwise → first ACTIVE staff for postforge_user_id, then their
         postforge_org_id, then first active TgAccount with that org_id
      3. If no Staff or no TgAccount → return 404. PostForge logs warning,
         /start flow continues unaffected.

    UPSERT key: (tg_account_id, real_tg_id). If contact exists — set
    postforge_campaign_id (don't overwrite other fields). If not — create
    a minimal contact row.
    """
    from telegram import generate_alias as _gen_alias
    from uuid import UUID as _UUID

    try:
        campaign_uuid = _UUID(req.campaign_id)
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid campaign_id")

    # 1. Find Staff
    staff_query = select(Staff).where(
        Staff.postforge_user_id == req.postforge_user_id,
        Staff.is_active.is_(True),
    )
    if req.postforge_org_id:
        staff_query = staff_query.where(Staff.postforge_org_id == req.postforge_org_id)
    staff_query = staff_query.limit(1)
    staff = (await db.execute(staff_query)).scalar_one_or_none()
    if not staff:
        return {"status": "skipped", "reason": "no active staff for postforge_user_id"}

    org_id = staff.postforge_org_id
    if not org_id:
        return {"status": "skipped", "reason": "staff has no postforge_org_id"}

    # 2. Find first active TgAccount in this org
    tg_account = (await db.execute(
        select(TgAccount)
        .where(TgAccount.org_id == org_id, TgAccount.is_active.is_(True))
        .limit(1)
    )).scalar_one_or_none()
    if not tg_account:
        return {"status": "skipped", "reason": "no active TgAccount in org"}

    # 3. UPSERT Contact by (tg_account_id, real_tg_id)
    existing = (await db.execute(
        select(Contact).where(
            Contact.tg_account_id == tg_account.id,
            Contact.real_tg_id == req.telegram_user_id,
        ).limit(1)
    )).scalar_one_or_none()

    if existing:
        # Just link to the campaign. Don't overwrite name/alias/tags etc.
        if existing.postforge_campaign_id != campaign_uuid:
            existing.postforge_campaign_id = campaign_uuid
            await db.commit()
            await db.refresh(existing)
        return {"status": "linked", "contact_id": str(existing.id), "created": False}

    # Build minimal new Contact. Encrypt the real name/username so we don't
    # break the encryption invariant the rest of the codebase relies on.
    real_name = " ".join(
        p for p in (req.first_name, req.last_name) if p
    ).strip() or (req.username or "")
    seq = (await db.execute(select(func.count(Contact.id)))).scalar() + 1
    alias = _gen_alias(real_name, seq)

    new_contact = Contact(
        tg_account_id=tg_account.id,
        real_tg_id=req.telegram_user_id,
        real_name_encrypted=encrypt(real_name) if real_name else None,
        real_username_encrypted=encrypt(req.username) if req.username else None,
        chat_type="private",
        alias=alias,
        status="approved",
        postforge_campaign_id=campaign_uuid,
    )
    db.add(new_contact)
    await db.commit()
    await db.refresh(new_contact)
    print(
        f"[INTERNAL] postforge-contact-upsert created Contact id={new_contact.id} "
        f"alias={alias} tg_user={req.telegram_user_id} campaign={req.campaign_id}",
        flush=True,
    )
    return {"status": "created", "contact_id": str(new_contact.id), "created": True}


class _StatsQueryRequest(PydanticBaseModel):
    pass


@app.get("/api/internal/user-crm-info/{pf_user_id}")
async def internal_user_crm_info(
    pf_user_id: str,
    db: DB,
    _: None = Depends(_verify_bot_secret),
):
    """Return CRM info for a PostForge user — their staff role and the
    TG accounts they can see. Used by the PostForge support panel to show
    support staff what CRM access each user has.

    Scoping rules:
    - super_admin / admin → sees ALL TG accounts in their org
    - operator → only accounts explicitly assigned via staff_tg_accounts
    """
    # Find ACTIVE staff rows for this pf user (may have multiple org contexts)
    staff_result = await db.execute(
        select(Staff).where(
            Staff.postforge_user_id == pf_user_id,
            Staff.is_active.is_(True),
        )
    )
    staff_rows = list(staff_result.scalars().all())
    if not staff_rows:
        from fastapi import HTTPException as _HTTPE
        raise _HTTPE(status_code=404, detail="No active CRM staff for this user")

    # For each staff row (one per org context), collect accessible accounts
    profiles = []
    for s in staff_rows:
        org_id = s.postforge_org_id or "unknown"
        if s.role in ("super_admin", "admin"):
            # All org accounts
            acct_result = await db.execute(
                select(TgAccount)
                .where(TgAccount.org_id == org_id)
                .order_by(TgAccount.connected_at.desc())
            )
            accounts = list(acct_result.scalars().all())
            access_type = "all_org_accounts"
        else:
            # Operator — only assigned accounts
            assigned_subq = (
                select(StaffTgAccount.tg_account_id)
                .where(StaffTgAccount.staff_id == s.id)
                .subquery()
            )
            acct_result = await db.execute(
                select(TgAccount)
                .where(TgAccount.id.in_(select(assigned_subq)))
                .order_by(TgAccount.connected_at.desc())
            )
            accounts = list(acct_result.scalars().all())
            access_type = "assigned_only"

        tg_accounts = [{
            "id": str(a.id),
            "phone": a.phone,
            "display_name": a.display_name,
            "is_active": bool(a.is_active),
            "connected_at": a.connected_at.isoformat() if a.connected_at else None,
        } for a in accounts]

        profiles.append({
            "staff_id": str(s.id),
            "org_id": org_id,
            "role": s.role,
            "is_crm_admin": bool(s.is_crm_admin),
            "signature_mode": s.signature_mode,
            "access_type": access_type,
            "tg_accounts": tg_accounts,
            "tg_accounts_count": len(tg_accounts),
        })

    return {
        "postforge_user_id": pf_user_id,
        "profiles": profiles,
    }


@app.get("/api/internal/tg-accounts")
async def internal_list_tg_accounts(
    db: DB,
    postforge_user_id: str,
    only_active: bool = True,
    _: None = Depends(_verify_bot_secret),
):
    """Flat list of CRM TG accounts a PostForge user can bind to a campaign.

    PostForge calls this when rendering the "Link CRM personal account"
    dropdown in the Edit Campaign modal. The scoping rule mirrors
    /api/internal/user-crm-info — admins see all org accounts, operators
    only their assigned ones — but the result is a flat deduplicated
    list instead of per-org profiles, because the dropdown doesn't care
    about org context, only whether the account is bindable.

    If the user has no active Staff rows, returns an empty list instead
    of 404 — PostForge renders "No CRM accounts available" rather than
    an error toast (solo users who never joined a CRM team).
    """
    staff_result = await db.execute(
        select(Staff).where(
            Staff.postforge_user_id == postforge_user_id,
            Staff.is_active.is_(True),
        )
    )
    staff_rows = list(staff_result.scalars().all())
    if not staff_rows:
        return {"accounts": []}

    # Dedupe across org contexts — same account_id shouldn't show twice
    # if somehow the user has staff rows in two orgs sharing accounts
    # (shouldn't happen today but cheap to guard against).
    seen: set[str] = set()
    out: list[dict] = []

    for s in staff_rows:
        org_id = s.postforge_org_id or "unknown"
        if s.role in ("super_admin", "admin"):
            q = select(TgAccount).where(TgAccount.org_id == org_id)
        else:
            assigned_subq = (
                select(StaffTgAccount.tg_account_id)
                .where(StaffTgAccount.staff_id == s.id)
                .subquery()
            )
            q = select(TgAccount).where(TgAccount.id.in_(select(assigned_subq)))

        if only_active:
            q = q.where(TgAccount.is_active.is_(True))

        q = q.order_by(TgAccount.connected_at.desc())
        acct_result = await db.execute(q)

        for a in acct_result.scalars().all():
            aid = str(a.id)
            if aid in seen:
                continue
            seen.add(aid)
            out.append({
                "id": aid,
                "phone": a.phone,
                "display_name": a.display_name,
                "is_active": bool(a.is_active),
                "connected_at": a.connected_at.isoformat() if a.connected_at else None,
                "org_id": org_id,
            })

    return {"accounts": out}


@app.get("/api/internal/stats")
async def internal_stats(
    db: DB,
    _: None = Depends(_verify_bot_secret),
):
    """Aggregate CRM-wide stats for the PostForge admin Статистика tab.

    Authenticated with the shared POSTFORGE_BOT_SECRET (same as other
    /api/internal/* endpoints). No per-org scoping — this is platform-
    wide telemetry only. Returns zero-like values on empty tables.
    """
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.utcnow()
    day_ago = now - _td(days=1)
    week_ago = now - _td(days=7)
    month_ago = now - _td(days=30)

    # Accounts
    accounts_total = (await db.execute(select(func.count(TgAccount.id)))).scalar() or 0
    accounts_active = (await db.execute(
        select(func.count(TgAccount.id)).where(TgAccount.is_active.is_(True))
    )).scalar() or 0
    accounts_inactive = accounts_total - accounts_active

    # Contacts (= unique chats)
    contacts_total = (await db.execute(select(func.count(Contact.id)))).scalar() or 0
    contacts_approved = (await db.execute(
        select(func.count(Contact.id)).where(Contact.status == "approved")
    )).scalar() or 0
    contacts_pending = (await db.execute(
        select(func.count(Contact.id)).where(Contact.status == "pending")
    )).scalar() or 0
    contacts_archived = (await db.execute(
        select(func.count(Contact.id)).where(Contact.is_archived.is_(True))
    )).scalar() or 0

    # Messages — windowed counts
    msg_total = (await db.execute(select(func.count(Message.id)))).scalar() or 0
    msg_day = (await db.execute(
        select(func.count(Message.id)).where(Message.created_at >= day_ago)
    )).scalar() or 0
    msg_week = (await db.execute(
        select(func.count(Message.id)).where(Message.created_at >= week_ago)
    )).scalar() or 0
    msg_month = (await db.execute(
        select(func.count(Message.id)).where(Message.created_at >= month_ago)
    )).scalar() or 0

    # Direction split last 30d — shows inbound vs outbound ratio
    dir_rows = await db.execute(
        select(Message.direction, func.count(Message.id))
        .where(Message.created_at >= month_ago)
        .group_by(Message.direction)
    )
    by_direction_30d: dict[str, int] = {}
    for d, c in dir_rows.all():
        by_direction_30d[str(d)] = int(c or 0)

    # Staff
    staff_total = (await db.execute(select(func.count(Staff.id)))).scalar() or 0
    staff_active = (await db.execute(
        select(func.count(Staff.id)).where(Staff.is_active.is_(True))
    )).scalar() or 0
    staff_role_rows = await db.execute(
        select(Staff.role, func.count(Staff.id))
        .where(Staff.is_active.is_(True))
        .group_by(Staff.role)
    )
    staff_by_role: dict[str, int] = {
        str(role): int(c or 0) for role, c in staff_role_rows.all()
    }

    # Tags + templates
    tags_total = (await db.execute(select(func.count(Tag.id)))).scalar() or 0
    templates_total = (await db.execute(select(func.count(MessageTemplate.id)))).scalar() or 0

    # Per-account activity — last 24h message counts top 20
    per_acc_rows = await db.execute(
        select(
            TgAccount.id,
            TgAccount.phone,
            TgAccount.display_name,
            TgAccount.is_active,
            func.count(Message.id).label("msgs_24h"),
        )
        .outerjoin(Contact, Contact.tg_account_id == TgAccount.id)
        .outerjoin(
            Message,
            (Message.contact_id == Contact.id) & (Message.created_at >= day_ago),
        )
        .group_by(TgAccount.id)
        .order_by(func.count(Message.id).desc())
        .limit(20)
    )
    per_account: list[dict] = []
    for row in per_acc_rows.all():
        per_account.append({
            "id": str(row.id),
            "phone": row.phone,
            "display_name": row.display_name,
            "is_active": bool(row.is_active),
            "messages_24h": int(row.msgs_24h or 0),
        })

    # Broadcasts summary
    try:
        broadcasts_total = (await db.execute(select(func.count(Broadcast.id)))).scalar() or 0
        broadcasts_30d = (await db.execute(
            select(func.count(Broadcast.id)).where(Broadcast.created_at >= month_ago)
        )).scalar() or 0
    except Exception:
        broadcasts_total = broadcasts_30d = 0

    return {
        "accounts": {
            "total": int(accounts_total),
            "active": int(accounts_active),
            "inactive": int(accounts_inactive),
        },
        "contacts": {
            "total": int(contacts_total),
            "approved": int(contacts_approved),
            "pending": int(contacts_pending),
            "archived": int(contacts_archived),
        },
        "messages": {
            "total": int(msg_total),
            "last_24h": int(msg_day),
            "last_7d": int(msg_week),
            "last_30d": int(msg_month),
            "per_day_avg_30d": round(int(msg_month) / 30, 1),
            "by_direction_30d": by_direction_30d,
        },
        "staff": {
            "total": int(staff_total),
            "active": int(staff_active),
            "by_role": staff_by_role,
        },
        "tags": {"total": int(tags_total)},
        "templates": {"total": int(templates_total)},
        "broadcasts": {
            "total": int(broadcasts_total),
            "last_30d": int(broadcasts_30d),
        },
        "per_account_24h": per_account,
        "generated_at": now.isoformat(),
    }


# ============================================================
# Health
# ============================================================

@app.get("/api/health")
async def health():
    return {"status": "ok"}

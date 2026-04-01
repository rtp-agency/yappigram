"""YappiGram — Main FastAPI application with all routes."""

import asyncio
import os
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


def _validate_upload(file: "UploadFile") -> None:
    """Validate uploaded file extension and content type for security."""
    # Strip path separators from filename to prevent path traversal
    safe_name = os.path.basename(file.filename or "")
    ext = os.path.splitext(safe_name)[1].lower()
    if ext in BLOCKED_EXTENSIONS:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"File type '{ext}' not allowed",
        )
    if ext and ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"File type '{ext}' not allowed",
        )
    ct = (file.content_type or "").lower()
    # Block executable content types
    blocked_ct = {"application/x-executable", "application/x-msdos-program",
                  "application/x-sh", "application/x-shellscript", "application/x-bat",
                  "text/html", "application/javascript", "image/svg+xml"}
    if ct in blocked_ct:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "File type not allowed")

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect, status
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

# --- Rate limiting for auth endpoints ---
_rate_limits: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 30  # per real IP per minute


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

app = FastAPI(title="YappiGram", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Media files served via custom endpoint with security headers
import mimetypes

@app.get("/media/{file_path:path}")
async def serve_media(file_path: str):
    """Serve media files with security headers."""
    from fastapi.responses import FileResponse
    # Prevent path traversal
    safe_path = os.path.join(MEDIA_DIR, file_path)
    safe_path = os.path.abspath(safe_path)
    if not safe_path.startswith(os.path.abspath(MEDIA_DIR)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid path")
    if not os.path.isfile(safe_path):
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    ext = os.path.splitext(safe_path)[1].lower()
    # Never serve HTML or SVG with native content-type
    if ext in (".html", ".htm", ".svg"):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "File type not allowed")

    content_type = mimetypes.guess_type(safe_path)[0] or "application/octet-stream"
    # Override dangerous content types
    if content_type in ("text/html", "image/svg+xml", "application/javascript"):
        content_type = "application/octet-stream"

    # Images served inline, everything else as attachment
    is_image = content_type.startswith("image/")
    disposition = "inline" if is_image else "attachment"
    raw_filename = os.path.basename(safe_path)
    # Strip UUID_msgid prefix from document filenames (e.g. "abc123_456_report.pdf" -> "report.pdf")
    # Telethon saves as "{contactid}_{msgid}{ext}" but for documents may produce "{contactid}_{msgid}_originalname.ext"
    import re
    cleaned = re.sub(r"^[0-9a-f-]+_\d+", "", raw_filename, count=1)
    # Remove leading underscores/dots left after stripping prefix
    cleaned = cleaned.lstrip("_").lstrip(".")
    filename = cleaned if cleaned else raw_filename

    return FileResponse(
        safe_path,
        media_type=content_type,
        headers={
            "X-Content-Type-Options": "nosniff",
            "Content-Disposition": f'{disposition}; filename="{filename}"',
        },
    )


# Type aliases for common dependencies
DB = Annotated[AsyncSession, Depends(get_db)]
CurrentUser = Annotated[Staff, Depends(get_current_user)]
AdminUser = Annotated[Staff, Depends(require_role("super_admin", "admin"))]
SuperAdmin = Annotated[Staff, Depends(require_role("super_admin"))]


def _org_id(user: Staff) -> str | None:
    """Get the effective org_id for data scoping."""
    return user.postforge_org_id


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
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS max_recipients INTEGER;
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS contact_ids UUID[] DEFAULT '{}';
                -- org_id columns for multi-tenancy
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE tags ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS media_path VARCHAR;
                ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS media_type VARCHAR;
                ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                -- Tags: tg_account_id
                ALTER TABLE tags ADD COLUMN IF NOT EXISTS tg_account_id UUID REFERENCES tg_accounts(id);
                -- Pinned chats: org_id for org-wide pins
                ALTER TABLE pinned_chats ADD COLUMN IF NOT EXISTS org_id VARCHAR;
                -- TG accounts: show_real_names, display_name
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS show_real_names BOOLEAN DEFAULT false;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS display_name VARCHAR;
                ALTER TABLE tg_accounts ADD COLUMN IF NOT EXISTS disconnected_at TIMESTAMP;
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
    await startup_listeners()
    asyncio.create_task(start_bot_polling())
    # Auto-sync dialogs for all connected accounts on startup
    await ws_manager.init_redis()
    asyncio.create_task(_auto_sync_on_startup())
    asyncio.create_task(_cleanup_disconnected_accounts())
    asyncio.create_task(_process_scheduled_messages())
    asyncio.create_task(_telethon_health_monitor())
    asyncio.create_task(_cleanup_old_media())


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

    # 1. Find staff by exact tg_user_id (legacy non-SSO staff)
    result = await db.execute(
        select(Staff).where(Staff.tg_user_id == tg_id, Staff.is_active.is_(True))
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

                # Build list of org_ids to link: personal + each org
                org_contexts = [f"personal_{pf_user_id}"]
                for org in pf_orgs:
                    org_contexts.append(str(org["id"]))

                # For each context: find existing staff or create new
                for org_id in org_contexts:
                    is_personal = org_id.startswith("personal_")
                    result = await db.execute(
                        select(Staff).where(
                            Staff.postforge_user_id == pf_user_id,
                            Staff.postforge_org_id == org_id,
                        )
                    )
                    existing = result.scalar_one_or_none()

                    if existing:
                        # Just set real_tg_id and activate
                        existing.real_tg_id = tg_id
                        if not existing.is_active:
                            existing.is_active = True
                        all_staff.append(existing)
                    else:
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

    # Find staff matching this TG user + selected org
    result = await db.execute(
        select(Staff).where(
            Staff.real_tg_id == tg_id,
            Staff.postforge_org_id == req.org_id,
            Staff.is_active.is_(True),
        )
    )
    user = result.scalar_one_or_none()

    # Also try legacy tg_user_id match
    if not user:
        result = await db.execute(
            select(Staff).where(
                Staff.tg_user_id == tg_id,
                Staff.postforge_org_id == req.org_id,
                Staff.is_active.is_(True),
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

    # Verify PostForge token by calling PostForge /api/me
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{settings.POSTFORGE_API_URL}/api/me",
                headers={"Authorization": f"Bearer {req.postforge_token}"},
            )
        if resp.status_code != 200:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid PostForge token")
        pf_user = resp.json()
    except httpx.RequestError as e:
        import logging
        logging.getLogger(__name__).error(f"Cannot reach PostForge: {e}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Cannot reach PostForge")

    pf_user_id = str(pf_user.get("id"))
    pf_email = pf_user.get("email", "")
    pf_nickname = pf_user.get("nickname", "User")
    pf_tg_id = pf_user.get("telegram_id")  # If PostForge user has linked Telegram
    pf_org_id = pf_user.get("organization_id")  # Team/org context
    pf_org_role = pf_user.get("organization_role")  # owner | admin | buyer | editor | custom
    pf_is_org_owner = pf_user.get("is_org_owner", False)

    import hashlib
    # Effective org_id: team UUID or "personal_{user_id}" for solo
    effective_org_id = str(pf_org_id) if pf_org_id else f"personal_{pf_user_id}"
    import logging
    logging.info(f"[SSO] user={pf_user_id} pf_org_id={pf_org_id} effective_org_id={effective_org_id} pf_org_role={pf_org_role}")
    # Unique tg_user_id per org context (hash of user_id + org_id)
    synthetic_tg_id = int(hashlib.sha256(f"{pf_user_id}:{effective_org_id}".encode()).hexdigest()[:15], 16)

    # Determine CRM role based on PostForge context:
    # - Personal (solo) mode → super_admin (full control of own CRM)
    # - Team owner → super_admin
    # - Team admin → admin
    # - Everyone else → operator
    if not pf_org_id:
        crm_role = "super_admin"
    elif pf_is_org_owner:
        crm_role = "super_admin"
    elif pf_org_role in ("owner", "admin"):
        crm_role = "admin"
    else:
        crm_role = "operator"

    # Find existing Staff by (postforge_user_id, postforge_org_id) pair
    result = await db.execute(
        select(Staff).where(
            Staff.postforge_user_id == pf_user_id,
            Staff.postforge_org_id == effective_org_id,
            Staff.is_active.is_(True),
        )
    )
    user = result.scalar_one_or_none()

    # Auto-create staff for SSO users
    if not user:
        user = Staff(
            tg_user_id=synthetic_tg_id,
            tg_username=pf_email.split("@")[0] if pf_email else None,
            role=crm_role,
            name=pf_nickname or pf_email.split("@")[0],
            postforge_user_id=pf_user_id,
            postforge_org_id=effective_org_id,
            real_tg_id=pf_tg_id,  # Store real TG ID for Mini App auth
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
    else:
        # Sync role, name, and real_tg_id from PostForge on each login
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
        if changed:
            await db.commit()

    return TokenResponse(
        access_token=create_token(user.id, "access"),
        refresh_token=create_token(user.id, "refresh"),
        role=user.role,
    )


# ============================================================
# Telegram Accounts
# ============================================================

MAX_TG_ACCOUNTS = 5  # test version limit

@app.post("/api/tg/connect")
async def tg_connect(req: TgConnectRequest, request: Request, user: AdminUser, db: DB):
    check_rate_limit(request)
    # Check account limit
    count = await db.execute(
        select(func.count(TgAccount.id)).where(TgAccount.org_id == _org_id(user))
    )
    if count.scalar() >= MAX_TG_ACCOUNTS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Максимум {MAX_TG_ACCOUNTS} аккаунтов")
    result = await start_connect(req.phone)
    return {"status": "code_sent", "debug": result}


@app.post("/api/tg/verify", response_model=TgAccountOut)
async def tg_verify(req: TgVerifyRequest, user: CurrentUser, db: DB):
    account = await verify_code(req.phone, req.code, req.password_2fa)
    # Set org_id on the newly connected account
    result = await db.execute(select(TgAccount).where(TgAccount.id == account.id))
    tg_acc = result.scalar_one_or_none()
    if tg_acc:
        tg_acc.org_id = _org_id(user)
        await db.commit()
    # Auto-sync ALL dialogs in background after connecting
    asyncio.create_task(_do_sync_dialogs(account.id, 500))
    return account


@app.get("/api/tg/status", response_model=list[TgAccountOut])
async def tg_status(user: CurrentUser, db: DB):
    from telegram import _clients
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


@app.delete("/api/tg/disconnect/{account_id}")
async def tg_disconnect(account_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(TgAccount).where(TgAccount.id == account_id, TgAccount.org_id == _org_id(user)))
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Mark as inactive with timestamp (data kept for 30 days)
    account.is_active = False
    account.disconnected_at = datetime.now(timezone.utc)

    # Unlink from staff
    await db.execute(sa_delete(StaffTgAccount).where(StaffTgAccount.tg_account_id == account_id))

    await db.commit()
    try:
        await disconnect_account(account_id)
    except Exception:
        pass  # Session may be expired — DB cleanup already done
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
    tg_account_id: UUID | None = None,
    archived: bool = Query(False),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
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

    query = query.order_by(Contact.last_message_at.desc().nullslast())
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    contacts = list(result.scalars().all())

    # Build per-account show_real_names map
    acct_result = await db.execute(
        select(TgAccount.id, TgAccount.show_real_names).where(TgAccount.org_id == _org_id(user))
    )
    show_real_map = {row[0]: row[1] for row in acct_result.all()}

    # Fetch last message content for each contact (batch query)
    contact_ids = [c.id for c in contacts]
    last_msg_map: dict = {}
    if contact_ids:
        from sqlalchemy import func as sa_func
        # Subquery: latest message per contact using created_at (MAX on UUID not supported in PG)
        latest_sub = (
            select(Message.contact_id, sa_func.max(Message.created_at).label("max_ts"))
            .where(Message.contact_id.in_(contact_ids))
            .group_by(Message.contact_id)
            .subquery()
        )
        msg_result = await db.execute(
            select(Message.contact_id, Message.content, Message.media_type, Message.direction, Message.is_read)
            .join(latest_sub, (Message.contact_id == latest_sub.c.contact_id) & (Message.created_at == latest_sub.c.max_ts))
        )
        for row in msg_result.all():
            cid = row[0]
            if cid in last_msg_map:
                continue
            content = row[1]
            if not content and row[2]:
                content = f"[{row[2]}]"
            last_msg_map[cid] = ((content or "")[:100], row[3], row[4])

    last_msg_dir_map: dict = {}
    for c in contacts:
        if c.chat_type != "private":
            # Groups/channels — always show real title
            title = decrypt(c.group_title_encrypted) if c.group_title_encrypted else None
            if title:
                c.alias = title
        elif show_real_map.get(c.tg_account_id, False):
            real_name = decrypt(c.real_name_encrypted) if c.real_name_encrypted else None
            if real_name:
                c.alias = real_name
        entry = last_msg_map.get(c.id)
        if entry:
            c.last_message_content = entry[0]
            c.last_message_direction = entry[1]
            c.last_message_is_read = entry[2]
        else:
            c.last_message_content = None
            c.last_message_direction = None
            c.last_message_is_read = None

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
    if req.is_archived is not None:
        contact.is_archived = req.is_archived

    await db.commit()
    await db.refresh(contact)

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
    org = _org_id(user)
    result = await db.execute(
        select(PinnedChat.contact_id).where(PinnedChat.org_id == org)
    )
    return [str(row[0]) for row in result.all()]


@app.post("/api/pinned/{contact_id}", status_code=204)
async def pin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    org = _org_id(user)
    existing = await db.execute(
        select(PinnedChat).where(PinnedChat.org_id == org, PinnedChat.contact_id == contact_id)
    )
    if existing.scalar_one_or_none():
        return
    db.add(PinnedChat(staff_id=user.id, contact_id=contact_id, org_id=org))
    await db.commit()


@app.delete("/api/pinned/{contact_id}", status_code=204)
async def unpin_chat(contact_id: UUID, user: Annotated[Staff, Depends(get_current_user)], db: DB):
    from sqlalchemy import delete as sa_delete
    org = _org_id(user)
    await db.execute(
        sa_delete(PinnedChat).where(PinnedChat.org_id == org, PinnedChat.contact_id == contact_id)
    )
    await db.commit()


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

    # Resolve CRM contact IDs to real TG IDs (org-scoped)
    member_tg_ids = []
    for cid in req.member_contact_ids:
        r = await db.execute(select(Contact).where(
            Contact.id == cid,
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        ))
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

    return messages


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
    await db.commit()
    await db.refresh(msg)

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

    _validate_upload(file)

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
    filepath = os.path.abspath(filepath)
    if not filepath.startswith(os.path.abspath(MEDIA_DIR)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")
    data = await file.read()
    MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
    if len(data) > MAX_UPLOAD_SIZE:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "File too large (max 50MB)")
    with open(filepath, "wb") as f:
        f.write(data)

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
    await db.commit()
    await db.refresh(msg)
    return msg


@app.post("/api/messages/{contact_id}/send-template-media", response_model=MessageOut)
async def send_template_media(contact_id: UUID, user: CurrentUser, db: DB, template_id: UUID = Query(...)):
    """Send a message using a template's media and text."""
    contact = await _get_contact_with_access(contact_id, user, db)
    if contact.status != "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contact not approved")

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
    await db.commit()
    await db.refresh(msg)
    return msg


@app.patch("/api/messages/{contact_id}/read")
async def mark_read(contact_id: UUID, user: CurrentUser, db: DB):
    """Mark all unread incoming messages in a contact chat as read."""
    # Verify contact belongs to user's org
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    from sqlalchemy import update
    await db.execute(
        update(Message)
        .where(Message.contact_id == contact_id, Message.direction == "incoming", Message.is_read.is_(False))
        .values(is_read=True)
    )
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

    # Get source messages (keep order, preserve content/media for CRM records)
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
    # Verify staff exists in same org
    result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.postforge_org_id == _org_id(user)))
    if not result.scalar_one_or_none():
        raise HTTPException(status.HTTP_404_NOT_FOUND)

    # Delete existing assignments
    from sqlalchemy import delete
    await db.execute(delete(StaffTgAccount).where(StaffTgAccount.staff_id == staff_id))

    # Create new assignments
    for acc_id in account_ids:
        db.add(StaffTgAccount(staff_id=staff_id, tg_account_id=acc_id))

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
async def create_tag(req: TagCreate, user: AdminUser, db: DB):
    tag = Tag(name=req.name, color=req.color, created_by=user.id, org_id=_org_id(user), tg_account_id=req.tg_account_id)
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return tag


@app.delete("/api/tags/{tag_id}")
async def delete_tag(tag_id: UUID, user: AdminUser, db: DB):
    result = await db.execute(select(Tag).where(Tag.id == tag_id, Tag.org_id == _org_id(user)))
    tag = result.scalar_one_or_none()
    if not tag:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    # Remove tag from contacts that have it
    tag_name = tag.name
    contacts_result = await db.execute(
        select(Contact).where(Contact.tags.any(tag_name))
    )
    for contact in contacts_result.scalars().all():
        contact.tags = [t for t in contact.tags if t != tag_name]
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
async def create_template(req: TemplateCreate, user: AdminUser, db: DB):
    tpl = MessageTemplate(
        title=req.title,
        content=req.content,
        category=req.category,
        shortcut=req.shortcut,
        tg_account_id=req.tg_account_id,
        created_by=user.id,
        org_id=_org_id(user),
    )
    db.add(tpl)
    await db.commit()
    await db.refresh(tpl)
    return tpl


@app.patch("/api/templates/{template_id}", response_model=TemplateOut)
async def update_template(template_id: UUID, req: TemplateUpdate, user: AdminUser, db: DB):
    result = await db.execute(select(MessageTemplate).where(MessageTemplate.id == template_id, MessageTemplate.org_id == _org_id(user)))
    tpl = result.scalar_one_or_none()
    if not tpl:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    for field, val in req.model_dump(exclude_unset=True).items():
        setattr(tpl, field, val)
    await db.commit()
    await db.refresh(tpl)
    return tpl


@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: UUID, user: AdminUser, db: DB):
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
    user: AdminUser,
    db: DB,
    file: UploadFile = File(...),
    send_as: str = Query("auto", description="auto|photo|video|video_note|voice|document"),
):
    _validate_upload(file)

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
    with open(filepath, "wb") as f:
        f.write(content)

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
                "-t", "60", "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=384:384",
                "-c:v", "libx264", "-preset", "fast", "-crf", "28", "-an",
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
    return {"status": "archived"}


@app.post("/api/contacts/{contact_id}/unarchive")
async def unarchive_contact(contact_id: UUID, user: CurrentUser, db: DB):
    result = await db.execute(select(Contact).where(Contact.id == contact_id, Contact.tg_account_id.in_(_org_accounts_subq(user))))
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    contact.is_archived = False
    await db.commit()
    return {"status": "unarchived"}


# ============================================================
# Avatars (proxy Telegram profile photos)
# ============================================================

@app.get("/api/contacts/{contact_id}/avatar")
async def get_contact_avatar(contact_id: UUID, db: DB, token: str = Query("")):
    """Download and return the Telegram avatar for a contact."""
    from fastapi.responses import FileResponse
    # Verify token (passed as query param since <img src> can't send headers)
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED)
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED)

    # Verify contact belongs to user's org
    staff_id = payload.get("sub")
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

    avatar_dir = os.path.join(MEDIA_DIR, "avatars")
    os.makedirs(avatar_dir, exist_ok=True)
    avatar_path = os.path.join(avatar_dir, f"{contact_id}.jpg")

    # Check cache (refresh every 24h)
    if os.path.exists(avatar_path):
        import time
        age = time.time() - os.path.getmtime(avatar_path)
        if age < 86400:  # 24h cache
            return FileResponse(avatar_path, media_type="image/jpeg")

    # Download from Telegram
    from telegram import _clients
    client = _clients.get(contact.tg_account_id)
    if not client:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not connected")

    try:
        photo = await client.download_profile_photo(contact.real_tg_id, file=avatar_path)
        if photo:
            return FileResponse(avatar_path, media_type="image/jpeg")
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

@app.post("/api/translate")
async def translate_text(req: TranslateRequest, user: CurrentUser):
    """Translate text using free Google Translate (via googletrans-like API)."""
    import httpx
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
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Translation failed: {e}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Translation failed")
    raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Translation service unavailable")


# ============================================================
# Broadcasts
# ============================================================

@app.get("/api/broadcasts", response_model=list[BroadcastOut])
async def list_broadcasts(user: CurrentUser, db: DB):
    result = await db.execute(select(Broadcast).where(Broadcast.org_id == _org_id(user)).order_by(Broadcast.created_at.desc()))
    return result.scalars().all()


@app.post("/api/broadcasts", response_model=BroadcastOut)
async def create_broadcast(req: BroadcastCreate, user: CurrentUser, db: DB):
    bc = Broadcast(
        title=req.title,
        content=req.content,
        tg_account_id=req.tg_account_id,
        tag_filter=req.tag_filter,
        delay_seconds=max(1, min(3600, req.delay_seconds)),
        max_recipients=req.max_recipients,
        contact_ids=req.contact_ids or [],
        created_by=user.id,
        org_id=_org_id(user),
    )
    db.add(bc)
    await db.commit()
    await db.refresh(bc)
    return bc


@app.post("/api/broadcasts/{broadcast_id}/upload-media")
async def broadcast_upload_media(
    broadcast_id: UUID,
    user: CurrentUser,
    db: DB,
    file: UploadFile = File(...),
    send_as: str = Query("auto", description="auto|photo|video|video_note|voice|document"),
):
    _validate_upload(file)

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
    with open(filepath, "wb") as f:
        f.write(content)

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
                "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=384:384",
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
    result = await db.execute(select(Broadcast).where(Broadcast.id == broadcast_id, Broadcast.org_id == _org_id(user)))
    bc = result.scalar_one_or_none()
    if not bc:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if bc.status not in ("draft", "paused"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Cannot start from status {bc.status}")

    # Build recipient list
    import random as _random
    if bc.contact_ids:
        # Manual selection — use specified contacts (private only)
        # Validate all contact_ids belong to user's org
        result = await db.execute(
            select(Contact).where(
                Contact.id.in_(bc.contact_ids),
                Contact.tg_account_id.in_(_org_accounts_subq(user)),
                Contact.tg_account_id == bc.tg_account_id,
                Contact.status == "approved",
                Contact.chat_type == "private",
                Contact.is_archived.is_(False),
            )
        )
        contacts = list(result.scalars().all())
    else:
        q = select(Contact).where(
            Contact.tg_account_id == bc.tg_account_id,
            Contact.status == "approved",
            Contact.chat_type == "private",
            Contact.is_archived.is_(False),
        )
        if bc.tag_filter:
            q = q.where(Contact.tags.overlap(bc.tag_filter))
        result = await db.execute(q)
        contacts = list(result.scalars().all())

    # Random N from filtered set
    if bc.max_recipients and len(contacts) > bc.max_recipients:
        contacts = _random.sample(contacts, bc.max_recipients)

    if not contacts:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No matching recipients")

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
    bc.delay_seconds = max(1, min(3600, req.delay_seconds))
    bc.max_recipients = req.max_recipients
    bc.contact_ids = req.contact_ids or []
    await db.commit()
    await db.refresh(bc)
    return bc


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


async def _run_broadcast(broadcast_id: UUID):
    """Background task to send broadcast messages with delay."""
    from models import async_session
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
                recip.error = "Contact not found"
                bc.failed_count += 1
                await db.commit()
                continue

            try:
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

                recip.status = "sent"
                recip.sent_at = datetime.utcnow()
                bc.sent_count += 1
            except Exception as e:
                recip.status = "failed"
                recip.error = str(e)[:500]
                bc.failed_count += 1

            await db.commit()

            # Broadcast progress via WS
            await ws_manager.broadcast_to_admins({
                "type": "broadcast_progress",
                "broadcast_id": str(broadcast_id),
                "sent": bc.sent_count,
                "failed": bc.failed_count,
                "total": bc.total_recipients,
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


# ============================================================
# Load Old Dialogs
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
            imported = await _do_sync_dialogs(account.id, 500)  # None = no limit
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


async def _do_sync_dialogs(account_id: UUID, limit: int | None = 500) -> int:
    """Background-safe: import dialogs for a TG account. Returns count imported."""
    from telegram import _clients, generate_alias, _extract_media, sanitize_text
    from crypto import encrypt

    client = _clients.get(account_id)
    if not client:
        print(f"[SYNC] Account {account_id} not connected, skipping")
        return 0

    me = await client.get_me()
    imported = 0

    async with async_session() as db:
        # Verify account exists
        result = await db.execute(select(TgAccount).where(TgAccount.id == account_id))
        account = result.scalar_one_or_none()
        if not account:
            print(f"[SYNC] Account {account_id} not found in DB")
            return 0

        async for dialog in client.iter_dialogs(limit=limit):
            peer_id = dialog.id
            if not peer_id:
                continue

            # Skip Telegram service account
            if peer_id == 777000:
                continue

            # Update last_message_at for existing contacts from Telegram dialog date
            result = await db.execute(
                select(Contact).where(Contact.tg_account_id == account_id, Contact.real_tg_id == peer_id)
            )
            existing = result.scalars().first()
            if existing:
                # Sync last_message_at from Telegram if more recent
                dialog_date = getattr(dialog, "date", None)
                if dialog_date:
                    tg_ts = dialog_date.replace(tzinfo=None) if dialog_date.tzinfo else dialog_date
                    if not existing.last_message_at or tg_ts > existing.last_message_at:
                        existing.last_message_at = tg_ts
                continue

            # Determine chat type
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

            # Get name
            name = dialog.name or ""
            username = getattr(entity, "username", None)

            # Generate alias
            seq_result = await db.execute(select(func.count(Contact.id)))
            seq = seq_result.scalar() + 1

            alias_base = generate_alias(name, seq)
            while True:
                check = await db.execute(select(Contact).where(Contact.alias == alias_base))
                if not check.scalar_one_or_none():
                    break
                seq += 1
                alias_base = generate_alias(name, seq)

            dialog_date = getattr(dialog, "date", None)
            last_msg_at = dialog_date.replace(tzinfo=None) if dialog_date and dialog_date.tzinfo else dialog_date
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
                last_message_at=last_msg_at,
            )
            db.add(contact)
            await db.commit()
            await db.refresh(contact)

            # Import last N messages from this dialog
            msg_limit = 200 if is_forum else 30
            try:
                msgs = await client.get_messages(peer_id, limit=msg_limit)
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
                            fname = f"{contact.id}_{msg_obj.id}{ext or ''}"
                            dl_path = os.path.join(MEDIA_DIR, fname)
                            await client.download_media(msg_obj, file=dl_path)
                            media_path = fname
                        except Exception:
                            pass

                    # Sender info for group messages
                    sender_tg_id_val = None
                    sender_alias_val = None
                    if chat_type != "private" and direction == "incoming":
                        sender = getattr(msg_obj, "sender", None)
                        if sender:
                            sender_tg_id_val = getattr(sender, "id", None)
                            fname = getattr(sender, "first_name", "") or ""
                            lname = getattr(sender, "last_name", "") or ""
                            title = getattr(sender, "title", "") or ""
                            sender_alias_val = (f"{fname} {lname}".strip() or title or "User")

                    # Topic info for forum supergroups
                    topic_id_val = None
                    topic_name_val = None
                    if is_forum and hasattr(msg_obj, "reply_to") and msg_obj.reply_to:
                        rt = msg_obj.reply_to
                        if getattr(rt, "forum_topic", False):
                            topic_id_val = getattr(rt, "reply_to_msg_id", None)
                        else:
                            topic_id_val = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
                    elif is_forum:
                        topic_id_val = 1  # General topic

                    if topic_id_val is not None:
                        from telegram import _resolve_topic_name
                        topic_name_val = await _resolve_topic_name(client, peer_id, topic_id_val, account_id)

                    # Forwarded from
                    fwd_alias = None
                    if msg_obj.forward:
                        fwd_sender = msg_obj.forward.sender
                        if fwd_sender:
                            fn = getattr(fwd_sender, "first_name", "") or ""
                            ln = getattr(fwd_sender, "last_name", "") or ""
                            tt = getattr(fwd_sender, "title", "") or ""
                            fwd_alias = (f"{fn} {ln}".strip() or tt or "User")

                    # Strip timezone to match naive datetime columns
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
                        topic_id=topic_id_val,
                        topic_name=topic_name_val,
                        forwarded_from_alias=fwd_alias,
                        created_at=msg_date,
                    )
                    db.add(db_msg)

                await db.commit()
            except Exception as e:
                await db.rollback()
                print(f"[SYNC] Failed to import messages for {peer_id}: {e}")

            imported += 1
            # Small delay to avoid overwhelming DB/Telegram
            if imported % 10 == 0:
                await asyncio.sleep(0.5)

        # Commit any last_message_at updates for existing contacts
        await db.commit()

    print(f"[SYNC] Finished: imported {imported} dialogs for account {account_id}")
    return imported


@app.post("/api/tg/{account_id}/sync-dialogs")
async def sync_old_dialogs(account_id: UUID, user: AdminUser, db: DB):
    """Trigger full dialog sync as a background task. Syncs ALL dialogs."""
    from telegram import _clients
    client = _clients.get(account_id)
    if not client:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Account not connected")
    asyncio.create_task(_do_sync_dialogs(account_id, 500))  # Max 500 dialogs per sync
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
    from_date: str = Query(..., description="ISO date, e.g. 2026-03-20"),
    to_date: str = Query(..., description="ISO date, e.g. 2026-03-23"),
    tg_account_id: UUID | None = Query(None),
    timezone: str = Query("UTC"),
):
    """Report: new chats (contacts) created in a date range, grouped by day and account."""
    from datetime import date as date_type

    # Validate timezone
    if timezone not in VALID_TIMEZONES:
        timezone = "UTC"

    try:
        start = datetime.fromisoformat(from_date)
        end = datetime.fromisoformat(to_date).replace(hour=23, minute=59, second=59)
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid date format. Use YYYY-MM-DD.")

    org = _org_id(user)
    org_accounts = select(TgAccount.id).where(TgAccount.org_id == org)

    # Base filter
    base_filter = [
        Contact.tg_account_id.in_(org_accounts),
        Contact.created_at >= start,
        Contact.created_at <= end,
    ]
    if tg_account_id:
        base_filter.append(Contact.tg_account_id == tg_account_id)

    # Total count
    total_q = select(func.count(Contact.id)).where(*base_filter)
    total_result = await db.execute(total_q)
    total = total_result.scalar() or 0

    # By day — convert created_at to target timezone, then group by date
    day_expr = func.date(Contact.created_at.op("AT TIME ZONE")("UTC").op("AT TIME ZONE")(timezone))
    by_day_q = (
        select(day_expr.label("day"), func.count(Contact.id).label("cnt"))
        .where(*base_filter)
        .group_by(day_expr)
        .order_by(day_expr)
    )
    by_day_result = await db.execute(by_day_q)
    by_day = [{"date": str(row.day), "count": row.cnt} for row in by_day_result.all()]

    # By account
    by_account_q = (
        select(
            TgAccount.id.label("account_id"),
            TgAccount.phone,
            TgAccount.display_name,
            func.count(Contact.id).label("cnt"),
        )
        .join(TgAccount, Contact.tg_account_id == TgAccount.id)
        .where(*base_filter)
        .group_by(TgAccount.id, TgAccount.phone, TgAccount.display_name)
        .order_by(func.count(Contact.id).desc())
    )
    by_account_result = await db.execute(by_account_q)
    by_account = [
        {
            "account_id": str(row.account_id),
            "phone": row.phone,
            "display_name": row.display_name,
            "count": row.cnt,
        }
        for row in by_account_result.all()
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

async def _handle_ws(ws: WebSocket, token: str):
    """Shared WebSocket handler."""
    payload = decode_token(token)
    if payload.get("type") != "access":
        await ws.close(code=4001)
        return

    staff_id = UUID(payload["sub"])

    # Verify user exists and is active
    from models import async_session as get_session
    async with get_session() as db:
        result = await db.execute(select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True)))
        user = result.scalar_one_or_none()
        if not user:
            await ws.close(code=4003)
            return

    await ws_manager.connect(staff_id, ws, org_id=user.postforge_org_id)
    try:
        while True:
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=45)
                # Respond to ping with pong
                if data == "ping":
                    await ws.send_text("pong")
            except asyncio.TimeoutError:
                # Send keepalive ping if no data received
                try:
                    await ws.send_text('{"type":"ping"}')
                except Exception:
                    break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        ws_manager.disconnect(staff_id, ws)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, token: str = Query(...)):
    await _handle_ws(ws, token)


@app.websocket("/crm/ws")
async def websocket_endpoint_crm(ws: WebSocket, token: str = Query(...)):
    await _handle_ws(ws, token)


# ============================================================
# Health
# ============================================================

@app.get("/api/health")
async def health():
    return {"status": "ok"}

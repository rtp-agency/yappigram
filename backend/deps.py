"""Shared dependencies for all route modules."""

import os
import time
from collections import defaultdict
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, get_db
from config import settings
from models import Contact, Staff, StaffTgAccount, TgAccount, async_session

# Type aliases
CurrentUser = Annotated[Staff, Depends(get_current_user)]
AdminUser = Annotated[Staff, Depends(lambda: None)]  # Will be overridden
DB = Annotated[AsyncSession, Depends(get_db)]

MEDIA_DIR = "media"
os.makedirs(MEDIA_DIR, exist_ok=True)

MAX_TG_ACCOUNTS = 50

# --- Rate limiting ---
_rate_limits: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 30


def _get_real_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip", "")
    if real_ip:
        return real_ip
    return request.client.host if request.client else "unknown"


def check_rate_limit(request: Request):
    ip = _get_real_ip(request)
    now = time.time()
    _rate_limits[ip] = [t for t in _rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limits[ip]) >= RATE_LIMIT_MAX:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many requests")
    _rate_limits[ip].append(now)


def _org_id(user: Staff) -> str | None:
    return user.postforge_org_id


def _org_accounts_subq(user: Staff):
    """Subquery: TG account IDs belonging to user's org."""
    return select(TgAccount.id).where(
        TgAccount.org_id == _org_id(user),
        TgAccount.is_active.is_(True),
    )


async def _get_contact_with_access(contact_id: UUID, user: Staff, db: AsyncSession) -> Contact:
    """Get a contact, ensuring it belongs to user's org."""
    result = await db.execute(
        select(Contact).where(
            Contact.id == contact_id,
            Contact.tg_account_id.in_(_org_accounts_subq(user)),
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Access denied")
    return contact

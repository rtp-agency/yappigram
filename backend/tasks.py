"""Background tasks for YappiGram CRM.

All long-running asyncio tasks:
- _auto_sync_on_startup: sync TG dialogs on startup
- _process_scheduled_messages: send due scheduled messages every 30s
- _telethon_health_monitor: check Telethon connections every 60s
- _cleanup_old_media: delete old media files daily
- _cleanup_disconnected_accounts: purge data for accounts disconnected >30 days
"""

import asyncio
import glob
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import func, select, delete as sa_delete

from config import settings
from deps import MEDIA_DIR
from models import (
    Contact, Message, MessageTemplate, PinnedChat, AuditLog,
    BroadcastRecipient, ScheduledMessage, Staff, Tag, TgAccount,
    async_session,
)
from telegram import send_message, _clients, _try_reconnect
from ws import ws_manager


async def auto_sync_on_startup():
    """Auto-sync ALL dialogs for all connected accounts on every startup."""
    await asyncio.sleep(3)
    async with async_session() as db:
        result = await db.execute(
            select(TgAccount).where(TgAccount.is_active.is_(True))
        )
        accounts = result.scalars().all()

    for account in accounts:
        try:
            print(f"[AUTO-SYNC] Syncing all dialogs for {account.phone}...")
            from app import _do_sync_dialogs
            imported = await _do_sync_dialogs(account.id, 500)
            print(f"[AUTO-SYNC] {account.phone}: imported {imported} new dialogs")
        except Exception as e:
            print(f"[AUTO-SYNC] {account.phone}: error: {e}")


async def process_scheduled_messages():
    """Check for due scheduled messages every 30 seconds and send them."""
    await asyncio.sleep(5)
    while True:
        try:
            async with async_session() as db:
                now = datetime.utcnow()
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


async def telethon_health_monitor():
    """Check Telethon client connections every 60s, reconnect if needed."""
    await asyncio.sleep(30)
    while True:
        try:
            for account_id, client in list(_clients.items()):
                if not client.is_connected():
                    async with async_session() as db:
                        result = await db.execute(
                            select(TgAccount).where(
                                TgAccount.id == account_id,
                                TgAccount.is_active.is_(True),
                            )
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
                                    "type": "account_status",
                                    "account_id": str(account_id),
                                    "connected": True,
                                })
                            else:
                                print(f"[HEALTH] {account.phone} reconnect failed")
                        except Exception as e:
                            print(f"[HEALTH] {account.phone} reconnect error: {e}")
        except Exception as e:
            print(f"[HEALTH] Monitor error: {e}")
        await asyncio.sleep(60)


async def cleanup_old_media():
    """Delete media files older than 60 days. Runs daily."""
    await asyncio.sleep(300)
    while True:
        try:
            cutoff = datetime.utcnow() - timedelta(days=60)
            async with async_session() as db:
                result = await db.execute(
                    select(Message.media_path).where(
                        Message.media_path.isnot(None),
                        Message.created_at >= cutoff,
                    )
                )
                recent_paths = {r[0] for r in result.all()}

            media_files = glob.glob(os.path.join(MEDIA_DIR, "*"))
            deleted = 0
            for filepath in media_files:
                filename = os.path.basename(filepath)
                if filename in recent_paths:
                    continue
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
        await asyncio.sleep(86400)


async def cleanup_disconnected_accounts():
    """Delete data for accounts disconnected more than 30 days ago. Runs daily."""
    while True:
        try:
            async with async_session() as db:
                cutoff = datetime.utcnow() - timedelta(days=30)
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
                    contact_rows = await db.execute(
                        select(Contact.id).where(Contact.tg_account_id == aid)
                    )
                    cids = [r[0] for r in contact_rows.all()]
                    if cids:
                        await db.execute(sa_delete(Message).where(Message.contact_id.in_(cids)))
                        await db.execute(sa_delete(PinnedChat).where(PinnedChat.contact_id.in_(cids)))
                        await db.execute(sa_delete(AuditLog).where(AuditLog.target_contact_id.in_(cids)))
                        await db.execute(sa_delete(BroadcastRecipient).where(BroadcastRecipient.contact_id.in_(cids)))
                        await db.execute(sa_delete(Contact).where(Contact.tg_account_id == aid))
                    await db.execute(sa_delete(MessageTemplate).where(MessageTemplate.tg_account_id == aid))
                    await db.execute(sa_delete(Tag).where(Tag.tg_account_id == aid))
                    await db.execute(sa_delete(TgAccount).where(TgAccount.id == aid))
                    print(f"[CLEANUP] Deleted expired disconnected account {acc.phone} ({aid})")
                if expired:
                    await db.commit()
        except Exception as e:
            print(f"[CLEANUP] Error: {e}")
        await asyncio.sleep(86400)

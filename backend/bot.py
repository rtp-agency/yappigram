"""YappiGram Telegram bot (aiogram 3.x).

Features:
- Invite-based staff onboarding via /start <code>
- Notifications about new chats/groups with approve/block buttons
- Assign operators to contacts from the bot
- Stats command
- Staff with access can interact with the bot
"""

from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message as TgMessage,
    WebAppInfo,
)
from sqlalchemy import func as sqlfunc, select

from config import settings
from models import BotInvite, Contact, Staff, async_session

bot: Bot | None = None
dp = Dispatcher()

_bot_username: str = ""


def get_bot() -> Bot:
    global bot
    if bot is None:
        if not settings.TG_BOT_TOKEN:
            raise RuntimeError("TG_BOT_TOKEN is not set")
        bot = Bot(token=settings.TG_BOT_TOKEN)
    return bot


def get_bot_username() -> str:
    return _bot_username


async def _has_access(tg_user_id: int) -> bool:
    """Check if user has a Staff record."""
    async with async_session() as db:
        result = await db.execute(
            select(Staff).where(Staff.tg_user_id == tg_user_id, Staff.is_active.is_(True))
        )
        return result.scalar_one_or_none() is not None


def _is_admin(chat_id: int) -> bool:
    return chat_id == settings.TG_ADMIN_CHAT_ID


def _mini_app_button() -> InlineKeyboardMarkup:
    if not settings.WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Open YappiGram",
            web_app=WebAppInfo(url=settings.WEBAPP_URL),
        )]
    ])


# ============================================================
# Notifications (called from telegram.py)
# ============================================================

async def notify_new_contact(
    contact: "Contact",
    real_name: str | None,
    username: str | None,
    first_message: str | None,
    chat_type: str = "private",
) -> None:
    """Send moderation card to admin chat."""
    if not settings.TG_BOT_TOKEN or not settings.TG_ADMIN_CHAT_ID:
        return

    alias = _esc(contact.alias)
    if chat_type in ("group", "channel"):
        type_label = "Новая группа" if chat_type == "group" else "Новый канал"
        text = (
            f"📥 <b>{type_label}</b>\n\n"
            f"Псевдоним: <b>{alias}</b>\n"
            f"Название: {_esc(real_name or '—')}\n"
            f"ID: <code>{contact.real_tg_id}</code>"
        )
    else:
        username_line = f"Username: @{_esc(username)}\n" if username else ""
        text = (
            f"📥 <b>Новый клиент</b>\n\n"
            f"Псевдоним: <b>{alias}</b>\n"
            f"Имя: {_esc(real_name or '—')}\n"
            f"{username_line}"
            f"ID: <code>{contact.real_tg_id}</code>\n"
            f"Сообщение: <i>«{_esc(first_message or '—')}»</i>"
        )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{contact.id}"),
            InlineKeyboardButton(text="❌ Заблокировать", callback_data=f"block:{contact.id}"),
        ],
        [
            InlineKeyboardButton(text="👤 Назначить оператора", callback_data=f"assign:{contact.id}"),
        ],
    ])

    await get_bot().send_message(
        settings.TG_ADMIN_CHAT_ID,
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


def _esc(text: str) -> str:
    """Escape HTML special chars for Telegram."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def notify_new_message(
    contact: "Contact",
    message_text: str | None,
    sender_alias: str | None = None,
    assigned_to=None,
    tg_account_id=None,
) -> None:
    """Send TG bot notification to the assigned operator (if offline) about a new message."""
    if not settings.TG_BOT_TOKEN:
        return

    from models import Staff, StaffTgAccount, async_session
    from ws import ws_manager

    staff_id = assigned_to
    acct_id = tg_account_id

    # Collect target operators to notify
    targets: list[Staff] = []

    async with async_session() as db:
        if staff_id:
            # Direct assignment
            result = await db.execute(
                select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True))
            )
            op = result.scalar_one_or_none()
            if op:
                targets.append(op)
        elif acct_id:
            # Find ONLY operators (not admins/super_admins) via staff_tg_accounts link
            result = await db.execute(
                select(Staff).join(StaffTgAccount, StaffTgAccount.staff_id == Staff.id)
                .where(StaffTgAccount.tg_account_id == acct_id, Staff.is_active.is_(True), Staff.role == "operator")
            )
            targets = list(result.scalars().all())
            if targets:
                print(f"[NOTIFY] Found {len(targets)} operator(s) via tg_account for {contact.alias}")

    if not targets:
        print(f"[NOTIFY] No operators found for contact {contact.alias}")
        return

    preview = _esc((message_text or "[media]")[:100])
    alias = _esc(contact.alias)
    sender_line = f" от <b>{_esc(sender_alias)}</b>" if sender_alias else ""
    text = (
        f"💬 <b>{alias}</b>{sender_line}\n"
        f"<i>{preview}</i>"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📱 Открыть CRM",
            web_app=WebAppInfo(url=settings.WEBAPP_URL) if settings.WEBAPP_URL else None,
            url=None if settings.WEBAPP_URL else "https://t.me",
        )],
    ])

    for operator in targets:
        if not operator.tg_user_id:
            continue
        # Skip if operator has CRM open (active WebSocket connection)
        if ws_manager.is_online(operator.id):
            print(f"[NOTIFY] Operator {operator.tg_user_id} is online in CRM, skipping")
            continue
        print(f"[NOTIFY] Sending to operator {operator.tg_user_id} for contact {contact.alias}")
        try:
            await get_bot().send_message(
                operator.tg_user_id, text, parse_mode="HTML", reply_markup=keyboard,
            )
        except Exception as e:
            print(f"[NOTIFY] Failed to notify {operator.tg_user_id}: {e}")


# ============================================================
# Commands
# ============================================================

@dp.message(CommandStart(deep_link=True))
async def cmd_start_with_code(message: TgMessage):
    """Handle /start <invite_code> — register new staff via bot invite."""
    code = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else ""
    if not code:
        return await cmd_start_no_code(message)

    tg_user_id = message.from_user.id
    tg_username = message.from_user.username
    first_name = message.from_user.first_name or "User"

    async with async_session() as db:
        # Check if already registered
        existing = await db.execute(select(Staff).where(Staff.tg_user_id == tg_user_id))
        if existing.scalar_one_or_none():
            await message.answer(
                f"✅ Вы уже зарегистрированы в <b>YappiGram</b>!",
                parse_mode="HTML",
                reply_markup=_mini_app_button(),
            )
            return

        # Validate invite
        result = await db.execute(
            select(BotInvite).where(
                BotInvite.code == code,
                BotInvite.used_by.is_(None),
                BotInvite.expires_at > datetime.utcnow(),
            )
        )
        invite = result.scalar_one_or_none()
        if not invite:
            await message.answer("❌ Ссылка недействительна или истекла.")
            return

        # Create staff
        user = Staff(
            tg_user_id=tg_user_id,
            tg_username=tg_username,
            role=invite.role,
            name=first_name,
        )
        db.add(user)
        invite.used_by = user.id
        invite.used_at = datetime.utcnow()
        await db.commit()

    role_text = {"operator": "оператор", "admin": "администратор"}.get(invite.role, invite.role)
    await message.answer(
        f"🎉 Добро пожаловать в <b>YappiGram</b>!\n\n"
        f"Ваша роль: <b>{role_text}</b>\n"
        f"Откройте CRM через кнопку ниже:",
        parse_mode="HTML",
        reply_markup=_mini_app_button(),
    )


@dp.message(CommandStart())
async def cmd_start_no_code(message: TgMessage):
    """Handle plain /start — show menu for admin, reject others."""
    tg_user_id = message.from_user.id

    if _is_admin(tg_user_id):
        await message.answer(
            "🤖 <b>YappiGram Bot</b>\n\n"
            "Команды:\n"
            "/pending — ожидающие модерации\n"
            "/add &lt;id&gt; — добавить чат по TG ID\n"
            "/stats — статистика\n"
            "/operators — список операторов\n"
            "/app — открыть CRM\n"
            "/help — помощь",
            parse_mode="HTML",
        )
        return

    # Check if they have access
    if await _has_access(tg_user_id):
        await message.answer(
            "✅ <b>YappiGram</b>\n\nОткройте CRM:",
            parse_mode="HTML",
            reply_markup=_mini_app_button(),
        )
        return

    await message.answer("⛔ Для доступа нужна пригласительная ссылка от администратора.")


@dp.message(Command("help"))
async def cmd_help(message: TgMessage):
    if not _is_admin(message.chat.id):
        return
    await message.answer(
        "📋 <b>Команды бота</b>\n\n"
        "/pending — показать клиентов, ожидающих модерации\n"
        "/add &lt;id&gt; — добавить чат/группу по Telegram ID\n"
        "/stats — общая статистика (клиенты, операторы)\n"
        "/operators — список активных операторов\n"
        "/app — открыть CRM Mini App\n\n"
        "При новом обращении бот автоматически присылает карточку "
        "с кнопками одобрения, блокировки и назначения оператора.",
        parse_mode="HTML",
    )


@dp.message(Command("pending"))
async def cmd_pending(message: TgMessage):
    if not _is_admin(message.chat.id):
        return

    async with async_session() as db:
        result = await db.execute(
            select(Contact).where(Contact.status == "pending").order_by(Contact.created_at)
        )
        pending = result.scalars().all()

    if not pending:
        await message.answer("✅ Нет ожидающих клиентов")
        return

    for contact in pending[:20]:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{contact.id}"),
                InlineKeyboardButton(text="❌ Блок", callback_data=f"block:{contact.id}"),
            ],
            [
                InlineKeyboardButton(text="👤 Назначить", callback_data=f"assign:{contact.id}"),
            ],
        ])
        chat_icon = "👥" if contact.chat_type in ("group", "channel") else "👤"
        await message.answer(
            f"⏳ {chat_icon} <b>{_esc(contact.alias)}</b>\n"
            f"Создан: {contact.created_at.strftime('%d.%m %H:%M') if contact.created_at else '—'}",
            parse_mode="HTML",
            reply_markup=keyboard,
        )


@dp.message(Command("stats"))
async def cmd_stats(message: TgMessage):
    if not _is_admin(message.chat.id):
        return

    async with async_session() as db:
        total = (await db.execute(select(sqlfunc.count(Contact.id)))).scalar()
        pending = (await db.execute(
            select(sqlfunc.count(Contact.id)).where(Contact.status == "pending")
        )).scalar()
        approved = (await db.execute(
            select(sqlfunc.count(Contact.id)).where(Contact.status == "approved")
        )).scalar()
        blocked = (await db.execute(
            select(sqlfunc.count(Contact.id)).where(Contact.status == "blocked")
        )).scalar()
        operators = (await db.execute(
            select(sqlfunc.count(Staff.id)).where(Staff.role == "operator", Staff.is_active.is_(True))
        )).scalar()

    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"Всего клиентов: {total}\n"
        f"⏳ Ожидают: {pending}\n"
        f"✅ Одобрены: {approved}\n"
        f"❌ Заблокированы: {blocked}\n\n"
        f"👥 Операторов онлайн: {operators}",
        parse_mode="HTML",
    )


@dp.message(Command("operators"))
async def cmd_operators(message: TgMessage):
    if not _is_admin(message.chat.id):
        return

    async with async_session() as db:
        result = await db.execute(
            select(Staff).where(Staff.is_active.is_(True)).order_by(Staff.role, Staff.name)
        )
        staff = result.scalars().all()

    if not staff:
        await message.answer("Нет активных сотрудников")
        return

    lines = []
    for s in staff:
        role_emoji = {"super_admin": "👑", "admin": "🔑", "operator": "👤"}.get(s.role, "")
        username_str = f" (@{s.tg_username})" if s.tg_username else ""
        lines.append(f"{role_emoji} <b>{s.name}</b>{username_str} — {s.role}")

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("app"))
async def cmd_app(message: TgMessage):
    """Send a button to open the CRM Mini App."""
    if not settings.WEBAPP_URL:
        await message.answer("WEBAPP_URL not configured")
        return
    await message.answer("Открыть YappiGram:", reply_markup=_mini_app_button())


@dp.message(Command("add"))
async def cmd_add_chat(message: TgMessage):
    """Add a chat/group by Telegram ID: /add <tg_id>"""
    print(f"[ADD] Command received from {message.from_user.id}: {message.text}")
    # Allow TG_ADMIN + any staff with admin/super_admin role
    is_allowed = _is_admin(message.chat.id)
    if not is_allowed:
        async with async_session() as db:
            r = await db.execute(
                select(Staff).where(Staff.tg_user_id == message.from_user.id, Staff.is_active.is_(True))
            )
            staff = r.scalar_one_or_none()
            is_allowed = staff is not None and staff.role in ("admin", "super_admin")
    if not is_allowed:
        await message.answer("⛔ Только для администраторов")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await message.answer(
            "📝 <b>Использование:</b>\n<code>/add &lt;telegram_id&gt;</code>\n\n"
            "Пример:\n<code>/add 123456789</code> — личный чат\n"
            "<code>/add -1001234567890</code> — группа/канал",
            parse_mode="HTML",
        )
        return

    tg_id = int(parts[1].strip())

    # Find active TG account
    from models import TgAccount, StaffTgAccount
    async with async_session() as db:
        # Get first active account
        acc_result = await db.execute(select(TgAccount).where(TgAccount.is_active.is_(True)).limit(1))
        account = acc_result.scalar_one_or_none()
        if not account:
            await message.answer("❌ Нет подключённых TG аккаунтов")
            return

        # Check if contact already exists — search by both ID formats
        possible_ids = list({tg_id, -tg_id, abs(tg_id)})
        existing = await db.execute(
            select(Contact).where(Contact.real_tg_id.in_(possible_ids), Contact.tg_account_id == account.id)
        )
        contact = existing.scalars().first()
        if contact:
            was_approved = contact.status == "approved"
            if not was_approved:
                contact.status = "approved"
                contact.approved_at = datetime.utcnow()
                await db.commit()

            # Always load history for existing contacts
            saved = 0
            try:
                from telegram import _clients, fetch_history, sanitize_text, _extract_media, MEDIA_DIR
                from models import Message
                import os
                tg_messages = await fetch_history(account.id, tg_id, limit=100)
                # Sort oldest first by Telegram message date
                sorted_msgs = sorted(
                    [m for m in tg_messages if m and (m.text or m.media)],
                    key=lambda m: m.date
                )
                for tg_msg in sorted_msgs:
                    dup = await db.execute(
                        select(Message).where(Message.tg_message_id == tg_msg.id, Message.contact_id == contact.id)
                    )
                    if dup.scalars().first():
                        continue
                    media_type, ext = _extract_media(tg_msg)
                    media_path = None
                    if media_type and ext is not None:
                        filename = f"{contact.id}_{tg_msg.id}{ext}"
                        filepath = os.path.join(MEDIA_DIR, filename)
                        try:
                            actual = await tg_msg.download_media(file=filepath)
                            media_path = os.path.basename(actual) if actual else filename
                        except Exception:
                            media_path = filename
                    direction = "outgoing" if tg_msg.out else "incoming"
                    msg = Message(
                        contact_id=contact.id,
                        tg_message_id=tg_msg.id,
                        direction=direction,
                        content=sanitize_text(tg_msg.text),
                        media_type=media_type,
                        media_path=media_path,
                        created_at=tg_msg.date.replace(tzinfo=None) if tg_msg.date else None,
                    )
                    db.add(msg)
                    saved += 1
                if saved:
                    contact.last_message_at = datetime.utcnow()
                    await db.commit()
            except Exception as e:
                print(f"[ADD] History fetch failed for existing: {e}")
                import traceback; traceback.print_exc()
                saved = 0

            status_text = "уже одобрен" if was_approved else "одобрен"
            await message.answer(
                f"✅ Чат {status_text}!\n\n"
                f"Псевдоним: <b>{_esc(contact.alias)}</b>\n"
                f"Загружено новых сообщений: {saved}",
                parse_mode="HTML",
            )
            return

    # Fetch chat info via Telethon
    try:
        from telegram import _clients, fetch_history, generate_alias, sanitize_text, _extract_media, MEDIA_DIR
        from crypto import encrypt
        import os

        client = _clients.get(account.id)
        if not client:
            await message.answer("❌ TG аккаунт не подключён")
            return

        entity = await client.get_entity(tg_id)
        # Use Telethon's peer ID format (matches event.chat_id)
        from telethon.utils import get_peer_id
        resolved_tg_id = get_peer_id(entity)
        print(f"[ADD] user input={tg_id}, resolved={resolved_tg_id}, entity.id={entity.id}")

        is_group = hasattr(entity, "title")
        chat_type = "private"
        if is_group:
            if getattr(entity, "megagroup", False):
                chat_type = "supergroup"
            elif getattr(entity, "broadcast", False):
                chat_type = "channel"
            else:
                chat_type = "group"

        # Generate alias with retry on collision
        async with async_session() as db:
            contact = None
            for attempt in range(10):
                count_result = await db.execute(select(sqlfunc.count(Contact.id)))
                seq = count_result.scalar() + 1 + attempt

                if is_group:
                    title = getattr(entity, "title", "") or ""
                    contact = Contact(
                        tg_account_id=account.id,
                        real_tg_id=resolved_tg_id,
                        real_name_encrypted=encrypt(title),
                        real_username_encrypted=encrypt(getattr(entity, "username", None) or ""),
                        group_title_encrypted=encrypt(title),
                        alias=generate_alias(title, seq),
                        chat_type=chat_type,
                        is_forum=getattr(entity, "forum", False),
                        status="approved",
                        approved_at=datetime.utcnow(),
                    )
                else:
                    first_name = getattr(entity, "first_name", "") or ""
                    username = getattr(entity, "username", None)
                    contact = Contact(
                        tg_account_id=account.id,
                        real_tg_id=resolved_tg_id,
                        real_name_encrypted=encrypt(first_name),
                        real_username_encrypted=encrypt(username) if username else None,
                        alias=generate_alias(first_name, seq),
                        chat_type="private",
                        status="approved",
                        approved_at=datetime.utcnow(),
                    )

                db.add(contact)
                try:
                    await db.commit()
                    await db.refresh(contact)
                    break
                except Exception:
                    await db.rollback()
                    contact = None
            if not contact:
                await message.answer("❌ Не удалось создать контакт (конфликт алиасов)")
                return

            # Fetch history (up to 100 messages)
            try:
                from models import Message
                tg_messages = await fetch_history(account.id, tg_id, limit=100)
                saved = 0
                sorted_msgs = sorted(
                    [m for m in tg_messages if m and (m.text or m.media)],
                    key=lambda m: m.date
                )
                for tg_msg in sorted_msgs:
                    dup = await db.execute(
                        select(Message).where(Message.tg_message_id == tg_msg.id, Message.contact_id == contact.id)
                    )
                    if dup.scalars().first():
                        continue
                    media_type, ext = _extract_media(tg_msg)
                    media_path = None
                    if media_type and ext is not None:
                        filename = f"{contact.id}_{tg_msg.id}{ext}"
                        filepath = os.path.join(MEDIA_DIR, filename)
                        try:
                            actual = await tg_msg.download_media(file=filepath)
                            media_path = os.path.basename(actual) if actual else filename
                        except Exception:
                            media_path = filename
                    direction = "outgoing" if tg_msg.out else "incoming"
                    msg = Message(
                        contact_id=contact.id,
                        tg_message_id=tg_msg.id,
                        direction=direction,
                        content=sanitize_text(tg_msg.text),
                        media_type=media_type,
                        media_path=media_path,
                        created_at=tg_msg.date.replace(tzinfo=None) if tg_msg.date else None,
                    )
                    db.add(msg)
                    saved += 1
                contact.last_message_at = datetime.utcnow()
                await db.commit()
            except Exception as e:
                print(f"[ADD] History fetch failed: {e}")
                import traceback; traceback.print_exc()
                saved = 0

        type_label = {"group": "Группа", "supergroup": "Супергруппа", "channel": "Канал", "private": "Личный чат"}.get(chat_type, chat_type)
        await message.answer(
            f"✅ <b>Чат добавлен!</b>\n\n"
            f"Тип: {type_label}\n"
            f"Псевдоним: <b>{_esc(contact.alias)}</b>\n"
            f"Загружено сообщений: {saved}\n\n"
            f"Чат сразу одобрен и доступен в CRM.",
            parse_mode="HTML",
        )

    except Exception as e:
        await message.answer(f"❌ Ошибка: {_esc(str(e))}", parse_mode="HTML")


# ============================================================
# Callbacks
# ============================================================

@dp.callback_query(F.data.startswith("approve:"))
async def on_approve(callback: CallbackQuery):
    if not _is_admin(callback.message.chat.id):
        await callback.answer("⛔ Нет доступа")
        return

    contact_id = callback.data.split(":")[1]
    async with async_session() as db:
        result = await db.execute(select(Contact).where(Contact.id == contact_id))
        contact = result.scalar_one_or_none()
        if not contact:
            await callback.answer("Контакт не найден")
            return

        if contact.status == "approved":
            await callback.answer("Уже одобрен")
            return

        contact.status = "approved"
        contact.approved_at = sqlfunc.now()
        await db.commit()

        # Load message history on approve (same as API endpoint)
        try:
            from telegram import fetch_history, sanitize_text, _extract_media, _extract_inline_buttons
            from models import Message
            import os
            MEDIA_DIR = "media"
            tg_messages = await fetch_history(contact.tg_account_id, contact.real_tg_id, limit=100)
            sorted_msgs = sorted(
                [m for m in tg_messages if m and (m.text or m.media)],
                key=lambda m: m.date
            )
            for msg_obj in sorted_msgs:
                dup = await db.execute(
                    select(Message).where(Message.tg_message_id == msg_obj.id, Message.contact_id == contact.id)
                )
                if dup.scalars().first():
                    continue
                media_type, ext = _extract_media(msg_obj)
                media_path = None
                if media_type and ext is not None:
                    filename = f"{contact.id}_{msg_obj.id}{ext}"
                    filepath = os.path.join(MEDIA_DIR, filename)
                    try:
                        actual = await msg_obj.download_media(file=filepath)
                        media_path = os.path.basename(actual) if actual else filename
                    except Exception:
                        media_path = filename
                direction = "outgoing" if msg_obj.out else "incoming"
                msg = Message(
                    contact_id=contact.id,
                    tg_message_id=msg_obj.id,
                    direction=direction,
                    content=sanitize_text(msg_obj.text),
                    media_type=media_type,
                    media_path=media_path,
                    inline_buttons=_extract_inline_buttons(msg_obj),
                    created_at=msg_obj.date.replace(tzinfo=None) if msg_obj.date else None,
                )
                db.add(msg)
            await db.commit()
        except Exception as e:
            print(f"[APPROVE-BOT] History fetch failed: {e}")

    await callback.answer("✅ Одобрен")
    try:
        await callback.message.edit_text(
            _esc(callback.message.text) + "\n\n✅ <b>ОДОБРЕН</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("block:"))
async def on_block(callback: CallbackQuery):
    if not _is_admin(callback.message.chat.id):
        await callback.answer("⛔ Нет доступа")
        return

    contact_id = callback.data.split(":")[1]
    async with async_session() as db:
        result = await db.execute(select(Contact).where(Contact.id == contact_id))
        contact = result.scalar_one_or_none()
        if not contact:
            await callback.answer("Контакт не найден")
            return

        contact.status = "blocked"
        await db.commit()

    await callback.answer("❌ Заблокирован")
    try:
        await callback.message.edit_text(
            _esc(callback.message.text) + "\n\n❌ <b>ЗАБЛОКИРОВАН</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("assign:"))
async def on_assign(callback: CallbackQuery):
    if not _is_admin(callback.message.chat.id):
        await callback.answer("⛔ Нет доступа")
        return

    contact_id = callback.data.split(":")[1]

    async with async_session() as db:
        result = await db.execute(
            select(Staff).where(Staff.is_active.is_(True), Staff.role.in_(["operator", "admin"]))
        )
        operators = result.scalars().all()

    if not operators:
        await callback.answer("Нет доступных операторов")
        return

    buttons = []
    for op in operators:
        buttons.append([
            InlineKeyboardButton(
                text=f"{'🔑' if op.role == 'admin' else '👤'} {op.name}",
                callback_data=f"do_assign:{contact_id}:{op.id}",
            )
        ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer(
        f"Выберите оператора для назначения:",
        reply_markup=keyboard,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("do_assign:"))
async def on_do_assign(callback: CallbackQuery):
    if not _is_admin(callback.message.chat.id):
        await callback.answer("⛔ Нет доступа")
        return

    parts = callback.data.split(":")
    contact_id = parts[1]
    operator_id = parts[2]

    async with async_session() as db:
        result = await db.execute(select(Contact).where(Contact.id == contact_id))
        contact = result.scalar_one_or_none()
        op_result = await db.execute(select(Staff).where(Staff.id == operator_id))
        operator = op_result.scalar_one_or_none()

        if not contact or not operator:
            await callback.answer("Не найдено")
            return

        if contact.status == "pending":
            contact.status = "approved"
            contact.approved_at = sqlfunc.now()

        contact.assigned_to = operator.id
        await db.commit()

    await callback.answer(f"✅ Назначен на {operator.name}")
    try:
        await callback.message.edit_text(
            f"✅ Назначен на <b>{operator.name}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass


# ============================================================
# Reject unknown
# ============================================================

@dp.message()
async def reject_unknown(message: TgMessage):
    print(f"[BOT-CATCHALL] from {message.from_user.id}: {(message.text or '')[:50]}")
    if _is_admin(message.chat.id):
        await message.answer("Неизвестная команда. Введите /help")
        return
    if await _has_access(message.from_user.id):
        await message.answer("Используйте /app для открытия CRM")
        return
    await message.answer("⛔ Для доступа нужна пригласительная ссылка.")


# ============================================================
# Lifecycle
# ============================================================

async def start_bot_polling() -> None:
    global _bot_username
    if not settings.TG_BOT_TOKEN:
        return

    b = get_bot()

    # Cache bot username
    try:
        me = await b.get_me()
        _bot_username = me.username or ""
    except Exception as e:
        print(f"Failed to get bot info: {e}")

    # Set Menu Button for Mini App
    if settings.WEBAPP_URL:
        try:
            await b.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="YappiGram",
                    web_app=WebAppInfo(url=settings.WEBAPP_URL),
                )
            )
        except Exception as e:
            print(f"Failed to set menu button: {e}")

    await dp.start_polling(b)


async def stop_bot() -> None:
    if bot:
        await bot.session.close()

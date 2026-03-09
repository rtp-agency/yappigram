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

    async with async_session() as db:
        if staff_id:
            # Direct assignment
            result = await db.execute(
                select(Staff).where(Staff.id == staff_id, Staff.is_active.is_(True))
            )
            operator = result.scalar_one_or_none()
        elif acct_id:
            # Find operator via staff_tg_accounts link
            result = await db.execute(
                select(Staff).join(StaffTgAccount, StaffTgAccount.staff_id == Staff.id)
                .where(StaffTgAccount.tg_account_id == acct_id, Staff.is_active.is_(True))
            )
            operator = result.scalar_one_or_none()
            if operator:
                print(f"[NOTIFY] Found operator {operator.tg_username} via tg_account for {contact.alias}")
        else:
            print(f"[NOTIFY] Contact {contact.alias} has no assigned operator and no tg_account, skipping")
            return

        if not operator:
            print(f"[NOTIFY] No operator found for contact {contact.alias}")
            return

    if not operator.tg_user_id:
        print(f"[NOTIFY] Operator has no tg_user_id for contact {contact.alias}")
        return

    # Skip if operator has CRM open (active WebSocket connection)
    if ws_manager.is_online(operator.id):
        print(f"[NOTIFY] Operator {operator.tg_user_id} is online in CRM, skipping")
        return

    print(f"[NOTIFY] Sending to operator {operator.tg_user_id} for contact {contact.alias}")

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

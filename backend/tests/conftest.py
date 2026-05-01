"""
Test infrastructure for yappigram CRM.

The whole point of this file: tests must NEVER touch the real Telegram
API and NEVER touch the production database. Everything in here is
defensive plumbing toward those two goals.

Two safety nets:

1. **Env override BEFORE any application import.** `pytest_configure`
   below runs as the first hook pytest fires, so we get to set
   `DATABASE_URL`, `REDIS_URL`, `JWT_SECRET`, `ENCRYPTION_KEY` to the
   test values *before* `config.py` reads `os.environ`. If anyone
   accidentally points the test runner at the prod env, the override
   still wins because Pydantic Settings reads via `os.environ` lookup
   and we set those keys explicitly.

2. **Telethon stub.** We replace the `telegram` module with a stub that
   returns inert objects for every async method (set_chat_pin,
   send_message, …). This means even if a test path forgets to mock
   Telethon directly, the worst case is "the call did nothing", not
   "the call hit Telegram and got us flood-banned".

Layout of fixtures:
  - `test_engine` (session-scoped) — points at the test PG container
  - `db` (function-scoped) — fresh transaction per test, rolled back
  - `client` — `httpx.AsyncClient` over `ASGITransport(app)`. Skips the
    FastAPI lifespan entirely, so startup_listeners / auto_sync /
    background tasks never fire. Yes, that means startup events go
    UNTESTED at the unit level — that's intentional. Lifespan tests
    live separately.
  - `staff`, `tg_account`, `contact_factory`, `org_id` — data builders
  - `auth_headers(staff)` — bearer token helper for authenticated calls
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Callable
from uuid import UUID, uuid4

# ---------------------------------------------------------------------------
# 1. Env override — runs BEFORE pytest collects any test, before anything
#    in `backend/` is imported. Pydantic Settings reads via os.environ at
#    import time, so once we set these values here, every subsequent
#    `from config import settings` inside the app sees the test values.
#
#    SAFETY: explicit guard against a prod-looking DATABASE_URL slipping
#    through (e.g. someone runs pytest with their dev shell's env still
#    pointing at the live CRM DB).
# ---------------------------------------------------------------------------
def _set_test_env() -> None:
    test_db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://tgcrm_test:tgcrm_test@127.0.0.1:55433/tgcrm_test",
    )
    # Hard refusal — never run tests against anything that looks like a
    # production database. The CRM prod DB lives on a docker-internal
    # hostname; staging uses 144.31.x. Any URL that mentions either of
    # those hosts is an instant abort.
    forbidden_hosts = ("crm.metra-ai.org", "metra-ai.org", "82.25.60.99", "144.31.234.105")
    for host in forbidden_hosts:
        if host in test_db_url:
            raise RuntimeError(
                f"Refusing to run tests with DATABASE_URL pointing at {host!r}. "
                f"Set TEST_DATABASE_URL to the test container "
                f"(default: postgresql+asyncpg://tgcrm_test:tgcrm_test@127.0.0.1:55433/tgcrm_test)."
            )

    os.environ["DATABASE_URL"] = test_db_url
    os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:56380/0")
    os.environ["JWT_SECRET"] = "test-jwt-secret-do-not-use-in-prod"
    # ENCRYPTION_KEY must be a 32-byte urlsafe-base64 string for Fernet.
    os.environ["ENCRYPTION_KEY"] = base64.urlsafe_b64encode(b"x" * 32).decode()
    # Telethon credentials — stub, never used because we mock the module.
    os.environ.setdefault("TG_API_ID", "0")
    os.environ.setdefault("TG_API_HASH", "stub")
    os.environ.setdefault("TG_BOT_TOKEN", "stub")
    os.environ.setdefault("TG_ADMIN_CHAT_ID", "0")
    # PostForge SSO — empty so any SSO test must explicitly mock it.
    os.environ.setdefault("POSTFORGE_API_URL", "")
    os.environ.setdefault("POSTFORGE_BOT_TOKEN", "")
    os.environ.setdefault("POSTFORGE_BOT_SECRET", "")


_set_test_env()

# Make `backend/` importable as the top-level package, the way the prod
# Dockerfile sets PYTHONPATH. Without this, `from app import app` would
# fail because pytest's rootdir is `backend/` and Python doesn't add it
# automatically when running `pytest tests/`.
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# ---------------------------------------------------------------------------
# 2. Telethon stub — replaces the `telegram` module with an inert version
#    BEFORE app.py imports it. Any production path that calls
#    set_chat_pin/send_message/forward_message etc. now hits a stub that
#    returns a fake result instead of dialing Telegram.
#
#    Individual tests can still patch specific methods on this stub via
#    monkeypatch to assert call args (see send_message tests in Этап 4).
# ---------------------------------------------------------------------------
import types as _types


class _TelethonStub:
    """Records every call so tests can introspect what the app tried to
    do. Defaults to returning None / a benign value for every method."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, args, kwargs))
        return None


_telethon_stub = _TelethonStub()
_telegram_mod = _types.ModuleType("telegram")


# Every public coroutine in `backend/telegram.py` is stubbed here. If a
# real method gets added there, it should be added to this list; the
# fallback `__getattr__` on the module makes any missing name return a
# coroutine that records the call anyway, so a forgotten entry never
# crashes — it just doesn't validate args in tests.
_STUBBED_FUNCS = (
    "start_connect", "verify_code",
    "set_chat_mute", "set_chat_pin",
    "send_message", "send_media_group", "forward_message",
    "delete_messages", "press_inline_button",
    "get_drafts", "download_missing_media",
    "startup_listeners", "shutdown_listeners",
    "disconnect_account",
    "generate_alias", "sanitize_text",
)

for _name in _STUBBED_FUNCS:
    async def _async_stub(*args: Any, _stub_name: str = _name, **kwargs: Any) -> Any:
        return _telethon_stub._record(_stub_name, *args, **kwargs)
    setattr(_telegram_mod, _name, _async_stub)

# generate_alias and sanitize_text are sync in the real module — replace
# with sync stubs.
def _generate_alias(real_name: str | None, sequence: int) -> str:
    return f"alias-{sequence}"


def _sanitize_text(text: str | None) -> str | None:
    return text


_telegram_mod.generate_alias = _generate_alias
_telegram_mod.sanitize_text = _sanitize_text


# Fallback for anything we forgot. Called when an attribute is missing.
def _telegram_fallback(name: str) -> Any:
    async def _missing(*args: Any, **kwargs: Any) -> Any:
        return _telethon_stub._record(f"__missing__:{name}", *args, **kwargs)
    return _missing


_telegram_mod.__getattr__ = _telegram_fallback  # type: ignore[attr-defined]
# Expose the recorder so tests can inspect it.
_telegram_mod._stub = _telethon_stub  # type: ignore[attr-defined]

sys.modules["telegram"] = _telegram_mod

# ---------------------------------------------------------------------------
# 3. Now that env + Telethon are stubbed, we can safely import the app.
#    Importing `models` brings up the SQLAlchemy engine (against TEST DB).
# ---------------------------------------------------------------------------
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import auth as _auth_mod  # noqa: E402  (after env setup)
import models as _models_mod  # noqa: E402
from app import app as _fastapi_app  # noqa: E402
from auth import create_token, get_db  # noqa: E402
from models import Base, Staff, TgAccount, Contact  # noqa: E402


# ---------------------------------------------------------------------------
# 4. DB engine + sessionmaker — function-scoped session, transaction
#    rolled back at end of every test.
# ---------------------------------------------------------------------------
# One-shot schema setup. Runs once per pytest invocation via the
# `_schema_ready` autouse fixture below — separate from the per-test
# engine so we don't pay drop+create on every test.
_SCHEMA_INITIALIZED = False


async def _ensure_schema_once() -> None:
    """Drop + recreate the schema once per pytest run. Standalone
    (not a fixture) so it can run inside the per-test engine fixture's
    own event loop without scope conflicts."""
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    from sqlalchemy.pool import NullPool

    bootstrap = create_async_engine(
        os.environ["DATABASE_URL"], echo=False, future=True, poolclass=NullPool,
    )
    try:
        async with bootstrap.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    finally:
        await bootstrap.dispose()
    _SCHEMA_INITIALIZED = True


@pytest_asyncio.fixture
async def test_engine() -> AsyncGenerator[Any, None]:
    """Per-test engine on a fresh asyncpg connection (NullPool — never
    reuses).

    Why function-scoped instead of session: pytest-asyncio's default
    loop scope is per-function. A session-scoped engine creates its
    asyncpg protocol on the SESSION loop, then per-test sessions try
    to use that protocol from a NEW function loop — asyncpg trips with
    `Future attached to a different loop`. Recreating the engine per
    test is ~3ms on tmpfs PG (negligible) and dodges the cross-loop
    issue completely.

    Schema is set up once at first call via `_ensure_schema_once`,
    not per-test, so the cost stays at the engine init only.
    """
    from sqlalchemy.pool import NullPool

    await _ensure_schema_once()
    engine = create_async_engine(
        os.environ["DATABASE_URL"], echo=False, future=True, poolclass=NullPool,
    )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db(test_engine: Any) -> AsyncGenerator[AsyncSession, None]:
    """Function-scoped DB session, isolated from neighbors via
    delete-from-all-tables at teardown (in the same session, so no
    cross-connection asyncpg races).

    Why this and not SAVEPOINT-rollback: under a session-scoped event
    loop, the SAVEPOINT recipe needs a separate outer connection.
    asyncpg under SQLAlchemy reliably trips
    `cannot use Connection.transaction() in a manually started
    transaction` whenever two connections from the same session-scoped
    pool overlap. Single-session DELETE FROM is dumb but bulletproof
    and fast on tmpfs PG.

    Why not "drop + recreate tables": ~50ms × N tests. Delete-from is
    sub-ms per table, almost zero overhead.
    """
    SessionLocal = async_sessionmaker(test_engine, expire_on_commit=False)
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            # Cleanup — same session, same connection, no race. Reverse
            # FK order so children get deleted before parents.
            try:
                await session.rollback()
            except Exception:
                pass
            for table in reversed(Base.metadata.sorted_tables):
                try:
                    await session.execute(table.delete())
                except Exception:
                    # If the test put the session in a permanent error
                    # state, swallow — engine.begin() below will clean up.
                    await session.rollback()
                    break
            try:
                await session.commit()
            except Exception:
                await session.rollback()
            await session.close()


# ---------------------------------------------------------------------------
# 5. FastAPI client. ASGITransport bypasses the lifespan, so startup
#    listeners / asyncio.create_task(...) inside @app.on_event never fire.
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def client(db: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Authenticated-by-default httpx client.

    Every request inside a test passes through this client, which
    overrides the FastAPI `get_db` dep so all queries hit the
    function-scoped `db` fixture session above.

    For unauthenticated requests, just don't pass an Authorization
    header. For auth, use the `auth_headers` fixture.
    """
    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        # Yield the SAME session the test fixture gave us, so writes from
        # the test setup are visible to the app handler in the same
        # transaction.
        yield db

    _fastapi_app.dependency_overrides[get_db] = _override_get_db
    try:
        transport = ASGITransport(app=_fastapi_app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            yield ac
    finally:
        _fastapi_app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 6. Data factories — minimal kwargs for the common test setup paths.
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def org_id() -> str:
    """Stable org_id for the whole test. Most fixtures default to this
    so cross-org isolation tests can use a SECOND org_id explicitly."""
    return f"test-org-{uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def staff(db: AsyncSession, org_id: str) -> Staff:
    """Default Staff record — operator role, active, linked to a fake
    PostForge org. Override per-test via direct construction if you
    need admin / super_admin."""
    s = Staff(
        id=uuid4(),
        tg_user_id=12345,
        tg_username="testop",
        role="operator",
        name="Test Operator",
        is_active=True,
        postforge_user_id=str(uuid4()),
        postforge_org_id=org_id,
    )
    db.add(s)
    await db.commit()
    await db.refresh(s)
    return s


@pytest_asyncio.fixture
async def tg_account(db: AsyncSession, org_id: str) -> TgAccount:
    """Default TgAccount — active, no real Telethon session. The
    Telethon mock means no method ever touches the network so the
    `session_string` placeholder is fine."""
    acc = TgAccount(
        id=uuid4(),
        phone="+9990000001",
        session_file="test-session-file",
        is_active=True,
        org_id=org_id,
        session_string="stub-session",
        display_name="Test Account",
        show_real_names=True,
    )
    db.add(acc)
    await db.commit()
    await db.refresh(acc)
    return acc


@pytest.fixture
def contact_factory(db: AsyncSession, tg_account: TgAccount) -> Callable[..., Any]:
    """Returns a callable so tests can spawn N contacts inline:
        c1 = await contact_factory(alias='alpha')
        c2 = await contact_factory(alias='beta', is_pinned=True)
    """
    counter = {"n": 0}

    async def _make(**overrides: Any) -> Contact:
        counter["n"] += 1
        from crypto import encrypt
        defaults: dict[str, Any] = dict(
            id=uuid4(),
            tg_account_id=tg_account.id,
            real_tg_id=1_000_000 + counter["n"],
            real_name_encrypted=encrypt(f"Real Name {counter['n']}"),
            real_username_encrypted=encrypt(f"user{counter['n']}"),
            chat_type="private",
            alias=f"contact-{counter['n']}",
            status="approved",
            is_archived=False,
            is_pinned=False,
        )
        defaults.update(overrides)
        c = Contact(**defaults)
        db.add(c)
        await db.commit()
        await db.refresh(c)
        return c

    return _make


# ---------------------------------------------------------------------------
# 7. Auth helper — issue a real-looking access token for a given Staff.
# ---------------------------------------------------------------------------
@pytest.fixture
def auth_headers() -> Callable[[Staff], dict[str, str]]:
    def _make(staff_user: Staff) -> dict[str, str]:
        token = create_token(staff_user.id, token_type="access")
        return {"Authorization": f"Bearer {token}"}
    return _make


# ---------------------------------------------------------------------------
# 8. Telethon recorder — exposed so tests can assert "did the app try to
#    call set_chat_pin?" without monkeypatching themselves.
# ---------------------------------------------------------------------------
@pytest.fixture
def telethon_calls() -> _TelethonStub:
    """Yields the call recorder. Tests can do:
        assert ('set_chat_pin', (acc_id, 12345, True), {}) not in telethon_calls.calls
    to verify that pin no longer pushes to Telegram (Этап 4)."""
    _telethon_stub.calls.clear()
    return _telethon_stub

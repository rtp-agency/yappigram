"""
Etap 1 verification — does the test plumbing actually work?

These aren't real product tests; they prove that:
  1. The app starts under ASGITransport without firing prod-only
     startup events.
  2. `db` fixture gives us an async session that can read+write.
  3. `client` fixture serves /api/health.
  4. Telethon stub is in place — calls to `set_chat_pin` etc.
     don't reach the network.
  5. `auth_headers(staff)` produces a token that get_current_user
     accepts.
  6. Forbidden-host guard would refuse a prod-looking DATABASE_URL
     (asserted indirectly: the test only runs because we set a
     test URL).

If any of these fails, the rest of the suite (Etap 2+) won't make
sense. Run this first when smoke-testing the infrastructure.
"""
from __future__ import annotations

from uuid import UUID

import pytest


# -- 1. App is reachable, lifespan didn't fire ------------------------
async def test_health_endpoint(client) -> None:
    r = await client.get("/api/health")
    assert r.status_code == 200, f"/api/health returned {r.status_code}: {r.text!r}"


# -- 2. DB session works ----------------------------------------------
async def test_db_can_write_and_read_back(db, staff) -> None:
    # `staff` fixture committed a row; we can read it back via the
    # SAME session, proving the test transaction is shared between
    # fixture setup and test body.
    from models import Staff
    from sqlalchemy import select

    result = await db.execute(select(Staff).where(Staff.id == staff.id))
    row = result.scalar_one_or_none()
    assert row is not None
    assert row.name == "Test Operator"
    assert row.role == "operator"


# -- 3. Auth flow ------------------------------------------------------
async def test_auth_headers_let_through_get_current_user(
    client, staff, auth_headers
) -> None:
    """An authenticated request gets through `get_current_user`. We
    use any endpoint that requires auth — `/api/me`-equivalent here is
    `/api/staff/me` (if it exists) or any `CurrentUser`-protected
    endpoint. We pick one that exists by inspecting the app's routes."""
    # Pick one well-known auth endpoint. If the route catalogue changes,
    # swap this for a still-existing one.
    headers = auth_headers(staff)
    # `/api/pinned` is a tiny read-only endpoint we know exists from
    # the recon above and requires auth.
    r = await client.get("/api/pinned", headers=headers)
    # Either 200 (empty list) or 200/204 — anything 2xx means auth let
    # the request through. A 401 would mean the token didn't decode.
    assert r.status_code < 300, f"Auth failed: {r.status_code} {r.text!r}"


async def test_no_auth_returns_401_403(client) -> None:
    r = await client.get("/api/pinned")
    assert r.status_code in (401, 403), (
        f"Expected 401/403 without auth, got {r.status_code}: {r.text!r}"
    )


# -- 4. Telethon stub is hot and recording ----------------------------
async def test_telethon_stub_records_calls(telethon_calls) -> None:
    """Direct call to the stubbed module proves it's intercepted.
    Real product tests will assert which calls happen vs don't happen
    via this same recorder."""
    import telegram  # this is OUR stub thanks to sys.modules patch

    fake_account_id = UUID("00000000-0000-0000-0000-000000000001")
    await telegram.send_message(fake_account_id, 12345, "hi")

    names = [name for name, *_ in telethon_calls.calls]
    assert "send_message" in names, (
        f"send_message wasn't recorded; got: {names}. "
        f"Telethon stub may not be intercepting the import."
    )


async def test_telethon_stub_handles_unknown_methods(telethon_calls) -> None:
    """Even if a method isn't in our explicit stub list, the fallback
    __getattr__ catches it and returns a coroutine — never raises.
    This protects future-added telegram.py methods from crashing the
    suite before someone updates the stub list."""
    import telegram

    # Pick a name that isn't in _STUBBED_FUNCS.
    result = await telegram.some_method_we_havent_added_yet(1, 2, foo="bar")
    assert result is None  # fallback returns None
    names = [name for name, *_ in telethon_calls.calls]
    assert any(n.startswith("__missing__:") for n in names)


# -- 5. The forbidden-host guard exists --------------------------------
def test_forbidden_host_guard_present() -> None:
    """We can't actually trigger the guard inside a running test (env
    is already set), but we verify the guard code path EXISTS in
    conftest by importing it and reading the literal string list.
    A regression that removes the guard would make this test fail."""
    import inspect

    import tests.conftest as ct
    src = inspect.getsource(ct._set_test_env)
    for sentinel in ("crm.metra-ai.org", "82.25.60.99", "144.31.234.105"):
        assert sentinel in src, (
            f"Forbidden-host guard missing {sentinel!r} — never remove this check"
        )


# -- 6. Env override is in effect --------------------------------------
def test_database_url_points_at_test_container() -> None:
    """The whole point of `_set_test_env`. If this fails, every other
    test ran against an unintended DB."""
    import os

    url = os.environ["DATABASE_URL"]
    assert "tgcrm_test" in url, f"Expected test DB URL, got {url!r}"
    assert "55433" in url or "TEST_DATABASE_URL" in os.environ, (
        f"Expected port 55433 (test container), got {url!r}"
    )

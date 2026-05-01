# yappigram regression test suite

Goal: cover every CRM endpoint and business-logic helper with a
golden-set test that pins the current behavior, so a future deploy
that breaks something is caught BEFORE it lands on prod.

## Quick start

```bash
# From repo root:
./scripts/run-tests.sh                   # run everything
./scripts/run-tests.sh -k pin             # only pin tests
./scripts/run-tests.sh tests/test_pin.py  # by file
```

The runner:
1. Boots an isolated test Postgres on `127.0.0.1:55433` and Redis on
   `127.0.0.1:56380` via `docker-compose.test.yml`.
2. Runs pytest with the env wired to those test containers.
3. Tears the containers down at the end (tmpfs storage — nothing
   persists).

## Safety guarantees

The whole suite is built so a misfire CAN'T touch prod:

- **conftest.py refuses** to run if `DATABASE_URL` mentions
  `crm.metra-ai.org`, `metra-ai.org`, `82.25.60.99`, or
  `144.31.234.105`. RuntimeError on import.
- **The Telethon module is replaced with a stub** before app import.
  Every `set_chat_pin`, `send_message`, `forward_message` etc. records
  the call but returns inert results. No real Telegram traffic
  possible during tests.
- **Test containers run on non-default ports** (55433/56380) so even
  if both prod and dev compose stacks are up, nothing collides.
- **No prod code is modified by tests** — they live entirely under
  `backend/tests/` and exercise the app via dependency overrides on
  `get_db`. The prod `app.py`, `models.py`, `telegram.py` are
  untouched.

## Layout

```
backend/
  tests/
    conftest.py        ← all fixtures + safety guards
    test_smoke.py      ← Этап 2: every endpoint is reachable
    test_models.py     ← Этап 3: model CRUD + constraints + migrations
    test_*.py          ← Этап 4+: feature-by-feature coverage
```

## Writing new tests

Use the format from `CLAUDE.md` § TEST-WRITING MANDATE:

```python
class TestPinChatNoTelegram:
    """Production repro 2026-04-27: pin button → 502 because TG hit
    the 5-pinned-dialogs cap. Fix: pin is CRM-local now, no Telethon
    call. Don't relax this assertion — see commit a523915."""

    async def test_pin_does_not_call_telethon(
        self, client, staff, contact_factory, auth_headers, telethon_calls
    ):
        c = await contact_factory()
        r = await client.post(
            f"/api/pinned/{c.id}",
            headers=auth_headers(staff),
        )
        assert r.status_code == 204
        assert all(name != "set_chat_pin" for name, *_ in telethon_calls.calls)
```

## Pre-deploy gate

`scripts/run-tests.sh` exits 1 on any test failure. Wire it into
deploy procedures (manual or CI) so a red suite blocks the deploy.

## Common fixtures

| Fixture | Scope | What it gives you |
|---|---|---|
| `db` | function | Async SQLAlchemy session, rolled back per test |
| `client` | function | `httpx.AsyncClient` over ASGI, lifespan skipped |
| `staff` | function | Default `Staff` row (operator, active) |
| `tg_account` | function | Default `TgAccount` row (active, no real session) |
| `contact_factory` | function | `await contact_factory(alias='x', is_pinned=True)` |
| `auth_headers(staff)` | function | `{"Authorization": "Bearer ..."}` for that staff |
| `telethon_calls` | function | Recorder; `.calls` is a list of `(name, args, kwargs)` |
| `org_id` | function | A unique test-org id; second org for isolation tests = pass a different value |

## Roadmap (ordered by priority)

1. **Этап 2 — smoke** — every endpoint returns a 2xx/4xx, never 500.
2. **Этап 3 — models + migrations** — startup migrations idempotent;
   FK cascade works.
3. **Этап 4 — business logic** — pin, broadcasts (incl. recipients +
   author meta from 30-Apr), messages, contacts, tags, scheduled
   messages, templates.
4. **Этап 5 — permissions / org isolation** — cross-org reads
   blocked at every endpoint.
5. **Этап 6 — websockets** — connect, broadcast updates, redis pubsub.
6. **Этап 7 — bot interactions** — notifications, callbacks.
7. **Этап 8 — pre-deploy CI** — GitHub Actions runs the suite on PRs
   and main pushes; deploy blocked on red.
8. **Этап 9 — coverage** — pytest-cov gate at ≥70%.

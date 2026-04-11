import asyncio
import json
from uuid import UUID

from fastapi import WebSocket

try:
    import redis.asyncio as aioredis
    from config import settings
    REDIS_URL = getattr(settings, "REDIS_URL", None)
except Exception:
    aioredis = None
    REDIS_URL = None


class WSManager:
    """Manages WebSocket connections per staff member, scoped by org.
    Optionally uses Redis pub/sub for cross-process broadcasting."""

    def __init__(self):
        # staff_id -> list of active websockets
        self._connections: dict[UUID, list[WebSocket]] = {}
        # staff_id -> org_id (workspace isolation)
        self._staff_org: dict[UUID, str | None] = {}
        self._redis = None
        self._pubsub = None
        self._listener_task = None

    async def init_redis(self):
        """Initialize Redis pub/sub if available."""
        if not aioredis or not REDIS_URL:
            return
        try:
            self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
            self._pubsub = self._redis.pubsub()
            await self._pubsub.subscribe("crm:ws:broadcast")
            self._listener_task = asyncio.create_task(self._redis_listener())
            print("[WS] Redis pub/sub initialized")
        except Exception as e:
            print(f"[WS] Redis pub/sub init failed: {e}")
            self._redis = None

    async def _redis_listener(self):
        """Listen for Redis pub/sub messages and forward to local WebSocket clients.

        SECURITY: Every message MUST include `_org_id`. A message without it
        is dropped (and logged) rather than broadcast to every connected
        staff member. The previous `_local_broadcast_all` fallback was a
        cross-org data-leak vector — one buggy publisher = full exposure.
        """
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    data = json.loads(message["data"])
                    org_id = data.pop("_org_id", None)
                    if not org_id:
                        import logging
                        logging.getLogger(__name__).warning(
                            "Redis pubsub message missing _org_id — dropping to prevent "
                            f"cross-org leak. Event type: {data.get('type', 'unknown')}"
                        )
                        continue
                    await self._local_broadcast_to_org(org_id, data)
                except Exception:
                    pass
        except Exception as e:
            print(f"[WS] Redis listener error: {e}")

    async def connect(self, staff_id: UUID, ws: WebSocket, org_id: str | None = None):
        await ws.accept()
        self._connections.setdefault(staff_id, []).append(ws)
        self._staff_org[staff_id] = org_id

    def disconnect(self, staff_id: UUID, ws: WebSocket):
        conns = self._connections.get(staff_id, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self._connections.pop(staff_id, None)
            self._staff_org.pop(staff_id, None)

    async def send_to_staff(self, staff_id: UUID, event: dict):
        dead: list[WebSocket] = []
        text = json.dumps(event, default=str)
        for ws in self._connections.get(staff_id, []):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(staff_id, ws)

    async def broadcast_to_staff_list(self, staff_ids: list[UUID], event: dict):
        for sid in staff_ids:
            await self.send_to_staff(sid, event)

    def is_online(self, staff_id: UUID) -> bool:
        return bool(self._connections.get(staff_id))

    async def _local_broadcast_to_org(self, org_id: str, event: dict):
        """Broadcast to local connections only (no Redis)."""
        for staff_id in list(self._connections.keys()):
            if self._staff_org.get(staff_id) == org_id:
                await self.send_to_staff(staff_id, event)

    async def broadcast_to_org(self, org_id: str | None, event: dict):
        if org_id is None:
            return
        if self._redis:
            # Publish via Redis for cross-process support
            event_with_org = {**event, "_org_id": org_id}
            await self._redis.publish("crm:ws:broadcast", json.dumps(event_with_org, default=str))
        else:
            await self._local_broadcast_to_org(org_id, event)

    async def broadcast_to_admins(self, event: dict, org_id: str | None = None):
        # SECURITY: org_id is REQUIRED. Broadcasting without org scoping would
        # send private Telegram messages to every connected user across all orgs.
        # The `org_id is None` fallback existed for legacy reasons but is a data
        # leak risk — one missed caller = full cross-org exposure. Log and drop.
        if org_id is None:
            import logging
            logging.getLogger(__name__).warning(
                "broadcast_to_admins called WITHOUT org_id — dropping event to prevent cross-org leak. "
                f"Event type: {event.get('type', 'unknown')}"
            )
            return
        await self.broadcast_to_org(org_id, event)


ws_manager = WSManager()

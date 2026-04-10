import os
import sys
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = ""

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # JWT — required, no insecure default. Validated below at startup.
    JWT_SECRET: str = ""
    JWT_ACCESS_EXPIRE_MINUTES: int = 480  # 8 hours
    JWT_REFRESH_EXPIRE_DAYS: int = 30

    # Encryption — required for at-rest contact + session encryption
    ENCRYPTION_KEY: str = ""

    # Telegram MTProto
    TG_API_ID: int = 0
    TG_API_HASH: str = ""

    # Telegram Bot
    TG_BOT_TOKEN: str = ""
    TG_ADMIN_CHAT_ID: int = 0

    # App
    APP_URL: str = "http://localhost:3000"
    WEBAPP_URL: str = ""  # Public HTTPS URL for TG Mini App (e.g. https://crm.example.com)
    CORS_ORIGINS: str = "http://localhost:3000"

    # SSO — PostForge integration
    # Trust model: CRM validates the PostForge token by calling
    # PostForge /api/me. There is no shared secret because the JWT itself
    # is the credential — if PostForge accepts it, CRM accepts it. The
    # legacy POSTFORGE_SSO_SECRET field has been removed (it was never
    # wired up). For network isolation in production, POSTFORGE_API_URL
    # should point at the internal docker network address, not the
    # public hostname.
    POSTFORGE_API_URL: str = ""  # e.g. http://backend:8000 (internal) or https://metra-ai.org
    POSTFORGE_BOT_TOKEN: str = ""  # PostForge bot token (for Mini App initData validation)
    POSTFORGE_BOT_SECRET: str = ""  # Shared secret for calling PostForge internal APIs (Bot {secret} header)

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()


# ─── Startup validation ───
# Fail fast if any production secret is missing or set to a known
# default. Allow tests to bypass via PYTEST_CURRENT_TEST env var.
def _abort(msg: str) -> None:
    print(f"FATAL: {msg}", file=sys.stderr)
    sys.exit(1)


_in_tests = bool(os.environ.get("PYTEST_CURRENT_TEST"))
_dev_mode = os.environ.get("CRM_DEV_MODE") == "1"

if not _in_tests and not _dev_mode:
    if not settings.JWT_SECRET or settings.JWT_SECRET in ("change-me", "yappigram-jwt-secret-change-in-production"):
        _abort("JWT_SECRET must be set to a strong random value (min 32 chars). Set CRM_DEV_MODE=1 to bypass for local dev.")
    if len(settings.JWT_SECRET) < 32:
        _abort(f"JWT_SECRET is too short ({len(settings.JWT_SECRET)} chars) — must be at least 32 characters.")
    if not settings.ENCRYPTION_KEY:
        _abort("ENCRYPTION_KEY must be set (Fernet base64 key). Generate with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'")
    if not settings.DATABASE_URL or "changeme" in settings.DATABASE_URL:
        _abort("DATABASE_URL must be set with a real password (not 'changeme').")

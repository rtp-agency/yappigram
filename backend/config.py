from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://tgcrm:changeme@db:5432/tgcrm"

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # JWT
    JWT_SECRET: str = "change-me"
    JWT_ACCESS_EXPIRE_MINUTES: int = 15
    JWT_REFRESH_EXPIRE_DAYS: int = 30

    # Encryption
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
    POSTFORGE_API_URL: str = ""  # e.g. http://backend:8000 (internal) or https://metra-ai.org
    POSTFORGE_SSO_SECRET: str = ""  # Shared secret for SSO token exchange
    POSTFORGE_BOT_TOKEN: str = ""  # PostForge bot token (for Mini App initData validation)

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()

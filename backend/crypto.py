from functools import lru_cache

from cryptography.fernet import Fernet

from config import settings

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        if not settings.ENCRYPTION_KEY:
            raise RuntimeError("ENCRYPTION_KEY is not set")
        _fernet = Fernet(settings.ENCRYPTION_KEY.encode())
    return _fernet


def encrypt(value: str | None) -> str | None:
    if value is None:
        return None
    return _get_fernet().encrypt(value.encode()).decode()


@lru_cache(maxsize=4096)
def decrypt(value: str | None) -> str | None:
    if value is None:
        return None
    return _get_fernet().decrypt(value.encode()).decode()


# ─── Telethon session storage ──────────────────────────────────────
# Sessions used to be stored in plaintext in tg_accounts.session_string.
# A DB breach was effectively a full Telegram account takeover for every
# connected number. Going forward sessions are Fernet-encrypted at rest.
#
# Backward compatibility: existing plaintext rows are detected by the
# absence of the Fernet `gAAAA` magic prefix and re-encrypted on first
# successful load. New rows are always encrypted before INSERT.

_FERNET_PREFIX = "gAAAA"


def encrypt_session(value: str | None) -> str | None:
    """Encrypt a Telethon StringSession for DB storage."""
    if value is None:
        return None
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_session(value: str | None) -> str | None:
    """
    Decrypt a stored session, transparently handling legacy plaintext
    rows. Returns the plaintext StringSession ready to hand to Telethon.
    """
    if value is None:
        return None
    if not value.startswith(_FERNET_PREFIX):
        # Legacy plaintext row — return as-is. The caller is responsible
        # for re-saving via encrypt_session() to migrate.
        return value
    try:
        return _get_fernet().decrypt(value.encode()).decode()
    except Exception:
        # Treat as legacy plaintext if Fernet rejects it (e.g. someone
        # manually edited the DB). Better to keep the bot online than
        # to crash on every reconnect.
        return value


def is_session_encrypted(value: str | None) -> bool:
    """True if the value is already a Fernet token, False otherwise."""
    return bool(value and value.startswith(_FERNET_PREFIX))

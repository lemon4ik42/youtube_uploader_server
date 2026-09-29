import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.core.config import settings


# Using a fixed salt is acceptable because the secret key is meant to be unique per deployment.
_SALT = b"youtube-uploader-server-v1"


def _get_fernet() -> Fernet:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=_SALT,
        iterations=100_000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(settings.app_secret_key.encode("utf-8")))
    return Fernet(key)


_fernet = _get_fernet()


def encrypt(value: str) -> str:
    return _fernet.encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt(token: str) -> str:
    return _fernet.decrypt(token.encode("utf-8")).decode("utf-8")

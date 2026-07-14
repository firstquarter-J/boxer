from __future__ import annotations

import hashlib
import hmac
import secrets

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


class AdminSessionManager:
    def __init__(self, secret_key: str) -> None:
        self._serializer = URLSafeTimedSerializer(secret_key=secret_key, salt="boxer-web-admin")

    def dump(self, admin_user_id: str) -> str:
        return self._serializer.dumps({"adminUserId": admin_user_id})

    def load(self, token: str, *, max_age: int) -> str | None:
        try:
            payload = self._serializer.loads(token, max_age=max_age)
        except (BadSignature, SignatureExpired):
            return None
        admin_user_id = str((payload or {}).get("adminUserId") or "").strip()
        return admin_user_id or None


class AdminCsrfManager:
    def issue_token(self) -> str:
        return secrets.token_urlsafe(32)

    def is_valid(self, cookie_token: str | None, header_token: str | None) -> bool:
        normalized_cookie = str(cookie_token or "").strip()
        normalized_header = str(header_token or "").strip()
        if not normalized_cookie or not normalized_header:
            return False
        return hmac.compare_digest(normalized_cookie, normalized_header)


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt.encode("utf-8"),
        n=2**14,
        r=8,
        p=1,
    )
    return f"{salt}:{derived.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    salt, separator, stored_hash = encoded.partition(":")
    if not salt or not separator or not stored_hash:
        return False

    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt.encode("utf-8"),
        n=2**14,
        r=8,
        p=1,
    )
    return hmac.compare_digest(derived.hex(), stored_hash)

"""JWT authentication utilities."""
from datetime import datetime, timedelta, timezone
from typing import Optional
import uuid

import bcrypt
import jwt


def create_token(payload: dict, secret: str, expires_minutes: int = 60) -> str:
    """Create a JWT access token."""
    now = datetime.now(timezone.utc)
    token_payload = {
        **payload,
        "iat": now,
        "exp": now + timedelta(minutes=expires_minutes),
        "jti": str(uuid.uuid4()),
    }
    return jwt.encode(token_payload, secret, algorithm="HS256")


def verify_token(token: str, secret: str) -> Optional[dict]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def hash_password(password: str) -> str:
    """Hash password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def check_password(password: str, hashed: str) -> bool:
    """Verify password against hash."""
    return bcrypt.checkpw(password.encode(), hashed.encode())

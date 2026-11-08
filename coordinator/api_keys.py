"""API keys (internal endpoints, called by the gateway with the cluster token).

Only the SHA-256 of a key is stored, so a database leak does not reveal
usable keys. A key is shown in plain text exactly once, when it is created.
Roles: admin (everything), readwrite (data), readonly (GET/HEAD only).
"""

import hashlib
import secrets
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from coordinator import config
from coordinator.db import connect_db

router = APIRouter()
ROLES = ("admin", "readwrite", "readonly")


def _check_token(token: str | None) -> None:
    if not token or token != config.CLUSTER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _describe(row: dict) -> dict:
    return {"key_id": str(row["key_id"]), "name": row["name"],
            "role": row["role"], "rate_limit_rps": row["rate_limit_rps"],
            "revoked": row["revoked"],
            "created_at": row["created_at"].isoformat(),
            "last_used_at": row["last_used_at"].isoformat()
            if row["last_used_at"] else None}


class AuthIn(BaseModel):
    key_hash: str


@router.post("/v1/internal/auth")
def authenticate(body: AuthIn,
                 x_cluster_token: str | None = Header(default=None)):
    """Resolve the SHA-256 of an API key to an identity."""
    _check_token(x_cluster_token)
    with connect_db() as conn:
        row = conn.execute(
            "UPDATE api_keys SET last_used_at = NOW() "
            "WHERE key_hash = %s AND NOT revoked RETURNING *",
            (body.key_hash,)).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return _describe(row)


class KeyIn(BaseModel):
    name: str
    role: str = "readwrite"
    rate_limit_rps: float | None = None


@router.post("/v1/api-keys", status_code=201)
def create_key(body: KeyIn,
               x_cluster_token: str | None = Header(default=None)):
    _check_token(x_cluster_token)
    if body.role not in ROLES:
        raise HTTPException(status_code=400, detail=f"role: {', '.join(ROLES)}")
    if not 1 <= len(body.name) <= 64:
        raise HTTPException(status_code=400, detail="name: 1-64 characters")
    if body.rate_limit_rps is not None and body.rate_limit_rps <= 0:
        raise HTTPException(status_code=400, detail="rate_limit_rps must be > 0")
    key = "dsk_" + secrets.token_urlsafe(32)
    with connect_db() as conn:
        row = conn.execute(
            "INSERT INTO api_keys (key_id, name, key_hash, role, rate_limit_rps) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (uuid4(), body.name, hashlib.sha256(key.encode()).hexdigest(),
             body.role, body.rate_limit_rps)).fetchone()
    return {**_describe(row), "api_key": key,
            "note": "Store this key now: it cannot be shown again."}


@router.get("/v1/api-keys")
def list_keys(x_cluster_token: str | None = Header(default=None)):
    _check_token(x_cluster_token)
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM api_keys ORDER BY created_at"
                            ).fetchall()
    return {"keys": [_describe(r) for r in rows]}


@router.delete("/v1/api-keys/{key_id}")
def revoke_key(key_id: str, x_cluster_token: str | None = Header(default=None)):
    _check_token(x_cluster_token)
    try:
        kid = UUID(key_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid key id")
    with connect_db() as conn:
        row = conn.execute("UPDATE api_keys SET revoked = TRUE "
                           "WHERE key_id = %s RETURNING *", (kid,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return _describe(row)

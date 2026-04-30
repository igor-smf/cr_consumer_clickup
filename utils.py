import os
import json
import base64
from typing import Optional, Tuple
from datetime import datetime, timedelta, timezone

from psycopg2.pool import SimpleConnectionPool


# ======================
# Pub/Sub helpers
# ======================
def _get_push_envelope(req_json: dict) -> dict:
    if not isinstance(req_json, dict) or "message" not in req_json:
        raise ValueError("Invalid Pub/Sub push: missing 'message'")
    return req_json


def _decode_message(message: dict) -> Tuple[dict, dict]:
    data_b64: Optional[str] = message.get("data")
    attributes: dict = message.get("attributes", {}) or {}

    if data_b64:
        try:
            raw = base64.b64decode(data_b64)
            payload = json.loads(raw)
        except Exception as e:
            raise ValueError(f"Failed to decode/parse message.data: {e}") from e
    else:
        payload = {}

    return payload, attributes


# ======================
# Postgres idempotency
# ======================
IDEMPOTENCY_TTL_MINUTES = int(os.getenv("IDEMPOTENCY_TTL_MINUTES", "15"))

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=int(os.getenv("DB_POOL_MAX_CONN", "5")),
    host=os.environ["DB_HOST"],
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)


def claim(
    processing_key: str,
    event_type: str,
    ttl_minutes: int = IDEMPOTENCY_TTL_MINUTES,
) -> bool:
    conn = db_pool.getconn()

    try:
        expire_at = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO clickup_webhook_idempotency (
                        processing_key,
                        event_type,
                        status,
                        expire_at
                    )
                    VALUES (%s, %s, 'processing', %s)
                    ON CONFLICT (processing_key)
                    DO UPDATE SET
                        status = 'processing',
                        updated_at = NOW(),
                        expire_at = EXCLUDED.expire_at
                    WHERE clickup_webhook_idempotency.status = 'processing'
                      AND clickup_webhook_idempotency.expire_at IS NOT NULL
                      AND clickup_webhook_idempotency.expire_at < NOW()
                    RETURNING processing_key;
                    """,
                    (
                        processing_key,
                        event_type,
                        expire_at,
                    ),
                )

                return cur.fetchone() is not None

    finally:
        db_pool.putconn(conn)


def release_success_guarded(processing_key: str) -> None:
    conn = db_pool.getconn()

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE clickup_webhook_idempotency
                    SET
                        status = 'done',
                        updated_at = NOW(),
                        expire_at = NULL
                    WHERE processing_key = %s
                      AND status = 'processing';
                    """,
                    (processing_key,),
                )

    finally:
        db_pool.putconn(conn)


def release_fail(processing_key: str) -> None:
    conn = db_pool.getconn()

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM clickup_webhook_idempotency
                    WHERE processing_key = %s
                      AND status = 'processing';
                    """,
                    (processing_key,),
                )

    finally:
        db_pool.putconn(conn)
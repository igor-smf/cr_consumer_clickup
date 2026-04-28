import os
import logging
from utils import _get_push_envelope
from fastapi import FastAPI, Request, HTTPException

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk import start_transaction, start_span

# ======================
# Logging
# ======================
logger = logging.getLogger("clickup-consumer")
logging.basicConfig(level=logging.INFO)

# ======================
# Sentry
# ======================
def before_send_transaction(event, hint):
    # Evita transações HTTP triviais no APM
    if event.get("contexts", {}).get("trace", {}).get("op") == "http.server":
        return None
    if event.get("transaction") in {"GET /", "POST /"}:
        return None
    return event

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    send_default_pii=True,
    enable_logs=True,
    integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
    before_send_transaction=before_send_transaction,
    traces_sample_rate=1.0,
)


app = FastAPI()

@app.post("/")
async def pubsub_push(request: Request):
    # 1) Valida envelope
    try:
        envelope = _get_push_envelope(await request.json())
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=400, detail=str(e))
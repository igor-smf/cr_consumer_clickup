import os
import json
import logging
from typing import Dict

from utils import (
    _get_push_envelope,
    _decode_message,
    claim,
    release_success_guarded,
    release_fail,
    IDEMPOTENCY_TTL_MINUTES,
)

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk import start_transaction, start_span


logger = logging.getLogger("clickup-consumer")
logging.basicConfig(level=logging.INFO)


def before_send_transaction(event, hint):
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

    try:
        envelope = _get_push_envelope(await request.json())
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=400, detail=str(e))

    message = envelope["message"]
    payload, attributes = _decode_message(message)

    sentry_trace = attributes.get("sentry-trace")
    baggage = attributes.get("baggage")

    continue_ctx: Dict[str, str] = {}

    if sentry_trace:
        continue_ctx["sentry-trace"] = sentry_trace

    if baggage:
        continue_ctx["baggage"] = baggage

    event_id = attributes.get("event_id")
    event_type = attributes.get("event_type")

    if not event_id:
        raise HTTPException(status_code=400, detail="Missing event_id")

    if not event_type:
        raise HTTPException(status_code=400, detail="Missing event_type")

    processing_key = event_id

    message_meta = {
        "pubsub_message_id": message.get("messageId"),
        "publish_time": message.get("publishTime"),
        "delivery_attempt": envelope.get("deliveryAttempt"),
        "subscription": request.headers.get("Ce-Subscription", "unknown"),
    }

    logger.info(json.dumps({
        "log": "consumer.receive",
        **message_meta,
        "processing_key": processing_key,
        "event_id": event_id,
        "event_type": event_type,
        "source": "clickup",
        "sentry_trace": sentry_trace,
    }))

    tx_ctx = sentry_sdk.continue_trace(continue_ctx) if continue_ctx else None

    with (
        start_transaction(tx_ctx, op="pubsub.process", name=f"consumer:clickup:{event_type}")
        if tx_ctx
        else start_transaction(op="pubsub.process", name=f"consumer:clickup:{event_type}")
    ):
        sentry_sdk.set_tag("source", "clickup")
        sentry_sdk.set_tag("event_id", event_id)
        sentry_sdk.set_tag("event_type", event_type)

        with start_span(op="idempotency.claim", description="Postgres claim"):
            got_claim = claim(
                processing_key=processing_key,
                event_type=event_type,
                ttl_minutes=IDEMPOTENCY_TTL_MINUTES,
            )

            if not got_claim:
                logger.info(json.dumps({
                    "log": "consumer.skip_duplicate",
                    "processing_key": processing_key,
                    "event_id": event_id,
                    "event_type": event_type,
                }))

                return Response(status_code=204)

        success = False

        try:
            with start_span(op="business.handle", description=f"Handle clickup:{event_type}"):

                logger.info(json.dumps({
                    "log": "consumer.processing",
                    "processing_key": processing_key,
                    "event_id": event_id,
                    "event_type": event_type,
                    "payload": payload,
                }))

                # Aqui entra a lógica real do ClickUp.
                #
                # Exemplo futuro:
                #
                # handler = EVENT_HANDLERS.get(event_type)
                #
                # if handler:
                #     handler(payload, attributes, ctx)
                # else:
                #     logger.info(f"Nenhum handler para event_type={event_type}")

                success = True
                return Response(status_code=204)

        except Exception as e:
            logger.exception(f"[Consumer] Erro no handler ({processing_key}): {e}")
            sentry_sdk.capture_exception(e)
            raise

        finally:
            try:
                if success:
                    with start_span(op="idempotency.done", description="Postgres mark done"):
                        release_success_guarded(processing_key)
                else:
                    with start_span(op="idempotency.release_fail", description="Postgres release fail"):
                        release_fail(processing_key)

            except Exception as upderr:
                logger.exception(
                    f"[Consumer] Falha ao atualizar idempotência {processing_key}: {upderr}"
                )
                sentry_sdk.capture_exception(upderr)
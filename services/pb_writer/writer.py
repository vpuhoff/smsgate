# services/pb_writer/writer.py
from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import os
import socket
import uuid
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server
from tenacity import retry, stop_after_attempt, wait_exponential

from libs.config import get_settings
from libs.models import ParsedSMS
from libs.nats_utils import (
    get_nats_connection,
    SUBJECT_PARSED,
    SUBJECT_FAILED,
)
from libs.sentry import init_sentry, sentry_capture

from pocketbase import upsert_parsed_sms

settings = get_settings()
# ──────────────────────────── logging ────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pb_writer")
log.setLevel(logging.INFO)

# ───────────────────────── Prometheus metrics ────────────────────────
PARSED_OK = Counter("pb_writer_parsed_ok_total", "Records saved to PocketBase")
PARSED_FAIL = Counter("pb_writer_parsed_fail_total", "Records failed to save")
STREAM_LAG = Gauge("pb_writer_stream_lag", "sms.parsed JetStream consumer lag (messages)")

METRICS_PORT = int(os.getenv("PBWRITER_METRICS_PORT", "9103"))

# ───────────────────────────── Settings ──────────────────────────────
STREAM_NAME = "SMS"           # тот же стрим, что использует parser_worker
CONSUMER_DURABLE = "pb_writer"  # durable-consumer в JetStream

# ──────────────────────────── Helpers ────────────────────────────────
async def _calc_lag(js, durable: str) -> None:
    """Периодически обновляем метрику отставания (pending)."""
    while True:
        try:
            info = await js.consumer_info(STREAM_NAME, durable)
            STREAM_LAG.set(info.num_pending)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.debug("Cannot update lag: %s", exc)
        await asyncio.sleep(1)


@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
async def _safe_upsert(parsed: ParsedSMS) -> None:
    """Идемпотентный upsert с автоматическим бэк-оффом."""
    await upsert_parsed_sms(parsed)
    PARSED_OK.inc()


async def _process_one(js, msg) -> None:
    """Обработка одного сообщения JetStream."""
    try:
        data = json.loads(msg.data)
        parsed = ParsedSMS.model_validate(data)
        if parsed.merchant:
            log.info(f'Save event to pocketbase: {parsed.raw_body}')
            if parsed.date > datetime.now():
                raise Exception("Bad date")
            await _safe_upsert(parsed)
        await msg.ack()
    except Exception as e:  # noqa: BLE001
        PARSED_FAIL.inc()
        sentry_capture(e, extras={"raw_msg": msg.data.decode(errors="ignore")})
        # кладём в DLQ, чтобы не потерять
        fail_payload = json.dumps(
            {"err": str(e), "entry": msg.data.decode(errors="ignore")}
        ).encode()
        await js.publish(SUBJECT_FAILED, fail_payload)
        await msg.ack()


async def _run() -> None:
    """Основной цикл приёмника."""
    settings = get_settings()  # noqa: F841 – понадобится, если будут ENV-флаги

    # ── Init NATS / JetStream ────────────────────────────────────────
    nc = await get_nats_connection()
    js = nc.jetstream()

    # ── Durable push-subscription ────────────────────────────────────
    sub = await js.subscribe(
        SUBJECT_PARSED,
        durable=CONSUMER_DURABLE,
        manual_ack=True
    )
    log.info("pb_writer subscribed to %s as durable %s", SUBJECT_PARSED, CONSUMER_DURABLE)

    # ── Metrics ──────────────────────────────────────────────────────
    start_http_server(METRICS_PORT)
    asyncio.create_task(_calc_lag(js, CONSUMER_DURABLE))

    # ── Main loop ────────────────────────────────────────────────────
    async for msg in sub.messages:
        await _process_one(js, msg)


def main() -> None:  # noqa: D401
    """Entry-point — вызывается из Dockerfile CMD."""
    init_sentry(release="pb_writer@2.0.0")
    asyncio.run(_run())


if __name__ == "__main__":
    main()

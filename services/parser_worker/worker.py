# services/parser_worker/worker.py

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import logging
import os
import signal
import sys
from contextlib import suppress
from typing import Any

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from libs.config import get_settings
from libs.models import ParsedSMS, RawSMS
from libs.nats_utils import (
    get_nats_connection,
    ensure_stream,
    SUBJECT_RAW,
    SUBJECT_PARSED,
    SUBJECT_FAILED,
)
from libs.gemini_parser import parse_sms_llm
from libs.sentry import init_sentry, sentry_capture

from metrics import (
    PARSED_FAIL,
    PARSED_OK,
    PROCESSING_TIME,
    start_metrics_server,
)

logger = logging.getLogger("parser_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

async def _process_one(
    nc: NATS,
    msg: Msg,
) -> None:
    """Parse *one* Raw SMS from a NATS message and publish the result."""
    
    js = nc.jetstream()
    
    try:
        # Декодируем и валидируем сырое сообщение
        raw_sms_data = json.loads(msg.data)
        raw_sms = RawSMS(**raw_sms_data)
    except Exception as err:
        logger.error("Ошибка валидации сообщения: %s. Данные: %s", err, msg.data)
        # Некорректная схема — сразу в DLQ
        failure_payload = json.dumps({"err": str(err), "entry": msg.data.decode(errors='ignore')}).encode()
        await js.publish(SUBJECT_FAILED, failure_payload)
        PARSED_FAIL.inc()
        sentry_capture(err, extras={"raw_data": msg.data.decode(errors='ignore')})
        await msg.ack()
        return

    if 'OTP' in raw_sms.body.upper() or \
            'CODE:' in raw_sms.body.upper() or \
            'NOT ENOUGH FUNDS' in raw_sms.body.upper() or \
            'INSUFFICIENT FUNDS' in raw_sms.body.upper() or \
            'CREDIT PAYMENT' in raw_sms.body.upper() or \
            'C2C RECEIVED' in raw_sms.body.upper() or \
            'PERSON TO PERSON' in raw_sms.body.upper():
        PARSED_OK.inc()
        await msg.ack()
        return 
    
    # Основная логика парсинга
    with PROCESSING_TIME.time():
        try:
            parsed = parse_sms_llm(raw_sms)
        except Exception as err:
            logger.error("Ошибка парсинга SMS: %s. SMS: %s", err, raw_sms)
            failure_payload = json.dumps({"err": str(err), "entry": raw_sms.model_dump()}).encode()
            await js.publish(SUBJECT_FAILED, failure_payload)
            PARSED_FAIL.inc()
            sentry_capture(err, extras={"raw_sms": raw_sms.model_dump()})
            await msg.ack()
            return
            
    if parsed is None:
        # Нераспознанный формат – в DLQ без stacktrace
        logger.warning("Не удалось распознать SMS → DLQ: %s", raw_sms.body[:60])
        failure_payload = json.dumps({"reason": "unmatched", "raw": raw_sms.model_dump()}).encode()
        await js.publish(SUBJECT_FAILED, failure_payload)
        PARSED_FAIL.inc()
        await msg.ack()
        return

    # Успешный парсинг: обогащаем и публикуем
    try:
        parsed_sms = ParsedSMS(**parsed.model_dump())
    except Exception as err:
        failure_payload = json.dumps({"err": str(err), "entry": msg.data.decode(errors='ignore')}).encode()
        await js.publish(SUBJECT_FAILED, failure_payload)
        PARSED_FAIL.inc()
        await msg.ack()
        return
    if not isinstance(parsed_sms.date, datetime):
        logger.warning("Не считалась дата: %s", raw_sms.body[:60])
    else:
        # Перепроверяем дату
        fix_broken_datetime(parsed_sms)

    success_payload = parsed_sms.model_dump_json().encode()

    await js.publish(SUBJECT_PARSED, success_payload)
    PARSED_OK.inc()
    logger.info("Успешно обработано: %s", raw_sms.body[:60])
    await msg.ack()

def fix_broken_datetime(parsed_sms):
    if parsed_sms.date.month != parsed_sms.date.day:
        formatted_date = parsed_sms.date.strftime("%m.%d.%y")
        if formatted_date in parsed_sms.raw_body:
            parsed_sms.date = parsed_sms.date.replace(month=parsed_sms.date.day, day=parsed_sms.date.month)
        formatted_date = parsed_sms.date.strftime("%m.%d.%Y")
        if formatted_date in parsed_sms.raw_body:
            parsed_sms.date = parsed_sms.date.replace(month=parsed_sms.date.day, day=parsed_sms.date.month)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def _worker_loop(nc: NATS, consumer_group: str) -> None:  # pragma: no cover – infinite loop
    """Main worker loop that listens for messages from NATS and processes them."""
    js = nc.jetstream()
    
    # Создаем durable-подписчика. Это позволяет нескольким воркерам 
    # в одной группе (`consumer_group`) распределять между собой нагрузку.
    # NATS запомнит, какие сообщения были обработаны, даже после перезапуска.
    sub = await js.subscribe(SUBJECT_RAW, durable=consumer_group)
    
    logger.info(f"Воркер запущен. Группа: '{consumer_group}'. Слушаем субъект: '{SUBJECT_RAW}'...")
    
    async for msg in sub.messages:
        await _process_one(nc, msg)

# ---------------------------------------------------------------------------
# Entrypoint helpers
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parser Worker for SMS pipeline")
    p.add_argument("--name", default=f"{os.uname().nodename}-{os.getpid()}", help="Уникальное имя этого воркера")
    p.add_argument("--group", default="parsers", help="Имя группы консьюмеров (durable name)")
    return p.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> None:  # pragma: no cover
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    _ = get_settings()

    # Metrics & Sentry first
    start_metrics_server()
    init_sentry(release="parser_worker@2.0.0")

    nc = await get_nats_connection()
    
    # Убеждаемся, что стрим и все нужные нам субъекты существуют
    await ensure_stream(
        nc=nc,
        stream_name="SMS",
        subjects=[SUBJECT_RAW, SUBJECT_PARSED, SUBJECT_FAILED]
    )

    # Graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    
    def _sig_handler(*_: Any) -> None:
        logger.info("Получен сигнал остановки, завершаем работу...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _sig_handler)

    worker_task = asyncio.create_task(_worker_loop(nc, args.group))
    
    await stop_event.wait()
    
    worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await worker_task
        
    await nc.close()
    logger.info("Соединение с NATS закрыто. Выход.")


def main() -> None:  # pragma: no cover – CLI
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
# services/parser_worker/worker.py

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import logging
import os
import re
import signal
import sys
from contextlib import suppress
from typing import Any

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
import sentry_sdk

from libs.config import get_settings
from libs.models import ParsedSMS, RawSMS
from libs.nats_utils import (
    SUBJECT_PROCESSING,
    get_nats_connection,
    ensure_stream,
    SUBJECT_RAW,
    SUBJECT_PARSED,
    SUBJECT_FAILED,
)
from libs.gemini_parser import BrokenMessage, parse_sms_llm
from libs.sentry import init_sentry, sentry_capture

from metrics import (
    ACK_PENDING,
    GEMINI_LATENCY,
    PARSED_FAIL,
    PARSED_OK,
    PARSED_SKIP,
    PROCESSING_TIME,
    start_metrics_server,
)

logger = logging.getLogger("parser_worker")
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

async def _process_one(
    nc: NATS,
    msg: Msg,
) -> None:
    """Parse *one* Raw SMS from a NATS message and publish the result."""
    
    with sentry_sdk.start_transaction(op="task", name="process_parsing"):
        with sentry_sdk.start_span(name="check_stream"):
            js = nc.jetstream()

            await ensure_stream(nc)

        logger.debug("⏱  V: start-validate %s", msg.metadata.sequence)
        with sentry_sdk.start_span(name="validate"):
            try:
                logger.info(f"Обрабатываем сообщение {msg}")
                if isinstance(msg.data, bytes):
                    # Данный блок обработки для сообщений из DLQ
                    msg.data = msg.data.decode()
                    raw_sms_data = json.loads(msg.data)
                    if 'raw' in raw_sms_data:
                        raw_sms_data = raw_sms_data['raw']
                else:
                    # Декодируем и валидируем сырое сообщение
                    raw_sms_data = json.loads(msg.data)
                raw_sms = RawSMS(**raw_sms_data)
                logger.debug("✅  V: ok %s", msg.metadata.sequence)
            except Exception as err:
                logger.error("❌  V: fail %s – %s", msg.metadata.sequence, err)
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
                'PASS:' in raw_sms.body.upper() or \
                'PASS=' in raw_sms.body.upper() or \
                'Daily limit exceeded' in raw_sms.body or \
                'PERSON TO PERSON' in raw_sms.body.upper():
            logger.info("Сообщение OTP типа.")
            PARSED_OK.inc()
            await msg.ack()
            logger.info("Сообщение пропущено.")
            return 

        logger.info("Начинаем парсинг с LLM")
        # Основная логика парсинга
        with sentry_sdk.start_span(name="parsing"):
            with PROCESSING_TIME.time():
                try:
                    with GEMINI_LATENCY.time():
                        parsed = parse_sms_llm(raw_sms)
                        logger.debug("✅  P: ok %s", msg.metadata.sequence)
                except BrokenMessage as err:
                    logger.error("Сообщение пропущено: %s. SMS: %s", err, raw_sms)
                    PARSED_SKIP.inc()
                    await msg.ack()
                    return
                except Exception as err:
                    logger.error("❌  P: fail %s – %s", msg.metadata.sequence, err)
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
        with sentry_sdk.start_span(name="validate_parsed"):
            try:
                parsed_sms = ParsedSMS(**parsed.model_dump())
            except Exception as err:
                failure_payload = json.dumps({"err": str(err), "entry": msg.data.decode(errors='ignore')}).encode()
                sentry_capture(err, extras={"raw_data": msg.data.decode(errors='ignore')})
                await js.publish(SUBJECT_FAILED, failure_payload)
                PARSED_FAIL.inc()
                await msg.ack()
                return
        with sentry_sdk.start_span(name="publish"):
            if not isinstance(parsed_sms.date, datetime):
                logger.warning("Не считалась дата: %s", raw_sms.body[:60])
            if parsed.date > datetime.now():
                logger.error(f"Дата больше чем сегодня: {parsed.date}")
                failure_payload = json.dumps({"err": str("Дата больше чем сегодня"), "entry": msg.data.decode(errors='ignore')}).encode()
                sentry_capture(Exception("Дата больше чем текущая"), extras={"raw_data": msg.data.decode(errors='ignore')})
                await js.publish(SUBJECT_FAILED, failure_payload)
                PARSED_FAIL.inc()
                await msg.ack()
            else:
                logger.info(f"Start publish: {raw_sms.body[:120]}")
                success_payload = parsed_sms.model_dump_json().encode()
                await js.publish(SUBJECT_PARSED, success_payload)
                await js.publish(SUBJECT_PROCESSING, success_payload)
                PARSED_OK.inc()
                logger.info("Успешно обработано: %s", raw_sms.body[:120])
                logger.debug("Сообщение: %s", success_payload)
                await msg.ack()

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
    p.add_argument("--group", default="parser_worker", help="Имя группы консьюмеров (durable name)")
    return p.parse_args(argv)


async def _stats_loop(js, durable):
    while True:
        info = await js.consumer_info("SMS", durable)  # один системный RPC
        ACK_PENDING.set(info.num_ack_pending)
        await asyncio.sleep(5)                         # частота обновления


async def _amain(argv: list[str] | None = None) -> None:  # pragma: no cover
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    _ = get_settings()

    # Metrics & Sentry first
    start_metrics_server()
    init_sentry(release="parser_worker@2.0.0")

    nc = await get_nats_connection()
    
    # Убеждаемся, что стрим и все нужные нам субъекты существуют
    await ensure_stream(nc=nc)

    # Graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    
    def _sig_handler(*_: Any) -> None:
        logger.info("Получен сигнал остановки, завершаем работу...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _sig_handler)

    worker_task = asyncio.create_task(_worker_loop(nc, args.group))
    stats_task  = asyncio.create_task(_stats_loop(nc.jetstream(), args.group))

    await stop_event.wait()
    
    worker_task.cancel()
    stats_task.cancel()
    await nc.drain()

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
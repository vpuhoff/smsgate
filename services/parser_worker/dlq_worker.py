# services/parser_worker/dlq_worker.py
"""
DLQ-воркер: читает сообщения из SUBJECT_FAILED (DLQ) и выводит их в консоль.
Опционально (--reparse) пытается снова пропарсить SMS, используя _process_one
из основного worker.py.  Нужен исключительно для ручной отладки.

Пример:
    python -m services.parser_worker.dlq_worker --group parser_debug --reparse
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import suppress
from typing import Any

from nats.aio.client import Client as NATS
from nats.js.api import DeliverPolicy
from nats.aio.msg import Msg
# import sentry_sdk                                              # type: ignore

# --- импорт всего необходимого из штатного воркера ------------
from .worker import _process_one, get_settings, ensure_stream  # noqa: F401
from libs.nats_utils import SUBJECT_FAILED, get_nats_connection

logger = logging.getLogger("dlq_worker")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# DLQ-обработчик
# ---------------------------------------------------------------------------
async def _handle_dlq_msg(nc: NATS, msg: Msg, reparse: bool) -> None:
    """
    Показывает содержимое сообщения из DLQ.
    Если reparse=True, пытается заново распарсить «сырую» SMS при помощи
    _process_one.  При этом:
      * Ack поступает в любом случае, чтобы сообщение не висело в pending.
      * Никаких публикаций в SUBJECT_PARSED / SUBJECT_PROCESSING не делаем —
        воркер чисто отладочный.
    """
    try:
        payload = json.loads(msg.data)
    except Exception:
        logger.error("Не JSON?! raw=%s", msg.data[:120])
        await msg.ack()
        return

    logger.info("-" * 80)
    logger.info("DLQ message seq=%s", msg.metadata.sequence)
    logger.info(">> payload: %s", json.dumps(payload, ensure_ascii=False, indent=2))

    if reparse:
        raw_entry = payload.get("raw", None)
        if raw_entry is None:
            logger.warning("В payload нет ключа 'raw', нечего парсить повторно")
            await msg.ack()
        else:
            # DLQ пишет either bytes.decode или model_dump_json.
            # Сначала пытаемся прочитать как JSON, иначе оставляем строкой.
            try:
                logger.info("🔄  Повторный парсинг через _process_one …")
                # Профилирование Sentry (только если включено)
                try:
                    import sentry_sdk
                    sentry_sdk.profiler.start_profiler()
                    await _process_one(nc, msg)
                    sentry_sdk.profiler.stop_profiler()
                except ImportError:
                    await _process_one(nc, msg)
            except Exception as err:
                logger.exception("Сбой при повторном парсинге: %s", err)


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------
async def _dlq_loop(nc: NATS, consumer_group: str, reparse: bool) -> None:
    js = nc.jetstream()
    sub = await js.subscribe(SUBJECT_FAILED, durable=consumer_group) # deliver_policy=DeliverPolicy.ALL 
    logger.info("DLQ-воркер запущен (group=%s). Слушаем '%s'…",
                consumer_group, SUBJECT_FAILED)
    async for msg in sub.messages:
        await _handle_dlq_msg(nc, msg, reparse)


# ---------------------------------------------------------------------------
# Entrypoint helpers
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DLQ debug worker for SMS pipeline")
    p.add_argument("--name", default=f"{os.uname().nodename}-{os.getpid()}",
                   help="Уникальное имя этого воркера")
    p.add_argument("--group", default="parser_worker_dlq",
                   help="Durable-группа для чтения DLQ")
    p.add_argument("--reparse", action="store_true",
                   help="Повторно пропускать сообщения через _process_one")
    return p.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    _ = get_settings()                      # прогреваем конфиг

    # Инициализируем Sentry через наш модуль (будет no-op если отключен)
    from libs.sentry import init_sentry
    init_sentry(release="dlq_worker@1.0.0")

    nc = await get_nats_connection()
    await ensure_stream(nc)                 # на всякий случай

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _sig_handler(*_: Any) -> None:
        logger.info("SIGTERM/SIGINT → shutdown…")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _sig_handler)

    dlq_task = asyncio.create_task(
        _dlq_loop(nc, args.group, args.reparse)
    )

    await stop_event.wait()
    dlq_task.cancel()
    await nc.drain()
    with suppress(asyncio.CancelledError):
        await dlq_task
    await nc.close()
    logger.info("NATS соединение закрыто. Выход.")


def main() -> None:  # pragma: no cover
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

# services/parser_worker/metrics.py
"""Prometheus-метрики для *Parser Worker*.

Экспортируются два типа показателей:
1. **Business** – сколько сообщений успешно распознано / упало в DLQ.
2. **Runtime**  – длина lag в `sms_raw`, время разбора одного сообщения.

> Запуск: вызовите `start_metrics_server()` один раз при старте процесса – он
> поднимет HTTP-endpoint `/metrics` на `METRICS_PORT` (по умолчанию 9102).
"""
from __future__ import annotations

import contextlib
import logging
import os
import time
from typing import Awaitable, Callable, Coroutine, Sequence

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Metric objects (module-level singletons)
# ---------------------------------------------------------------------------
PARSED_OK = Counter(
    "sms_parsed_ok_total",
    "Количество SMS, успешно распознанных парсером",
)
PARSED_FAIL = Counter(
    "sms_parsed_fail_total",
    "Количество SMS, отправленных в DLQ из-за ошибок парсинга",
)
STREAM_LAG = Gauge(
    "sms_parser_stream_lag",
    "Текущее число сообщений, ожидающих разбора в consumer-group",
)
PROCESSING_TIME = Histogram(
    "sms_parser_processing_seconds",
    "Время (сек) затраченное на парс одного сообщения",
    buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)


# ---------------------------------------------------------------------------
# Helper decorators / context managers
# ---------------------------------------------------------------------------

def track_processing(func: Callable[[Redis, str, str, str, str], Awaitable[None]]):
    """Coroutine-decorator that measures processing time & success/failure."""

    async def _wrapper(
        redis: Redis,
        stream: str,
        group: str,
        consumer: str,
        message_id: str,
        raw_payload: str,
    ) -> None:  # pragma: no cover – runtime decorator
        started = time.perf_counter()
        try:
            await func(redis, stream, group, consumer, message_id, raw_payload)
            PARSED_OK.inc()
        except Exception:  # noqa: BLE001  – в метрику ловим вообще всё
            PARSED_FAIL.inc()
            raise
        finally:
            PROCESSING_TIME.observe(time.perf_counter() - started)

    return _wrapper


# async def update_stream_lag(
#     redis: Redis,
#     *,
#     stream: str = STREAM_RAW,
#     group: str,
# ) -> None:
#     """Set **STREAM_LAG** gauge = pending messages for *group* on *stream*."""
#     info = await redis.xpending(stream, group)  # returns tuple[min, max, total, consumers]
#     STREAM_LAG.set(info[2])  # total pending


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def start_metrics_server(port: int | None = None) -> None:  # pragma: no cover – network
    """Запускает HTTP-эндпоинт `/metrics` в отдельном треде.

    *Интеграция с prometheus_client «из коробки».*
    """
    port = port or int(os.getenv("METRICS_PORT", 9102))
    with contextlib.suppress(OSError):  # идемпотентность при повторном импорте
        start_http_server(port)
        log.info("Prometheus metrics available on http://0.0.0.0:%s/metrics", port)

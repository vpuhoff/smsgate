# libs/nats_utils.py
"""NATS-helpers used by all services (async, JetStream-only).

Стараемся держать слой минимальным: единое преобразование моделей в байты
и несколько публичных функций, которые реально нужны микросервисам.
"""
from __future__ import annotations

import logging
from async_lru import alru_cache
from typing import Awaitable, Callable

import nats
from nats.aio.client import Client as NATS
from nats.js.api import StorageType, RetentionPolicy
from nats.js.api import StreamConfig
from nats.js.api import PubAck
from libs.config import get_settings
from libs.models import RawSMS

# ---------------------------------------------------------------------------
# Constants / settings
# ---------------------------------------------------------------------------
# В NATS принято называть каналы "субъектами" (subjects)
SUBJECT_RAW = "sms.raw"          # Cырые сообщения от поставщика
SUBJECT_PARSED = "sms.parsed"    # Успешно обработанные сообщения
SUBJECT_PROCESSING = "sms.processing"    # Успешно обработанные сообщения
SUBJECT_FAILED = "sms.failed"    # DLQ для обработчиков
SUBJECT_CATEGORIZED = "sms.categorized"


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NATS connection singleton
# ---------------------------------------------------------------------------
# lru_cache отлично работает и с async функциями для создания синглтона
@alru_cache(maxsize=1)
async def get_nats_connection() -> NATS:  # pragma: no cover – network
    """Singleton-подключение к NATS.
    
    Создает и возвращает единственное на приложение подключение к NATS.
    """
    settings = get_settings()
    logger.info("Connecting to NATS %s", settings.nats_dsn)
    nc = await nats.connect(settings.nats_dsn)
    return nc


async def ensure_stream(nc: NATS):
    """
    Проверяет существование стрима и создает/обновляет его, если он отсутствует или его конфигурация устарела.
    Эта функция идемпотентна: её можно безопасно вызывать многократно.

    Parameters
    ----------
    nc: NATS
        Активное подключение к NATS.
    stream_name: str
        Имя создаваемого стрима (например, "SMS").
    subjects: list[str]
        Список субъектов, которые будет слушать этот стрим (например, ["sms.raw", "sms.failed"]).
    """
    stream_name = "SMS"
    subjects = [SUBJECT_RAW, SUBJECT_PARSED,
                SUBJECT_FAILED, SUBJECT_PROCESSING, SUBJECT_CATEGORIZED]
    logging.debug(f"Проверка и создание стрима '{stream_name}' для субъектов {subjects}...")
    jsm = nc.jetstream()
    
    config = StreamConfig(
        name=stream_name,
        subjects=subjects,
        storage=StorageType.FILE,
        retention=RetentionPolicy.LIMITS,
        max_age=60 * 60 * 24 * 3,  # 120 дней в секундах
    )

    try:
        stream_info = await jsm.stream_info(stream_name)
        # Если стрим существует, проверяем, не нужно ли обновить список субъектов
        if sorted(stream_info.config.subjects) != sorted(subjects): # type: ignore
            logging.warning(f"Конфигурация стрима '{stream_name}' устарела. Обновляем...")
            await jsm.update_stream(config)
            logging.info(f"✅ Стрим '{stream_name}' успешно обновлен.")
        else:
            logging.debug(f"☑️ Стрим '{stream_name}' уже существует и настроен корректно.")

    except Exception as e:
        logging.error(f"❌ Произошла непредвиденная ошибка при создании/обновлении стрима: {e}")
        raise

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def publish_raw_sms(
    nc: NATS | None,
    sms: RawSMS,
    *,
    subject: str = SUBJECT_RAW,
) -> PubAck:
    """Publish *unparsed* SMS into a NATS JetStream.

    Для работы этой функции на сервере NATS должен быть создан Stream,
    который подписывается на соответствующий subject (например, 'sms.raw').

    Parameters
    ----------
    nc: NATS | None
        Активное подключение; если `None`, будет создан singleton через `get_nats_connection()`.
    sms: RawSMS
        Pydantic-модель «как есть».
    subject: str
        Название NATS subject (по умолчанию `sms.raw`).

    Returns
    -------
    PubAck
        Объект подтверждения от JetStream, содержащий `stream` и `seq` (номер сообщения).
    """
    if nc is None:  # pragma: no cover – convenience
        nc = await get_nats_connection()

    # Убеждаемся, что стрим существует и слушает все нужные каналы
    await ensure_stream(nc=nc)

    js = nc.jetstream()
    payload = sms.model_dump_json().encode('utf-8')
    ack = await js.publish(subject, payload)
    return ack


__all__ = [
    "publish_raw_sms",
    "get_nats_connection",
    "ensure_stream",
    "SUBJECT_RAW",
    "SUBJECT_PARSED",
    "SUBJECT_FAILED",
]
# services/api_gateway/schemas.py
"""Pydantic DTO-models used by *API Gateway*.

Отделяем их от `main.py`, чтобы:
1. Избежать циклических импортов (если понадобятся вспомогательные схемы).
2. Упростить автогенерацию OpenAPI-документации.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class RawSMSPayload(BaseModel):
    """Входящее SMS-сообщение от устройства или XML-watcher.

    Совпадает 1-в-1 с доменной моделью :class:`libs.models.RawSMS`, но хранится
    в отдельном модуле, чтобы слой FastAPI не зависел напрямую от доменных
    моделей.
    """

    device_id: str = Field(...) # example="android-pixel-8a"
    message: str = Field(...)   # example="APPROVED PURCHASE DB SALE: …"
    sender: str = Field(...)    # example="AMTBBANK"
    timestamp: int = Field(...)
    source: str | None = Field(None) # device | xml_backup | other…

    class Config:
        json_schema_extra = {
            "description": "Raw SMS message as sent by the mobile device or backup-watcher.",
        }

class ShortSMSPayload(BaseModel):
    """Входящее SMS-сообщение от устройства или XML-watcher.

    Совпадает 1-в-1 с доменной моделью :class:`libs.models.RawSMS`, но хранится
    в отдельном модуле, чтобы слой FastAPI не зависел напрямую от доменных
    моделей.
    """

    device_id: str = Field(...) # example="android-pixel-8a"
    msg_id: str = Field(...) # example="1718291822123"
    message: str = Field(...) # example="APPROVED PURCHASE DB SALE: …"
    sender: str = Field(...) # example="AMTBBANK"
    timestamp: str = Field(...) # example="2025-02-17"
    source: str | None = Field(None) # device | xml_backup | other… 

    class Config:
        json_schema_extra = {
            "description": "Raw SMS message as sent by the mobile device or backup-watcher.",
        }

class RawSMSResponse(BaseModel):
    """Упрощённый ответ API после приёма сообщения."""

    result: str = Field("queued", description="always 'queued' if 202 accepted")

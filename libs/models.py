# libs/models.py
"""Domain models shared by all services.

The system intentionally passes around *only* these objects (JSON-serialised)
across the Redis streams so every component speaks the same language.

Levels
------
1. **RawSMS** – exactly what пришло в шлюз: минимальная нормализация, никаких
   выводов о типе операции.
2. **ParsedSMS** – результат детерминированного разбора регулярками. Это то, что
   летит дальше в PocketBase-writer.

Дизайн-оговорка: мы используем Pydantic v2 (BaseModel) для полной валидации и
удобного JSON-dump (`model_dump_json()`).  В каждом сервисе следует работать с
этими классами напрямую, избегая произвольных dict-ов.
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from enum import Enum
from typing import Literal, Optional
import hashlib

from pydantic import BaseModel, Field, field_validator, validator

__all__ = [
    "RawSMS",
    "ParsedSMS",
    "TxnType",
]


class TxnType(str, Enum):
    """Классификация СМС после парсинга."""

    DEBIT = "debit"  # списание
    CREDIT = "credit"  # пополнение
    OTP = "otp"  # одноразовый код / не транзакция
    UNKNOWN = "unknown"  # не распознано


class RawSMS(BaseModel):
    """Что отдает *любой* инжестер.

    Все поля обязательны, кроме `device_id`, если источник – XML-бэкап.
    """

    msg_id: str = Field(..., description="Уникальный ID сообщения")
    sender: str = Field(..., min_length=1)
    body: str = Field(..., min_length=1)
    date: str = Field(..., description="Дата/время на телефоне")
    device_id: Optional[str] = Field(None, description="IMEI или кастомный ID")
    source: Literal["device", "xml"] = Field(
        "device", description="Откуда пришло сообщение"
    )


class ParsedSMS(BaseModel):
    """Нормализованный результат парсинга."""

    # --- первичное -----------------------------------------------------------
    msg_id: str
    device_id: Optional[str]
    sender: str
    date: _dt.datetime
    raw_body: str = Field(..., description="Оригинальный текст СМС")

    # --- выводы парсера ------------------------------------------------------
    txn_type: TxnType
    amount: Optional[Decimal] = None
    currency: Optional[str] = None  # ISO 4217 (AMD, USD, EUR)
    card: Optional[str] = Field(None, min_length=4, max_length=4)

    merchant: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None

    balance: Optional[Decimal] = None  # после операции (если доступно)

    # --- служебное -----------------------------------------------------------
    parser_version: str = Field("0.1.0", description="SemVer парсера")

    model_config = {
        "json_encoders": {
            _dt.datetime: lambda v: v.isoformat(),
            Decimal: lambda v: str(v),
        }
    }

    # --- валидации -----------------------------------------------------------
    @field_validator("currency")
    def _upper_currency(cls, v: str | None) -> str | None:  # noqa: N805
        return v.upper() if v else v

def get_md5_hash(input_string: str) -> str:
  """
  Вычисляет MD5-хеш для входной строки.

  Args:
    input_string: Строка для хеширования.

  Returns:
    MD5-хеш в виде шестнадцатеричной строки.
  """
  # Кодируем строку в байты (UTF-8) и вычисляем хеш
  md5_hash = hashlib.md5(input_string.encode('utf-8')).hexdigest()
  return md5_hash

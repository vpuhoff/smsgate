# tests/test_purchase_sale.py
from decimal import Decimal
from datetime import datetime

import pytest

from libs.gemini_parser import parse_sms_llm
from libs.decimal_utils import parse_ambiguous_decimal
from libs.models import RawSMS, TxnType


CASES = [
    # ─ 1. С адресом ────────────────────────────────────────────────
    (
        "APPROVED PURCHASE DB SALE: KHOJAYAN LLC, YEREVAN, "
        "GYULBENKYAN STR. 29, 24 AREA,06.05.25 14:23,card ***0018. "
        "Amount:5200.00 AMD, Balance:184260.74 AMD",
        dict(
            merchant="KHOJAYAN LLC",
            city="YEREVAN",
            address="GYULBENKYAN STR. 29, 24 AREA",
            amount=Decimal("5200.00"),
            balance=Decimal("184260.74"),
            date=datetime(2025, 5, 6, 14, 23),
        ),
    ),
    # ─ 2. Без адреса ───────────────────────────────────────────────
    (
        "APPROVED PURCHASE DB SALE: BAREV, YEREVAN,"
        "06.05.25 15:11,card ***0018. Amount:3460.00 AMD, "
        "Balance:180800.74 AMD",
        dict(
            merchant="BAREV",
            city="YEREVAN",
            address="null",
            amount=Decimal("3460.00"),
            balance=Decimal("180800.74"),
            date=datetime(2025, 5, 6, 15, 11),
        ),
    ),
]


def _mk_raw(body: str) -> RawSMS:
    """Упрощённая фабрика RawSMS для тестов."""
    return RawSMS(
        msg_id="test-msg-id",
        device_id="test-device",
        sender="BANK",
        date="2025-05-06T00:00:00",
        body=body,
        source="device"
    )


@pytest.mark.parametrize("sms_body, expected", CASES)
def test_parse_purchase_sale(sms_body: str, expected: dict):
    result = parse_sms_llm(_mk_raw(sms_body))

    assert result is not None
    # сквозные
    assert result.raw_body == sms_body
    # извлечённые
    assert result.txn_type == TxnType.DEBIT
    assert result.merchant == expected["merchant"]
    assert result.city == expected["city"]
    assert result.address == expected["address"]
    assert result.card == "0018"
    assert result.amount == expected["amount"]
    assert result.currency == "AMD"
    assert result.balance == expected["balance"]
    assert result.date == expected["date"]


def test_parse_decimals():
    test_cases = [
        '79,825.89',     # Американский формат
        '79.825,89',     # Европейский формат
        '79 825,89',     # С пробелом как разделителем тысяч
        '1,234,567.89',  # Много разделителей тысяч
        '1.234.567,89',  # Много разделителей тысяч
        '123456',        # Целое число
        '123.45',        # Простое десятичное
        '1,23',          # Десятичное с запятой
        '1,000',         # Неоднозначность: скорее всего, целое число 1000
        '999,999'        # Целое
    ]

    for case in test_cases:
        try:
            result = parse_ambiguous_decimal(case)
            print(f"'{case}' -> {result} (тип: {type(result)})")
        except ValueError as e:
            print(e)
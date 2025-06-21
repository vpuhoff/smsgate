# tests/test_purchase_sale.py
from decimal import Decimal
from datetime import datetime

import pytest

from libs.gemini_parser import parse_sms_llm
from libs.decimal_utils import parse_ambiguous_decimal
from libs.models import ParsedSMS, RawSMS, TxnType

CASES = [
    # ─ 1. С адресом ────────────────────────────────────────────────
    (
        "APPROVED PURCHASE DB SALE: TEST LLC, MOSKOW, "
        "TEST STR. 29, 24 AREA,06.05.25 14:23,card ***0018. "
        "Amount:52.00 USD, Balance:1842.74 USD",
        dict(
            merchant="TEST LLC",
            city="MOSKOW",
            address="TEST STR. 29, 24 AREA",
            amount=Decimal("52.00"),
            balance=Decimal("1842.74"),
            date=datetime(2025, 5, 6, 14, 23),
            card="0018",
            currency="USD"
        ),
    ),
    # ─ 2. Без адреса ───────────────────────────────────────────────
    (
        "APPROVED PURCHASE DB SALE: TEST, MOSKOW,"
        "06.05.25 15:11,card ***0018. Amount:3460.00 USD, "
        "Balance:1800.74 USD",
        dict(
            merchant="TEST",
            city="MOSKOW",
            address="",
            amount=Decimal("3460.00"),
            balance=Decimal("1800.74"),
            date=datetime(2025, 5, 6, 15, 11),
            card="0018",
            currency="USD"
        ),
    ),
    (
        "DEBIT ACCOUNT&#10;27,252.00 AMD&#10;4083***7538,&#10;AMERIABANK API GATE, AM"
        "&#10;10.06.2025 20:51&#10;BALANCE: 391,469.09 AMD",
        dict(
            merchant="AMERIABANK API GATE",
            city="AM",
            address="",
            amount=Decimal("27252.00"),
            balance=Decimal("391469.09"),
            date=datetime(2025, 6, 10, 20, 51),
            card="7538",
            currency="AMD"
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
    assert result.txn_type == TxnType.DEBIT
    assert result.merchant == expected["merchant"]
    assert result.city == expected["city"]
    assert result.address == expected["address"]
    assert result.card == expected["card"]
    assert result.amount == expected["amount"]
    assert result.currency == expected["currency"]
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

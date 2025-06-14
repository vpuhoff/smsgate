# libs/regexes.py
"""Single point of truth for **all** regular-expression patterns and a helper
that converts an arbitrary bank SMS into :class:`libs.models.ParsedSMS`.

Новая схема = достаточно добавить паттерн в таблицу ниже – *парсер-воркеры
подхватят автоматически*.
"""
from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Callable, Match, Optional

from libs.models import ParsedSMS, RawSMS, TxnType

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
# Общие части
DATE_RE = r"(?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})"
TIME_RE = r"(?P<time>\d{2}:\d{2})"
CARD_RE = r"card\s+\*{2,}(?P<card>\d{4})"
AMOUNT_RE = r"Amount:\s*(?P<amount>[\d,.]+)\s+(?P<currency>[A-Z]{3})"
BALANCE_RE = r"Balance:\s*(?P<balance>[\d,.]+)\s+(?P=currency)"


# --- 1. Покупка в обычном магазине ------------------------------------------------
PURCHASE_SALE_RE = re.compile(
    rf"""
    ^APPROVED\s+PURCHASE\s+DB\s+SALE:\s+
    (?P<merchant>[^,]+?),\s*
    (?P<city>[^,]+?),\s*
    (?P<address>[^,]+?),\s*
    {DATE_RE}\s+{TIME_RE},\s*
    {CARD_RE}\.\s*
    {AMOUNT_RE},\s*
    {BALANCE_RE}$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# --- 2. Интернет-покупка ----------------------------------------------------------
PURCHASE_INET_RE = re.compile(
    rf"""
    ^APPROVED\s+PURCHASE\s+DB\s+INTERNET:\s+
    (?P<description>.+?),\s*
    {DATE_RE}\s+{TIME_RE},\s*
    {CARD_RE}\.\s*
    {AMOUNT_RE},\s*
    {BALANCE_RE}$
    """,
    re.VERBOSE | re.IGNORECASE,
)


# --- 3. CREDIT PAYMENT (погашение кредита) ---------------------------------------
CREDIT_PAYMENT_RE = re.compile(
    rf"""
    ^CREDIT\s+PAYMENT:\s*
    {DATE_RE}\s+{TIME_RE},\s*
    {CARD_RE}\.\s*
    {AMOUNT_RE},\s*
    Balance:\s*[\d,.]+\s+(?P=currency)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# --- 4. C2C RECEIVED (входящий перевод) ------------------------------------------
C2C_RECEIVED_RE = re.compile(
    rf"""
    ^C2C\s+RECEIVED\s*
    {AMOUNT_RE},\s*
    {CARD_RE},\s*
    (?P<sender>[^,]+?),\s*
    (?:{DATE_RE}\s+{TIME_RE},?)?\s* # <-- ИЗМЕНЕНИЕ ЗДЕСЬ: дата/время опциональны
    Balance:\s*[\d,.]+\s+(?P=currency)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# --- 5. OTP – парсим *только* чтобы понять, что пропустить -----------------------
OTP_RE = re.compile(r"\bOTP\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helper table: pattern → builder
# ---------------------------------------------------------------------------
Builder = Callable[[Match[str], RawSMS], ParsedSMS]


def _make_purchase_sale(m: Match[str], raw_sms: RawSMS) -> ParsedSMS:
    """Builder for regular store purchases."""
    return ParsedSMS(
        msg_id=raw_sms.msg_id,
        device_id=raw_sms.device_id,
        sender=raw_sms.sender,
        date=_to_timestamp(m["date"], m["time"]),
        raw_body=raw_sms.body,
        txn_type=TxnType.DEBIT,
        amount=Decimal(m["amount"].replace(",", "")),
        currency=m["currency"],
        card=m["card"],
        merchant=m["merchant"].strip(),
        city=m["city"].strip(),
        address=m["address"].strip(),
        balance=Decimal(m["balance"].replace(",", "")),
        parser_version="0.1.1"
    )


def _make_purchase_inet(m: Match[str], raw_sms: RawSMS) -> ParsedSMS:
    """Builder for internet purchases (e-commerce)."""
    description_parts = [p.strip() for p in m["description"].strip().split(',')]
    merchant = description_parts[0]
    address = ", ".join(description_parts[1:]) if len(description_parts) > 1 else None

    return ParsedSMS(
        msg_id=raw_sms.msg_id,
        device_id=raw_sms.device_id,
        sender=raw_sms.sender,
        date=_to_timestamp(m["date"], m["time"]),
        raw_body=raw_sms.body,
        txn_type=TxnType.DEBIT,
        amount=Decimal(m["amount"].replace(",", "")),
        currency=m["currency"],
        card=m["card"],
        merchant=merchant,
        address=address,
        balance=Decimal(m["balance"].replace(",", "")),
        parser_version="0.1.1"
    )


def _make_credit_payment(m: Match[str], raw_sms: RawSMS) -> ParsedSMS:
    """Builder for credit line payments."""
    return ParsedSMS(
        msg_id=raw_sms.msg_id,
        device_id=raw_sms.device_id,
        sender=raw_sms.sender,
        date=_to_timestamp(m["date"], m["time"]),
        raw_body=raw_sms.body,
        txn_type=TxnType.DEBIT,
        amount=Decimal(m["amount"].replace(",", "")),
        currency=m["currency"],
        card=m["card"],
        parser_version="0.1.1"
    )


def _make_c2c_received(m: Match[str], raw_sms: RawSMS) -> ParsedSMS:
    """Builder for incoming C2C transfers."""
    try:
        # Дата/время в сообщении приоритетнее, т.к. точнее
        timestamp = _to_timestamp(m["date"], m["time"])
    except (KeyError, IndexError):
        # Если в СМС нет даты, берем ее из метаданных RawSMS
        timestamp = datetime.fromisoformat(raw_sms.date)

    return ParsedSMS(
        msg_id=raw_sms.msg_id,
        device_id=raw_sms.device_id,
        sender=raw_sms.sender,
        date=timestamp,
        raw_body=raw_sms.body,
        txn_type=TxnType.CREDIT,
        amount=Decimal(m["amount"].replace(",", "")),
        currency=m["currency"],
        card=m["card"],
        merchant=m["sender"].strip(), # "Отправитель" в данном контексте - наш "мерчант"
        parser_version="0.1.1"
    )


_PATTERNS: list[tuple[re.Pattern[str], Builder]] = [
    (PURCHASE_SALE_RE, _make_purchase_sale),
    (PURCHASE_INET_RE, _make_purchase_inet),
    (CREDIT_PAYMENT_RE, _make_credit_payment),
    (C2C_RECEIVED_RE, _make_c2c_received),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def parse_sms(raw_sms: RawSMS) -> Optional[ParsedSMS]:
    """Attempt to recognise `raw_sms.body` using one of the known patterns.

    Returns
    -------
    ParsedSMS | None
        * Instance of ParsedSMS if message matched.
        * `None` — when message is unrecognised.
    """
    if OTP_RE.search(raw_sms.body):
        return ParsedSMS(
            # --- Первичные поля из исходного сообщения ---
            msg_id=raw_sms.msg_id,
            device_id=raw_sms.device_id,
            sender=raw_sms.sender,
            date=datetime.fromisoformat(raw_sms.date),
            raw_body=raw_sms.body,

            # --- Выводы парсера ---
            # Главный вывод - это тип сообщения
            txn_type=TxnType.OTP,

            # Для OTP-сообщений все финансовые и транзакционные поля
            # не имеют смысла, поэтому явно указываем их как None.
            amount=None,
            currency=None,
            card=None,
            merchant=None,
            city=None,
            address=None,
            balance=None,

            # --- Служебные поля ---
            # Явно указываем версию парсера
            parser_version="0.1.0",
        )

    for pattern, builder in _PATTERNS:
        if match := pattern.match(raw_sms.body):
            try:
                return builder(match, raw_sms)
            except Exception as exc:  # pragma: no cover – safety net
                # worker will call sentry_capture if None.
                raise ValueError(
                    f"Failed to build ParsedSMS for pattern {pattern.pattern}"
                ) from exc
    return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _to_timestamp(date_str: str, time_str: str) -> datetime:
    """Converts substrings to a single :class:`datetime`.

    Supports both 2- and 4-digit years (assumes 2000-based for 2-digit).
    """
    day, month, year = date_str.replace("/", ".").replace("-", ".").split(".")
    year = int(year)
    if year < 100:
        year += 2000
    dt_str = f"{year}-{month}-{day} {time_str}"
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")


__all__ = [
    "parse_sms",
]
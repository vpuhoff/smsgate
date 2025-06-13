"""SMS transaction processor with Sentry error reporting and local diskcache fallbacks.

* Parses debit (purchase/sale) and credit (incoming payment) SMS messages from Armenian
  bank notifications and converts them into structured dictionaries.
* Uses three DiskCache stores:
    - SOURCE_CACHE_DIR   – raw SMS payloads fetched from Android/IMAP/etc.
    - PURCHASE_CACHE_DIR – successfully parsed debit transactions.
    - CREDIT_CACHE_DIR   – successfully parsed credit transactions.
* Sends all runtime errors to Sentry **and** stores a copy of every captured event
  in `SENTRY_CACHE_DIR`, ensuring nothing is lost if the network is offline.

Supported debit prefixes so far:
    • "PURCHASE: …"
    • "SALE: …"
    • "PURCHASE DB INTERNET: …"         (online, e‑commerce)
    • "PURCH.COMPLETION.DB INTERNET: …" (post‑auth completion)

If you spot a new prefix, just add it to *TRANSACTION_RE*’s alternation list.

Environment variables expected (recommended via a dotenv or Docker secrets):
    SENTRY_DSN                 ─ your project DSN (optional → Sentry disabled)
    SENTRY_TRACES_SAMPLE_RATE  ─ overrides traces_sample_rate (defaults to 0.0)
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from diskcache import Cache

try:
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration
except ImportError:  # pragma: no cover – Sentry is optional
    sentry_sdk = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sentry setup (optional)
# ---------------------------------------------------------------------------
SENTRY_DSN = os.environ.get("SENTRY_DSN")
SENTRY_CACHE_DIR = "sentry_event_cache"


def _init_sentry() -> None:  # noqa: D401 – short description fine
    """Initialise Sentry SDK with a *very* small wrapper to persist events locally."""

    if not SENTRY_DSN or sentry_sdk is None:  # type: ignore[truthy-bool]
        logger.info("Sentry disabled – either DSN not set or sentry-sdk missing")
        return

    def _store_event_locally(event: dict, _hint: dict | None) -> None:
        """Persist event to *SENTRY_CACHE_DIR* (fire‑and‑forget)."""
        try:
            with Cache(SENTRY_CACHE_DIR) as cache:
                cache.set(event.get("event_id"), event)
        except Exception as exc:  # noqa: BLE001, S110
            logger.error("Failed to store Sentry event locally: %s", exc)

    sentry_logging = LoggingIntegration(level=logging.ERROR, event_level=logging.ERROR)

    sentry_sdk.init(  # type: ignore[attr-defined]
        dsn=SENTRY_DSN,
        integrations=[sentry_logging],
        traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.0")),
        before_send=lambda e, h: (_store_event_locally(e, h) or e),
    )


_init_sentry()

# ---------------------------------------------------------------------------
# DiskCache directories
# ---------------------------------------------------------------------------
SOURCE_CACHE_DIR = "sms_cache"
PURCHASE_CACHE_DIR = "parsed_sms_cache"  # debit messages
CREDIT_CACHE_DIR = "credit_sms_cache"    # credit messages

# ---------------------------------------------------------------------------
# Regular expressions
# ---------------------------------------------------------------------------
TRANSACTION_RE = re.compile(
    r"""
    ^.*?                              # префикс (APPROVED, REVERSE, etc.)
    (?:                               # допустимые ключевые слова
        PURCHASE\s+DB\s+INTERNET   | # e‑commerce auth
        PURCH\.COMPLETION\.DB\s+INTERNET | # e‑commerce completion
        PURCHASE                      | # классическая покупка
        SALE                            # списание
    ):\s*
    (?P<merchant>.+?),\s*
    (?:                                # необязательный блок с городом
        (?P<city>YEREVAN),\s*
    )?
    (?P<address>.*?)(?=,\s*\d{2}[./-]\d{2}[./-]\d{2,4}\s) # адрес до даты
    ,\s*(?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\s+\*{3}(?P<card>\d{4})\.\s*
    Amount:\s*(?P<amount>[\d,.]+)\s+
    (?P<currency>\w{3}),\s*
    Balance:\s*(?P<balance>[\d,.]+)
    """,
    re.VERBOSE | re.IGNORECASE | re.DOTALL,
)

CREDIT_PAYMENT_RE = re.compile(
    r"""
    ^.*?                              # произвольное начало
    (?P<type>[\w\s]+?):\s*
    (?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\s+\*{3}(?P<card>\d{4})\.\s*
    Amount:\s*(?P<amount>[\d.,]+)\s+
    (?P<currency>\w{3}),\s*
    Balance:\s*(?P<balance>[\d.,]+)\s+
    (?:\w{3})\s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_decimal(value: str) -> Decimal:
    """Convert a *numeric* string with optional thousand separators to **Decimal**."""
    return Decimal(value.replace(",", ""))


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_transaction_message(message: str) -> Optional[Dict[str, Any]]:
    """Parse debit (purchase/sale) SMS payload."""
    if not message:
        return None
    match = TRANSACTION_RE.search(message)
    if not match:
        return None

    g = match.groupdict()
    try:
        city_value = (g.get("city") or "ONLINE").upper()
        return {
            "direction": "debit",
            "merchant": g["merchant"].strip(),
            "city": city_value,
            "address": g["address"].strip() or "N/A",
            "date": g["date"].strip(),
            "time": g["time"].strip(),
            "card": f"***{g['card']}",
            "amount": float(_to_decimal(g["amount"])),
            "currency": g["currency"].strip(),
            "balance": float(_to_decimal(g["balance"])),
        }
    except (InvalidOperation, TypeError) as exc:
        logger.error("Ошибка конвертации суммы/баланса: %s. Текст: %s", exc, message)
        if sentry_sdk is not None:  # type: ignore[truthy-bool]
            sentry_sdk.capture_exception(exc)  # type: ignore[attr-defined]
        return None


def parse_credit_message(message: str) -> Optional[Dict[str, Any]]:
    """Parse incoming credit/payment SMS payload."""
    if not message:
        return None
    match = CREDIT_PAYMENT_RE.search(message)
    if not match:
        return None

    g = match.groupdict()
    try:
        return {
            "direction": "credit",
            "type": g["type"].strip().upper(),
            "date": g["date"].strip(),
            "time": g["time"].strip(),
            "card": f"***{g['card']}",
            "amount": float(_to_decimal(g["amount"])),
            "currency": g["currency"].strip(),
            "balance": float(_to_decimal(g["balance"])),
        }
    except (InvalidOperation, TypeError) as exc:
        logger.error("Ошибка конвертации суммы/баланса: %s. Текст: %s", exc, message)
        if sentry_sdk is not None:  # type: ignore[truthy-bool]
            sentry_sdk.capture_exception(exc)  # type: ignore[attr-defined]
        return None

# ---------------------------------------------------------------------------
# Main processing routine
# ---------------------------------------------------------------------------

def process_sms_from_cache(
    source_dir: str = SOURCE_CACHE_DIR,
    purchase_dest_dir: str = PURCHASE_CACHE_DIR,
    credit_dest_dir: str = CREDIT_CACHE_DIR,
) -> None:
    """Process raw SMS messages from *source_dir* and fan‑out to two result caches."""

    with Cache(source_dir) as source_cache, Cache(purchase_dest_dir) as purchase_cache, Cache(credit_dest_dir) as credit_cache:
        logger.info("Начало обработки сообщений из кэша: %s", source_dir)

        stats = {
            "processed_debit": 0,
            "processed_credit": 0,
            "failed": 0,
            "skipped": 0,
        }

        for key in list(source_cache):
            message_data: dict[str, Any] = source_cache.get(key)  # type: ignore[arg-type]
            if message_data.get("status") == "processed":
                stats["skipped"] += 1
                continue

            body_text = (message_data.get("body") or "").strip()

            # -- Skip OTP/non‑transactional alerts ----------------------------------
            if "OTP" in body_text.upper() or "PASS=" in body_text.upper() or "CODE:" in body_text.upper():
                message_data["status"] = "skipped_otp"
                source_cache.set(key, message_data)
                stats["skipped"] += 1
                continue

            # Try debit → credit ----------------------------------------------------
            parsed = parse_transaction_message(body_text)
            target_cache = purchase_cache
            if parsed:
                stats["processed_debit"] += 1
            else:
                parsed = parse_credit_message(body_text)
                target_cache = credit_cache
                if parsed:
                    stats["processed_credit"] += 1

            if parsed:
                body_hash = hashlib.sha256(body_text.encode("utf-8")).hexdigest()
                parsed["original_key"] = key
                parsed["original_body"] = body_text
                target_cache.set(body_hash, parsed)
                message_data["status"] = "processed"
            else:
                message_data["status"] = "failed_to_parse"
                stats["failed"] += 1

            source_cache.set(key, message_data)

        logger.info(
            "Обработка завершена. Покупки: %d, Зачисления: %d, Ошибки: %d, Пропущено: %d",
            stats["processed_debit"], stats["processed_credit"], stats["failed"], stats["skipped"],
        )


# ---------------------------------------------------------------------------
# Verification helper (optional)
# ---------------------------------------------------------------------------

def verify_processed_cache(cache_dir: str, label: str) -> None:
    logger.info("\n--- Проверка кэша %s ---", label)
    with Cache(cache_dir) as cache:
        keys = list(cache)
        if not keys:
            logger.warning("Кэш '%s' пуст.", label)
            return
        logger.info("Всего записей в '%s': %d", label, len(keys))
        for key in keys[:3]:  # показываем до 3 записей
            logger.info("Запись %s: %s", key[:15] + "...", cache.get(key))


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    process_sms_from_cache()

    # Optional inspection of result caches – remove in production
    verify_processed_cache(PURCHASE_CACHE_DIR, "покупки/списания")
    verify_processed_cache(CREDIT_CACHE_DIR, "зачисления")
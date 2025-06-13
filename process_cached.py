import re
import hashlib
import logging
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any
from diskcache import Cache

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Константы кэшей ---
SOURCE_CACHE_DIR = 'sms_cache'
PURCHASE_CACHE_DIR = 'parsed_sms_cache'  # для сообщений о покупках/списаниях
CREDIT_CACHE_DIR = 'credit_sms_cache'    # для сообщений о зачислениях

# --- Регулярные выражения ---
# Покупка / списание
TRANSACTION_RE = re.compile(
    r"""
    ^.*?                              # любой мусор в начале
    (?:PURCHASE|SALE):\s*            # ключевое слово
    (?P<merchant>.+?),\s*YEREVAN,\s* # название магазина
    (?P<address>.*?)(?=,\s*\d{2}[./-]\d{2}[./-]\d{2,4}\s) # адрес до даты
    ,\s*(?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\ \*{3}(?P<card>\d{4})\.\s*
    Amount:\s*(?P<amount>[\d,.]+)\s+
    (?P<currency>\w{3}),\s*
    Balance:\s*(?P<balance>[\d,.]+)
    """,
    re.VERBOSE | re.IGNORECASE | re.DOTALL,
)

# Зачисление / платеж
CREDIT_PAYMENT_RE = re.compile(r"""
    ^.*?                              # произвольное начало
    (?P<type>[\w\s]+?):\s*            # тип транзакции
    (?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\s+\*{3}(?P<card>\d{4})\.\s*
    Amount:\s*(?P<amount>[\d.,]+)\s+
    (?P<currency>\w{3}),\s*           # код валюты суммы (USD и т.-д.)
    Balance:\s*(?P<balance>[\d.,]+)\s+
    (?:\w{3})\s*$                     # код валюты баланса, игнорируем
""", re.VERBOSE | re.IGNORECASE)

# --- Парсеры ---

def _to_decimal(value: str) -> Decimal:
    """Преобразуем строку-число, убирая разделители тысяч."""
    return Decimal(value.replace(',', ''))


def parse_transaction_message(message: str) -> Optional[Dict[str, Any]]:
    """Парсит SMS о покупке/списании."""
    if not message:
        return None
    match = TRANSACTION_RE.search(message)
    if not match:
        return None
    g = match.groupdict()
    try:
        return {
            'direction': 'debit',
            'merchant': g['merchant'].strip(),
            'city': 'YEREVAN',
            'address': g['address'].strip() or 'N/A',
            'date': g['date'].strip(),
            'time': g['time'].strip(),
            'card': f"***{g['card']}",
            'amount': float(_to_decimal(g['amount'])),
            'currency': g['currency'].strip(),
            'balance': float(_to_decimal(g['balance'])),
        }
    except (InvalidOperation, TypeError) as exc:
        logging.error("Ошибка конвертации суммы/баланса: %s. Текст: %s", exc, message)
        return None


def parse_credit_message(message: str) -> Optional[Dict[str, Any]]:
    """Парсит SMS о зачислении средств или платеже."""
    if not message:
        return None
    match = CREDIT_PAYMENT_RE.search(message)
    if not match:
        return None
    g = match.groupdict()
    try:
        return {
            'direction': 'credit',
            'type': g['type'].strip().upper(),
            'date': g['date'].strip(),
            'time': g['time'].strip(),
            'card': f"***{g['card']}",
            'amount': float(_to_decimal(g['amount'])),
            'currency': g['currency'].strip(),
            'balance': float(_to_decimal(g['balance'])),
        }
    except (InvalidOperation, TypeError) as exc:
        logging.error("Ошибка конвертации суммы/баланса: %s. Текст: %s", exc, message)
        return None

# --- Основная обработка ---

def process_sms_from_cache(source_dir: str, purchase_dest_dir: str, credit_dest_dir: str):
    """Обрабатывает все сообщения из SOURCE_CACHE_DIR и раскладывает по двум кэшам."""
    with Cache(source_dir) as source_cache, Cache(purchase_dest_dir) as purchase_cache, Cache(credit_dest_dir) as credit_cache:
        logging.info("Начало обработки сообщений из кэша: %s", source_dir)

        stats = {
            'processed_debit': 0,
            'processed_credit': 0,
            'failed': 0,
            'skipped': 0,
        }

        for key in list(source_cache):
            message_data = source_cache.get(key)
            if message_data.get('status') == 'processed':
                stats['skipped'] += 1
                continue

            body_text = message_data.get('body', '')

            # Сначала пытаемся как покупку/списание
            parsed = parse_transaction_message(body_text)
            target_cache = purchase_cache
            if parsed:
                stats['processed_debit'] += 1
            else:
                # Если не получилось, пробуем как кредит
                parsed = parse_credit_message(body_text)
                target_cache = credit_cache
                if parsed:
                    stats['processed_credit'] += 1

            if parsed:
                body_hash = hashlib.sha256(body_text.encode('utf-8')).hexdigest()
                parsed['original_key'] = key
                parsed['original_body'] = body_text
                target_cache.set(body_hash, parsed)
                message_data['status'] = 'processed'
            else:
                message_data['status'] = 'failed_to_parse'
                stats['failed'] += 1

            source_cache.set(key, message_data)

        logging.info(
            "Обработка завершена. Покупки: %d, Зачисления: %d, Ошибки: %d, Пропущено: %d",
            stats['processed_debit'], stats['processed_credit'], stats['failed'], stats['skipped']
        )

# --- Вспомогательная проверка ---

def verify_processed_cache(cache_dir: str, label: str):
    logging.info("\n--- Проверка кэша %s ---", label)
    with Cache(cache_dir) as cache:
        keys = list(cache)
        if not keys:
            logging.warning("Кэш '%s' пуст.", label)
            return
        logging.info("Всего записей в '%s': %d", label, len(keys))
        for key in keys[:3]:  # показываем до 3 записей
            logging.info("Запись %s: %s", key[:15] + '...', cache.get(key))


if __name__ == '__main__':
    process_sms_from_cache(SOURCE_CACHE_DIR, PURCHASE_CACHE_DIR, CREDIT_CACHE_DIR)

    # Проверка содержимого кэшей (необязательно)
    verify_processed_cache(PURCHASE_CACHE_DIR, 'покупки/списания')
    verify_processed_cache(CREDIT_CACHE_DIR, 'зачисления')

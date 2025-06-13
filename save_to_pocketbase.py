""" 
Скрипт синхронизации локальных кэшей DiskCache с PocketBase. 

 * parsed_sms_cache  -> коллекция `sms_data`     (покупки/списания)
 * credit_sms_cache  -> коллекция `transactions` (зачисления)

Перед использованием задайте переменные окружения:
    PB_URL, PB_EMAIL, PB_PASSWORD
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from diskcache import Cache
from pocketbase import PocketBase

# --- Настройка логирования ---------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация PocketBase -------------------------------------------------
PB_URL      = os.environ.get("PB_URL", "http://127.0.0.1:8090")
PB_EMAIL    = os.environ.get("PB_EMAIL", None)
PB_PASSWORD = os.environ.get("PB_PASSWORD", None)

# --- Локальные кэши ----------------------------------------------------------
PURCHASE_CACHE_DIR = "parsed_sms_cache"   # дебет (покупки/списания)
CREDIT_CACHE_DIR   = "credit_sms_cache"   # кредит (зачисления)

# --- Карта кэш -> коллекция + трансформер ------------------------------------
# Функция-трансформер получает запись из кэша и возвращает payload для PB

def _dt_str(date: str, time_: str) -> Optional[str]:
    """Преобразует d.m.Y + HH:MM в формат YYYY-MM-DD HH:MM:SS"""
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%y", "%d/%m/%y", "%d-%m-%y"):
        try:
            dt = datetime.strptime(f"{date} {time_}", f"{fmt} %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    logging.warning("Не удалось распарсить дату-время %s %s", date, time_)
    return None


def _build_sms_data(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Подготовка payload для коллекции sms_data."""
    pb_dt = _dt_str(record.get("date", ""), record.get("time", ""))
    if not pb_dt:
        return None
    return {
        "merchant":      record.get("merchant"),
        "city":          record.get("city"),
        "address":       record.get("address"),
        "datetime":      pb_dt,
        "card":          record.get("card"),
        "amount":        str(record.get("amount", 0.0)),
        "currency":      record.get("currency"),
        "balance":       str(record.get("balance", 0.0)),
        "original_key":  record.get("original_key"),
        "original_body": record.get("original_body"),
    }


def _build_transactions(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Payload для коллекции transactions (схема предоставлена пользователем)."""
    pb_dt = _dt_str(record.get("date", ""), record.get("time", ""))
    if not pb_dt:
        return None
    return {
        "transaction_id":   record.get("original_key"),
        "transaction_type": record.get("type", record.get("direction")),
        "amount":           record.get("amount"),
        "currency":         record.get("currency"),
        "balance_after":    record.get("balance"),
        "timestamp":        pb_dt,
        "status":           "parsed",
    }

SYNC_MAP = {
    PURCHASE_CACHE_DIR: {
        "collection": "sms_data",
        "builder":    _build_sms_data,
    },
    CREDIT_CACHE_DIR: {
        "collection": "transactions",
        "builder":    _build_transactions,
    },
}

# -----------------------------------------------------------------------------

def _login(client: PocketBase):
    """Login as admin (if credentials provided)."""
    if PB_EMAIL and PB_PASSWORD:
        try:
            client.admins.auth_with_password(PB_EMAIL, PB_PASSWORD)
            logging.info("Успешная аутентификация администратора PB.")
        except Exception as e:
            logging.warning("Не удалось войти админом: %s. Продолжаем без авторизации.", e)


def sync_cache(cache_dir: str, config: Dict[str, Any], client: PocketBase):
    """Синхронизирует один кэш с одной коллекцией."""
    collection = client.collection(config["collection"])
    build      = config["builder"]

    synced = skipped = errors = 0

    with Cache(cache_dir) as cache:
        keys = list(cache)
        logging.info("Кэш %s: найдено %d записей.", cache_dir, len(keys))
        for key in keys:
            rec = cache.get(key)
            # пропуск уже синхронизированных
            if rec.get("status") == "synced":
                skipped += 1
                continue

            original_key = rec.get("original_key")
            if not original_key:
                logging.warning("Отсутствует original_key для %s", key)
                errors += 1
                continue

            # Дедупликация в PB
            try:
                dup = collection.get_list(query_params={"filter": f'original_key="{original_key}"'}) if "sms_data" == config["collection"] else collection.get_list(query_params={"filter": f'transaction_id="{original_key}"'})
                if dup.total_items:
                    rec["status"] = "synced"
                    cache.set(key, rec)
                    skipped += 1
                    continue
            except Exception as e:
                logging.error("Ошибка запроса к PocketBase: %s", e)
                errors += 1
                continue

            payload = build(rec)
            if not payload:
                errors += 1
                continue

            # Сохраняем
            try:
                collection.create(payload)
                rec["status"] = "synced"
                cache.set(key, rec)
                synced += 1
            except Exception as e:
                logging.error("Ошибка создания записи в PB: %s", e)
                errors += 1

    logging.info("%s => %s | синхр: %d, пропущено: %d, ошибки: %d", cache_dir, config["collection"], synced, skipped, errors)

# -----------------------------------------------------------------------------

if __name__ == "__main__":
    client = PocketBase(PB_URL)
    _login(client)

    for cdir, cfg in SYNC_MAP.items():
        sync_cache(cdir, cfg, client)

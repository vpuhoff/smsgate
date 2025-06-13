import os
import logging
from datetime import datetime
from diskcache import Cache
from pocketbase import PocketBase

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация Pocketbase ---
# ВАЖНО: Для реального проекта используйте переменные окружения, а не храните пароли в коде!
POCKETBASE_URL = os.environ.get("PB_URL", "http://127.0.0.1:8090") # URL вашего Pocketbase
POCKETBASE_EMAIL = os.environ.get("PB_EMAIL") # Email администратора
POCKETBASE_PASSWORD = os.environ.get("PB_PASSWORD") # Пароль администратора

# --- Константы ---
PROCESSED_CACHE_DIR = 'parsed_sms_cache'
POCKETBASE_COLLECTION = 'sms_data' # Название вашей коллекции

def format_datetime_for_pb(date_str: str, time_str: str) -> str:
    """
    Преобразует отдельные строку даты и времени в стандартный формат Pocketbase.
    Пример: "14.02.2024", "12:47" -> "2024-02-14 12:47:00"
    """
    try:
        # Пытаемся распознать разные разделители в дате
        dt_obj = datetime.strptime(f"{date_str.replace('.24', '.2024').replace('.25', '.2025')} {time_str}", "%d.%m.%Y %H:%M")
    except ValueError:
        try:
            dt_obj = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        except ValueError:
            logging.warning(f"Не удалось распознать формат даты-времени: {date_str} {time_str}. Пропускаем.")
            return ""

    # Форматируем в строку, которую понимает Pocketbase
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S")

def sync_cache_to_pocketbase(cache_dir: str):
    """
    Синхронизирует обработанные данные из кэша в Pocketbase.
    """
    logging.info("--- Начало синхронизации с Pocketbase ---")

    try:
        # 1. Подключение к Pocketbase
        client = PocketBase(POCKETBASE_URL)
        # Аутентификация администратора для получения прав на запись
        #client.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info(f"Успешная аутентификация в Pocketbase на {POCKETBASE_URL}")
    except Exception as e:
        logging.error(f"Не удалось подключиться к Pocketbase: {e}")
        return

    synced_count = 0
    skipped_count = 0
    error_count = 0

    # 2. Открытие кэша для чтения и записи
    with Cache(cache_dir) as cache:
        keys_to_process = list(cache)
        logging.info(f"Найдено {len(keys_to_process)} записей в локальном кэше.")

        for key in keys_to_process:
            record = cache.get(key)

            # 3. Пропускаем уже синхронизированные записи
            if record.get('status') == 'synced':
                skipped_count += 1
                continue

            # Уникальный идентификатор для проверки на дубликаты
            original_key = record.get('original_key')
            if not original_key:
                logging.warning(f"У записи с ключом {key} отсутствует original_key. Пропускаем.")
                error_count += 1
                continue

            try:
                # 4. Проверка на существование записи в Pocketbase
                existing_records = client.collection(POCKETBASE_COLLECTION).get_list(
                    query_params={"filter": f'original_key="{original_key}"'}
                )
                if existing_records.total_items > 0:
                    logging.info(f"Запись с original_key={original_key} уже существует. Помечаем как синхронизированную.")
                    record['status'] = 'synced'
                    cache.set(key, record)
                    skipped_count += 1
                    continue

                # 5. Подготовка данных для Pocketbase
                pb_datetime = format_datetime_for_pb(record.get('date', ''), record.get('time', ''))
                if not pb_datetime:
                    error_count += 1
                    continue

                payload = {
                    "merchant": record.get("merchant"),
                    "city": record.get("city"),
                    "address": record.get("address"),
                    "datetime": pb_datetime,
                    "card": record.get("card"),
                    # Поля amount и balance в вашей схеме - текстовые
                    "amount": str(record.get("amount", 0.0)),
                    "currency": record.get("currency"),
                    "balance": str(record.get("balance", 0.0)),
                    "original_key": original_key,
                    "original_body": record.get("original_body")
                }

                # 6. Создание новой записи
                client.collection(POCKETBASE_COLLECTION).create(payload)
                logging.info(f"Успешно создана запись для original_key={original_key}")

                # 7. Обновление статуса в кэше
                record['status'] = 'synced'
                cache.set(key, record)
                synced_count += 1

            except Exception as e:
                logging.error(f"Непредвиденная ошибка при обработке original_key={original_key}: {e}")
                error_count += 1

    logging.info("--- Синхронизация завершена ---")
    logging.info(f"Новых записей синхронизировано: {synced_count}")
    logging.info(f"Пропущено (уже существуют или обработаны): {skipped_count}")
    logging.info(f"Записей с ошибками: {error_count}")


if __name__ == '__main__':
    sync_cache_to_pocketbase(PROCESSED_CACHE_DIR)
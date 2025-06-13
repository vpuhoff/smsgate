import re
import hashlib
import logging
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any
from diskcache import Cache

# --- Настройка логирования ---
# Простое логирование в консоль для отладки
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Константы ---
SOURCE_CACHE_DIR = 'sms_cache'
PROCESSED_CACHE_DIR = 'parsed_sms_cache'

# --- Регулярное выражение и функция парсинга (предоставлены вами) ---
# Немного модифицируем re для большей гибкости с пробелами и валютами
TRANSACTION_RE = re.compile(
    r"""
    ^.*?  # Не захватываем мусор в начале, если он есть
    (?:PURCHASE|SALE):\s*
    (?P<merchant>.+?),\s*YEREVAN,\s*
    (?P<address>.*?)(?=,\s*\d{2}[./-]\d{2}[./-]\d{2,4}\s)  # Адрес до даты
    ,\s*(?P<date>\d{2}[./-]\d{2}[./-]\d{2,4})\s+
    (?P<time>\d{2}:\d{2}),\s*
    card\ \*{3}(?P<card>\d{4})\.\s*
    Amount:\s*(?P<amount>[\d,.]+)\s+
    (?P<currency>\w{3}),\s*
    Balance:\s*(?P<balance>[\d,.]+)
    """,
    re.VERBOSE | re.IGNORECASE | re.DOTALL,
)

def parse_transaction_message(message: str) -> Optional[Dict[str, Any]]:
    """
    Разбирает SMS-уведомление от банка в структурированные данные.
    
    Возвращает None, если сообщение не соответствует шаблону.
    В случае ошибок парсинга чисел возвращает None.
    """
    if not message:
        return None
        
    match = TRANSACTION_RE.search(message)
    if not match:
        return None
        
    g = match.groupdict()
    try:
        # Убираем запятые-разделители тысяч для корректной конвертации
        amount = Decimal(g["amount"].replace(",", ""))
        balance = Decimal(g["balance"].replace(",", ""))
        
        return {
            "merchant": g["merchant"].strip(),
            "city": "YEREVAN",
            "address": g["address"].strip() or "N/A",
            "date": g["date"].strip(),
            "time": g["time"].strip(),
            "card": f"***{g['card']}",
            "amount": float(amount),
            "currency": g["currency"].strip(),
            "balance": float(balance),
        }
    except (InvalidOperation, TypeError) as exc:
        logging.error(f"Не удалось преобразовать число в сообщении. Ошибка: {exc}. Текст: {message}")
        return None

def process_sms_from_cache(source_dir: str, dest_dir: str):
    """
    Обрабатывает сообщения из исходного кэша и сохраняет результаты в целевой кэш.
    """
    # Используем одну транзакцию для source_cache для атомарного обновления
    with Cache(source_dir) as source_cache, Cache(dest_dir) as dest_cache:
        logging.info(f"Начало обработки сообщений из кэша: {source_dir}")
        
        processed_count = 0
        failed_count = 0
        skipped_count = 0

        # Итерируемся по всем ключам в исходном кэше
        # Преобразуем в list, чтобы избежать проблем с изменением кэша во время итерации
        for key in list(source_cache):
            message_data = source_cache.get(key)
            
            # 1. Проверяем, было ли сообщение обработано ранее
            if message_data.get('status') == 'processed' :
                skipped_count += 1
                continue
            
            body_text = message_data.get('body')
            
            # 2. Применяем парсинг
            parsed_data = parse_transaction_message(body_text)
            
            # 3. Если парсинг успешен
            if parsed_data:
                # 4. Создаем ключ-хэш и сохраняем в новый кэш
                body_hash = hashlib.sha256(body_text.encode('utf-8')).hexdigest()
                
                # Добавляем в распарсенные данные оригинальный ключ для связности
                parsed_data['original_key'] = key
                parsed_data['original_body'] = body_text

                dest_cache.set(body_hash, parsed_data)
                
                # 5. Обновляем статус в исходном кэше
                message_data['status'] = 'processed'
                source_cache.set(key, message_data)
                processed_count += 1
            else:
                # 6. Если парсинг неуспешен, обновляем статус
                message_data['status'] = 'failed_to_parse'
                source_cache.set(key, message_data)
                failed_count += 1
        
        logging.info("Обработка завершена.")
        logging.info(f"Успешно обработано: {processed_count}")
        logging.info(f"Не удалось распознать: {failed_count}")
        logging.info(f"Пропущено ранее обработанных: {skipped_count}")

def verify_processed_cache(cache_dir: str):
    """
    Проверяет и выводит несколько записей из кэша обработанных сообщений.
    """
    logging.info("\n--- Проверка кэша обработанных сообщений ---")
    with Cache(cache_dir) as cache:
        # **ИСПРАВЛЕНИЕ ЗДЕСЬ:** Итерируемся по ключам и получаем значение для каждого.
        keys = list(cache)
        if not keys:
            logging.warning("Кэш обработанных сообщений пуст.")
            return

        logging.info(f"Всего записей в кэше: {len(keys)}")
        count = 0
        for key in keys:
            value = cache.get(key) # Получаем значение по ключу
            logging.info(f"Найдена запись с ключом (хэш): {key[:15]}...")
            logging.info(f"  Данные: {value}")
            count += 1
            if count >= 3: # Показываем не более 3 записей для примера
                break


if __name__ == '__main__':
    process_sms_from_cache(SOURCE_CACHE_DIR, PROCESSED_CACHE_DIR)
    
    # --- Блок для верификации (необязательно) ---
    verify_processed_cache(PROCESSED_CACHE_DIR)
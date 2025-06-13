import xml.etree.ElementTree as ET
from diskcache import Cache

# Укажите путь к вашему XML-файлу
XML_FILE_PATH = 'sms-20250613021329.xml'
# Укажите директорию для хранения кэша
CACHE_DIRECTORY = 'sms_cache'

def parse_and_cache_sms(xml_file: str, cache_dir: str):
    """
    Считывает SMS-сообщения из XML-файла, разбирает их и сохраняет в DiskCache.

    Args:
        xml_file (str): Путь к XML-файлу с SMS.
        cache_dir (str): Путь к директории для хранения кэша.
    """
    # Инициализация кэша
    # FanoutCache используется для лучшей производительности при большом количестве файлов
    cache = Cache(cache_dir, fanout_cache=True)
    
    print(f"Начало обработки файла: {xml_file}")
    
    try:
        # Разбор XML-файла
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Счетчик обработанных сообщений
        processed_count = 0
        
        # Итерация по всем элементам <sms>
        for sms_element in root.findall('sms'):
            # Атрибуты элемента уже представляют собой словарь
            message_data = sms_element.attrib
            
            # Получаем ключ для кэша из поля 'date'
            cache_key = message_data.get('date')
            
            if cache_key:
                # Сохраняем словарь с данными сообщения в кэш
                cache.set(cache_key, message_data)
                processed_count += 1
            else:
                print(f"Предупреждение: у сообщения отсутствует атрибут 'date'. Сообщение пропущено: {message_data}")

        print(f"Обработка завершена. Всего сообщений сохранено в кэш: {processed_count}")

    except FileNotFoundError:
        print(f"Ошибка: XML-файл не найден по пути: {xml_file}")
    except ET.ParseError:
        print(f"Ошибка: не удалось разобрать XML-файл. Убедитесь, что он имеет корректную структуру.")
    finally:
        # Закрываем соединение с кэшем
        cache.close()

def verify_cache(cache_dir: str, sample_key: str):
    """
    Проверяет наличие и выводит одно сообщение из кэша по ключу.
    """
    if not sample_key:
        print("\nНе удалось найти ключ для проверки.")
        return
        
    cache = Cache(cache_dir)
    print("\n--- Проверка кэша ---")
    
    # Получаем сообщение из кэша по ключу
    cached_message = cache.get(sample_key)
    
    if cached_message:
        print(f"Сообщение с ключом '{sample_key}' успешно найдено в кэше:")
        for key, value in cached_message.items():
            print(f"  {key}: {value}")
    else:
        print(f"Сообщение с ключом '{sample_key}' не найдено в кэше.")
    
    cache.close()


if __name__ == '__main__':
    # Запускаем основную функцию
    parse_and_cache_sms(XML_FILE_PATH, CACHE_DIRECTORY)
    
    # --- Блок для верификации (необязательно) ---
    # Чтобы проверить, что данные сохранились, можно взять ключ 'date'
    # из примера в вашем вопросе и проверить наличие записи в кэше.
    # Ключ из примера: '1707900464673'
    VERIFICATION_KEY = '1707900464673' 
    verify_cache(CACHE_DIRECTORY, VERIFICATION_KEY)
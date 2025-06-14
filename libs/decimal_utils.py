from decimal import Decimal, InvalidOperation
import re

def parse_ambiguous_decimal(num_str: str) -> Decimal:
    """
    Пытается преобразовать строку с неизвестным форматом числа в Decimal.
    - Обрабатывает пробелы, запятые и точки как возможные разделители.
    - Использует эвристику для определения десятичного разделителя.
    """
    if not isinstance(num_str, str):
        # Если это уже число, просто преобразуем
        return Decimal(num_str)

    # 1. Предварительная очистка: убираем пробелы по краям и внутри
    cleaned_str = num_str.strip().replace(' ', '')
    if not cleaned_str:
        raise ValueError("Input string cannot be empty")

    # 2. Находим позиции последнего вхождения точки и запятой
    last_dot_pos = cleaned_str.rfind('.')
    last_comma_pos = cleaned_str.rfind(',')

    # 3. Применяем эвристику
    # Если и точка, и запятая присутствуют, последний символ скорее всего десятичный разделитель
    if last_dot_pos != -1 and last_comma_pos != -1:
        if last_comma_pos > last_dot_pos:
            # Формат типа "1.234,56" (европейский)
            # Точки - разделители тысяч, запятая - десятичный
            final_str = cleaned_str.replace('.', '').replace(',', '.')
        else:
            # Формат типа "1,234.56" (американский)
            # Запятые - разделители тысяч, точка - десятичный
            final_str = cleaned_str.replace(',', '')
    # Если присутствует только запятая
    elif last_comma_pos != -1:
        # Может быть "1,234,567" или "1,23".
        # Если запятых несколько, они точно разделители тысяч.
        # Если одна, скорее всего, это десятичный разделитель.
        if cleaned_str.count(',') > 1:
            # "1,234,567" -> "1234567"
            final_str = cleaned_str.replace(',', '')
        else:
            # "1,23" -> "1.23"
            final_str = cleaned_str.replace(',', '.')
    # Если присутствует только точка
    elif last_dot_pos != -1:
        # Может быть "1.234.567" или "1.23".
        # Если точек несколько, все, кроме последней, - разделители тысяч.
        if cleaned_str.count('.') > 1:
            # "1.234.567" -> "1234567"
            parts = cleaned_str.split('.')
            final_str = "".join(parts[:-1]) + "." + parts[-1]
        else:
            # "1.23" -> "1.23"
            final_str = cleaned_str
    # Если разделителей нет
    else:
        final_str = cleaned_str
    final_str = re.sub(r'[^0-9.-]', '', final_str)
    try:
        return Decimal(final_str)
    except InvalidOperation:
        raise ValueError(f"Не удалось преобразовать строку '{num_str}' в число после очистки до '{final_str}'")


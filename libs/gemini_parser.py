# libs/gemini_parser.py
"""
LLM-парсер банковских SMS на основе Google Gemini.
Точка входа: parse_sms_llm(raw: RawSMS) -> ParsedSMS | None
"""
from __future__ import annotations
from datetime import datetime
from dateutil.parser import parse
import os, re, json, hashlib
import diskcache
from google import genai
from google.genai import types
from libs.models   import RawSMS, ParsedSMS
from libs.llm_core import ParsedSmsCore          # Pydantic-ядро
from libs.sentry   import sentry_capture         # опционально
from libs.decimal_utils import parse_ambiguous_decimal

# ────────────────────────────────
# 1. Инициализация клиента Gemini
# ────────────────────────────────
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-preview-05-20")
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

# ────────────────────────────────
# 2. Общие инструменты
# ────────────────────────────────
cache = diskcache.Cache(".gemini_cache")
print(f"Хэш gemini загружен: {len(cache)}")
_JSON_RE = re.compile(r"\{.*\}", re.S)           # «первый» JSON в тексте

SYSTEM_INSTRUCTION = (
    "Ты — банковский парсер. Верни ТОЛЬКО JSON "
    f"со строго следующими ключами: {', '.join(ParsedSmsCore.model_fields)}. "
    "Без Markdown-обёрток и лишнего текста."
    "txn_type может иметь значения 'debit', 'credit', 'otp' или 'unknown'"
    "Дата в сообщении обычно в формате день.месяц.год часы:минуты"
)

# Ручная схема Gemini (чисто строковые типы; кортежи/enum проверит Pydantic)
SCHEMA_PROPERTIES = {
    "txn_type":  types.Schema(type=types.Type.STRING),
    "date":      types.Schema(type=types.Type.STRING),
    "amount":    types.Schema(type=types.Type.STRING),
    "currency":  types.Schema(type=types.Type.STRING),
    "card":      types.Schema(type=types.Type.STRING),
    "merchant":  types.Schema(type=types.Type.STRING),
    "city":      types.Schema(type=types.Type.STRING),
    "address":   types.Schema(type=types.Type.STRING),
    "balance":   types.Schema(type=types.Type.STRING),
}
RESPONSE_SCHEMA = types.Schema(
    type=types.Type.OBJECT,
    properties=SCHEMA_PROPERTIES,
    required=["txn_type", "date"],
)

def _extract_json(chunk_text: str) -> str | None:
    m = _JSON_RE.search(chunk_text)
    return json.loads(m.group(0)) if m else None

def fix_broken_datetime(message, current_date):
    """
    Ищет в сообщении дату в форматах 'dd.mm.yy' или 'dd.mm.yyyy' и,
    если найдена, обновляет ее временем из current_date.
    """
    # Список пар (шаблон регулярного выражения, формат для strptime)
    date_patterns = [
        (r"\d{2}\.\d{2}\.\d{4}", "%d.%m.%Y"), # Сначала ищем полный год
        (r"\d{2}\.\d{2}\.\d{2}", "%d.%m.%y")
    ]

    for pattern, date_format in date_patterns:
        match = re.search(pattern, message)
        if not match:
            continue

        date_string = match.group(0)
        try:
            # Парсим найденную дату
            date_object = datetime.strptime(date_string, date_format)
            
            # Копируем время (hour, minute, second, microsecond) из current_date
            # .combine() более явно показывает намерение
            updated_date = datetime.combine(date_object.date(), current_date.time())

            if updated_date != current_date:
                print(f"Найдена и исправлена дата: {updated_date.strftime('%d.%m.%Y')}")
                return updated_date
            
            # Если дата совпала, нет смысла искать дальше
            return current_date

        except ValueError:
            # На случай, если регулярное выражение что-то нашло, но это невалидная дата
            continue
    
    print("Дата в тексте не найдена.")
    return current_date

def parse_custom_datetime(date_string):
    """
    Парсит строку формата 'дд.мм.гг чч:мм' в объект datetime.

    Args:
        date_string: Строка с датой и временем.

    Returns:
        Объект datetime.
    """
    try:
        return datetime.strptime(date_string, '%d.%m.%y %H:%M')
    except Exception:
        return parse(date_string)

def mask_card_number_with_prefix(text: str) -> str:
    """
    Находит в строке номер карты вида 'XXXX***XXXX' и заменяет его
    на строку "CARD:" и последние 4 цифры этого номера.

    Args:
        text: Входная строка для поиска.

    Returns:
        Строка с замаскированным и помеченным номером карты.
    """
    # Шаблон остался тем же: ищем 4 цифры, 3 звездочки и захватываем последние 4 цифры.
    pattern = r'\d{4}\*{3}(\d{4})'

    # В строке замены мы добавляем 'CARD:' перед ссылкой на захваченную группу \1.
    # r'CARD:\1' - означает "заменить на строку 'CARD:' плюс то, что было в скобках".
    return re.sub(pattern, r'CARD:\1', text)

# ────────────────────────────────
# 3. Основная функция
# ────────────────────────────────
def parse_sms_llm(raw: RawSMS) -> ParsedSMS | None:
    """
    Отправляет текст SMS в Gemini и пытается вернуть ParsedSMS.
    В случае любой ошибки → None (worker переложит в sms_failed + Sentry).
    """
    if 'OTP' in raw.body or 'CODE:' in raw.body:
        return None
    # Исправляем длинные номера карт
    fixed_body = mask_card_number_with_prefix(raw.body)

    resp_data: dict = None # type: ignore

    cache_key = hashlib.sha256(fixed_body.encode()).hexdigest()
    if cache_key in cache:
        resp_data = cache[cache_key] # type: ignore
    else:
        # Собираем «чат»
        contents = [
            types.Content(role="user", parts=[types.Part.from_text(text=fixed_body)])
        ]
        config = types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,
            system_instruction=[types.Part.from_text(text=SYSTEM_INSTRUCTION)],
        )
        resp_data = call_gemini(contents, config)
        cache[cache_key] = resp_data

    try:
        resp_data['date'] = parse_custom_datetime(resp_data['date'])
        resp_data['date'] = fix_broken_datetime(raw.body, resp_data['date'])

        resp_data['card'] = resp_data['card'].replace("*", '')
        if len(resp_data['card']) > 4:
            resp_data['card'] = resp_data['card'][:4]
        resp_data['amount'] = parse_ambiguous_decimal(str(resp_data['amount']))
        resp_data['balance'] = parse_ambiguous_decimal(str(resp_data['balance']))
        core = ParsedSmsCore.model_validate(resp_data)
    except Exception as exc:
        if resp_data['txn_type'] != 'otp':
            sentry_capture(exc)      # схему нарушили
        return None
    
    if core.address == "null":
        core.address = ""

    parsed = ParsedSMS(
        msg_id   = raw.msg_id,
        device_id= raw.device_id,
        sender   = raw.sender,
        date     = core.date,
        raw_body = raw.body,

        txn_type = core.txn_type,
        amount   = core.amount,
        currency = core.currency,
        card     = core.card,

        merchant = core.merchant,
        city     = core.city,
        address  = core.address,

        balance  = core.balance,

        parser_version = "llm-0.2.0",
    )

    
    return parsed

def call_gemini(contents, config) -> dict:
    try:
        stream = client.models.generate_content_stream(
            model=GEMINI_MODEL,
            contents=contents,
            config=config,
        )
        # Потоковый ответ может прийти кусками → склеиваем
        raw_answer = "".join(chunk.text for chunk in stream if chunk.text)
        resp_data: dict = {}
        try:
            resp_data = _extract_json(raw_answer) # type: ignore
            if resp_data is None:
                raise ValueError("Gemini вернул не-JSON")
        except Exception as e:
            raise ValueError("Gemini вернул JSON с ошибкой")
    except Exception as exc:
        sentry_capture(exc)
        raise exc
    return resp_data

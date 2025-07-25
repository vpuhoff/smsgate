# pb_writer/upsert.py
from sqlalchemy.dialects.postgresql import insert
from db.models import SmsData
from db.session import SessionLocal
from libs.sentry import sentry_capture

async def upsert_parsed_sms(parsed: dict):
    """
    Выполняет "upsert" (INSERT или UPDATE) для данных SMS.
    Если запись с таким `msg_id` уже существует, она обновляется.
    В противном случае создается новая запись.
    """
    try:
        async with SessionLocal() as sess:
            # Шаг 1: Создаем базовый insert-оператор
            fixed = parsed.copy()
            fixed["datetime"] = fixed.pop("date", None)
            fixed["original_body"] = fixed.pop("raw_body", None)
            insert_stmt = insert(SmsData).values(**fixed)

            # Шаг 2: Создаем финальный оператор, добавляя ON CONFLICT.
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["msg_id"],
                set_={
                    c.name: c
                    for c in insert_stmt.excluded 
                    if c.name not in ("id", "msg_id")
                }
            )
            await sess.execute(upsert_stmt)
            await sess.commit() # Не забывайте коммитить транзакцию
    except Exception as e:
        sentry_capture(e)
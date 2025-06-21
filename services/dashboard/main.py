#!/usr/bin/env python3
"""PocketBase → Telegram hourly notifier + basic access control + last balance

Что делает:
1. Каждые `CHECK_INTERVAL_SECONDS` (по умолчанию — час) забирает *новые* записи из PocketBase
   коллекции `sms_data`, строит bar-chart (HTML + PNG) **и присылает**
   последнюю известную сумму *баланса карты*.
2. **Параллельно** слушает входящие сообщения (`getUpdates`). Если бот получает
   личное сообщение/упоминание из чата, чей `chat_id` **отсутствует** в списке
   `TG_CHAT_IDS`, он отвечает текстом:
   ``⛔️ У вас нет доступа к этому боту. Ваш chat_id: <id>``.

Дополнительно: в caption к PNG добавляется строка вида
```
Последний баланс: 184 260.74 AMD
```  (если поле `balance` удалось разобрать).

Зависимости (pip install):
    httpx pandas plotly kaleido python-dateutil

Переменные окружения (или config.toml):
    PB_URL, PB_EMAIL, PB_PASSWORD          – данные PocketBase
    TG_BOT_TOKEN                           – токен Telegram-бота
    TG_CHAT_IDS                            – допустимые chat_id через запятую
    CHECK_INTERVAL_SECONDS                 – опционально, период (сек), default = 3600

Файл состояния (`last_state.json`) хранит метку времени `last_ts`
последней обработанной записи **и** `offset` Telegram-обновлений.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import mimetypes
import os
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Optional, Set, Tuple

import httpx
import pandas as pd
import plotly.express as px
from dateutil import parser as dt_parse

from libs.config import get_settings
from libs.pocketbase import AsyncPocketBaseClient, PocketBaseClient

# ---------- Логирование ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- Константы / конфиг ----------
settings = get_settings()
PB_URL = settings.pb_url
PB_EMAIL = settings.pb_email
PB_PASSWORD = settings.pb_password
TG_BOT_TOKEN: str = settings.tg_bot_token
TG_CHAT_IDS_RAW: str = settings.tg_chat_ids

ALLOWED_CHAT_IDS: Set[int] = {
    int(cid.strip()) for cid in TG_CHAT_IDS_RAW.split(",") if cid.strip()
}

COLLECTION_NAME = "sms_data"
STATE_PATH = pathlib.Path("last_state.json")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SECONDS", "3600"))

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"

# ---------- Telegram helpers ----------
async def tg_request(method: str, **kwargs) -> httpx.Response:
    async with httpx.AsyncClient(
        base_url=TELEGRAM_API_BASE, timeout=60.0
    ) as client:
        resp = await client.post(f"/{method}", **kwargs)
        resp.raise_for_status()
        return resp


async def tg_send_document(path: pathlib.Path, caption: str | None = None) -> None:
    for chat_id in ALLOWED_CHAT_IDS:
        logger.info(f"Telegram: отправка документа {path.name} → {chat_id}")
        with open(path, "rb") as f:
            files = {"document": (path.name, f.read())}
            data = {"chat_id": chat_id, "caption": caption or ""}
            try:
                await tg_request("sendDocument", data=data, files=files)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    f"TG: ошибка отправки документа {exc.response.status_code}: {exc.response.text}"
                )
            else:
                logger.info("Telegram: OK")


async def tg_send_photo(path: pathlib.Path, caption: str | None = None) -> None:
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type is None:
        mime_type = "application/octet-stream"

    for chat_id in ALLOWED_CHAT_IDS:
        logger.info(
            f"Telegram: отправка фото {path.name} → {chat_id} (тип: {mime_type})"
        )
        with open(path, "rb") as f:
            files = {"photo": (path.name, f.read(), mime_type)}
            data = {"chat_id": chat_id, "caption": caption or ""}
            try:
                await tg_request("sendPhoto", data=data, files=files)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    f"TG: ошибка отправки фото {exc.response.status_code}: {exc.response.text}"
                )
            else:
                logger.info("Telegram: OK")


# ---------- Состояние (last_ts и offset) ----------
def load_state() -> dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception as e:
            raise Exception(f"STATE: файл состояния поврежден, сброс ({e})")
    # Значение по умолчанию
    return {
        "last_ts": (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
        "offset": 0,
    }


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2))


STATE = load_state()


# ---------- Обработка данных и график ----------
def build_chart(
    records: List[Mapping[str, Any]], title: str
) -> Tuple[pathlib.Path, pathlib.Path, Optional[Tuple[float, str]]]:
    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("DataFrame пуст, график не построить.")

    df["merchant"] = (
        df["merchant"].fillna("Unknown").replace({"": "Unknown", "null": "Unknown"})
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)

    if "balance" in df.columns:
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce")
    else:
        df["balance"] = pd.NA

    df.dropna(subset=["amount", "datetime"], inplace=True)
    df["date"] = df["datetime"].dt.date

    daily = (
        df.groupby(["date", "merchant"])["amount"]
        .sum()
        .reset_index()
        .sort_values("date")
    )

    fig = px.bar(
        daily,
        x="date",
        y="amount",
        color="merchant",
        labels={"date": "Дата", "amount": "Сумма", "merchant": "Продавец"},
        height=600,
    )
    fig.update_layout(title_text=title, xaxis_tickangle=-45)

    html_path = pathlib.Path("payments_by_day.html")
    png_path = pathlib.Path("payments_by_day.jpg")
    fig.write_html(html_path)
    fig.write_image(png_path, format="jpg", scale=2)

    last_balance: Optional[Tuple[float, str]] = None
    if "balance" in df.columns and not df["balance"].isna().all():
        latest_row = df.loc[df["datetime"].idxmax()]
        bal_value = latest_row["balance"]
        if pd.notna(bal_value):
            currency = latest_row.get("currency", "") or ""
            last_balance = float(bal_value), str(currency)

    return html_path, png_path, last_balance


# ---------- Основные циклы ----------
async def run_pb_cycle(pb: PocketBaseClient) -> None:
    # 1. От какой даты берём данные
    STATE = load_state()
    last_ts = dt_parse.isoparse(STATE["last_ts"])
    
    # 2. Формируем строку для фильтра в формате, который точно поймет PocketBase
    since_pb_str = (last_ts + timedelta(microseconds=1) - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # 3. Тянем новые записи
    records = await pb.get_records_since(COLLECTION_NAME, since_pb_str)
    if not records:
        logger.info("PB cycle: новых записей нет")
        return

    # 4. Проверяем last_ts
    valid_dts = [
        pd.to_datetime(r.get("datetime"), utc=True, errors="coerce") for r in records
    ]
    valid_dts = [dt for dt in valid_dts if pd.notna(dt)]

    if valid_dts:
        latest_dt = max(valid_dts)

        if latest_dt > last_ts:
            # 5. Строим график
            try:
                html_path, png_path, last_balance = build_chart(
                    records, title="Статистика платежей по дням"
                )
            except ValueError as e:
                logger.error(f"PB cycle: ошибка построения графика: {e}")
                return

            # 6. Формируем подпись к изображению
            caption = "Обновлённая статистика платежей"
            if last_balance:
                value, currency = last_balance
                caption += f"\nПоследний баланс: {value:,.2f} {currency}".replace(",", " ")

            # 7. Шлём файлы в Telegram
            await tg_send_photo(png_path, caption=caption)
            await tg_send_document(html_path)
        
            STATE["last_ts"] = latest_dt.isoformat()
            save_state(STATE)
            logger.info("PB cycle: STATE['last_ts'] обновлен → %s", STATE["last_ts"])
        else:
            logger.info("Новых записей нет, последняя запись: %s", STATE["last_ts"])
    else:
        logger.warning(
            "PB cycle: в новых записях не найдено корректных дат. Состояние не обновлено."
        )


async def listen_updates() -> None:
    offset = STATE.get("offset", 0)
    async with httpx.AsyncClient(base_url=TELEGRAM_API_BASE, timeout=60.0) as client:
        while True:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            try:
                resp = await client.get("/getUpdates", params=params)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                logger.warning(f"TG getUpdates error: {e}")
                await asyncio.sleep(5)
                continue

            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                STATE["offset"] = offset
                save_state(STATE)

                message = upd.get("message") or upd.get("edited_message")
                if not message:
                    continue
                chat_id = message["chat"]["id"]
                if chat_id not in ALLOWED_CHAT_IDS:
                    warn = f"⛔️ У вас нет доступа к этому боту. Ваш chat_id: {chat_id}"
                    logger.info(f"Unknown chat {chat_id} → sending deny message")
                    try:
                        await client.post("/sendMessage", data={"chat_id": chat_id, "text": warn})
                    except httpx.HTTPError as e:
                        logger.error(f"TG deny send error: {e}")
            # loop continues immediately (long-poll)


async def main() -> None:
    logger.info("🚀 notifier started. Allowed chats: %s", ", ".join(map(str, ALLOWED_CHAT_IDS)))

    async with AsyncPocketBaseClient(base_url=PB_URL, email=PB_EMAIL, password=PB_PASSWORD) as pb:
        # Запускаем слушатель Telegram
        tg_task = asyncio.create_task(listen_updates())

        try:
            while True:
                try:
                    await run_pb_cycle(pb)
                except Exception:
                    logger.exception("PB cycle error")
                logger.info("sleep %s sec …", CHECK_INTERVAL)
                await asyncio.sleep(CHECK_INTERVAL)
        finally:
            tg_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tg_task

if __name__ == "__main__":
    # Я убрал из примера циклы listen_updates и main, 
    # так как они не менялись, но в вашем файле они должны остаться.
    # Этот блок для демонстрации
    async def run_once():
        logger.info("🚀 Notifier started. Allowed chats: %s", ", ".join(map(str, ALLOWED_CHAT_IDS)))
        async with AsyncPocketBaseClient(base_url=PB_URL, email=PB_EMAIL, password=PB_PASSWORD) as pb: # type: ignore
            await run_pb_cycle(pb)

    try:
        # Для проверки можно запустить один раз
        # asyncio.run(run_once())
        # В рабочем режиме вы вернете ваш оригинальный main()
        asyncio.run(main()) 
    except KeyboardInterrupt:
        logger.info("Exiting on Ctrl-C")
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")


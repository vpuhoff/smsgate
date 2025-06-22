# services/api_gateway/main.py
"""FastAPI шлюз, принимающий *сырые* SMS-данные и публикующий их в Redis Stream.

* **POST  /sms/raw**  сообщения от мобильного устройства.
* **GET   /health**   простая проверка живости (ping Redis).

❗ DTO-модели вынесены в `services.api_gateway.schemas`, чтобы избежать
циклических импортов и централизовать OpenAPI-описание.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any, Dict

from fastapi.concurrency import asynccontextmanager
import ngrok
import uvicorn
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from libs.config import get_settings
from libs.models import RawSMS, get_md5_hash
from libs.nats_utils import publish_raw_sms, get_nats_connection
from libs.sentry import sentry_capture
from schemas import RawSMSPayload
from pathlib import Path

# ---------------------------------------------------------------------------#
# Graceful shutdown helpers (optional)                                       #
# ---------------------------------------------------------------------------#
shutdown_event = asyncio.Event()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API Gateway started")
    yield
    logger.info("API Gateway shutting down…")
    _stop_ngrok()
    shutdown_event.set()

logger = logging.getLogger(__name__)
app = FastAPI(title="SMS API Gateway", version="0.1.0", lifespan=lifespan)


# ──────────────────────────────────────────────────────────────────────────
# ✨  Файловое логирование
# ──────────────────────────────────────────────────────────────────────────
LOG_DIR = Path("/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(LOG_DIR / "api_gateway.log", encoding="utf-8")
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
logger.addHandler(_file_handler)

# ---------------------------------------------------------------------------#
# ngrok helpers                                                              #
# ---------------------------------------------------------------------------#
_NGROK_LISTENER: ngrok.Listener | None = None

def _start_ngrok(settings) -> None:  # pragma: no cover
    """Запускает ngrok-туннель поверх локального API Gateway."""
    global _NGROK_LISTENER

    authtoken = os.getenv("NGROK_AUTHTOKEN")
    if not authtoken:
        logger.info("NGROK_AUTHTOKEN не задан - туннель не поднимаю.")
        return

    # Формируем базовый конфиг
    ngrok_cfg: dict[str, Any] = {"authtoken": authtoken}

    # Необязательный кастомный домен
    domain = os.getenv("NGROK_DOMAIN")
    if domain:
        ngrok_cfg["domain"] = domain

    public_url = f"http://127.0.0.1:{settings.api_port}"
    try:
        _NGROK_LISTENER = ngrok.connect(public_url, **ngrok_cfg)   # type: ignore[arg-type]
        logger.info("🌐  ngrok tunnel: %s  ➜  %s", _NGROK_LISTENER.url(), public_url)
    except Exception as exc:
        sentry_capture(exc)
        logger.exception("Не удалось поднять ngrok-туннель")

def _stop_ngrok() -> None:  # pragma: no cover
    """Корректно закрывает туннель при завершении приложения."""
    global _NGROK_LISTENER
    if _NGROK_LISTENER:
        logger.info("Закрываю ngrok-туннель…")
        try:
            _NGROK_LISTENER.close()
        except Exception:
            logger.warning("Не удалось закрыть туннель корректно", exc_info=True)
        _NGROK_LISTENER = None


# ---------------------------------------------------------------------------#
# Routes                                                                     #
# ---------------------------------------------------------------------------#
@app.post("/sms/raw", status_code=status.HTTP_202_ACCEPTED)
async def post_raw_sms(payload: RawSMSPayload) -> JSONResponse:  # noqa: D401
    """Принимаем сырое SMS-сообщение и кладём в Redis Stream `sms_raw`."""
    try:
        payload_data = payload.model_dump()
        logger.debug("↘︎ /sms/raw payload: %s", payload_data)
        data = {
            "msg_id": get_md5_hash(payload_data.get("message")),
            "sender": payload_data.get("sender"),
            "body": payload_data.get("message"),
            "date": str(payload_data.get("timestamp")),
            "device_id": payload_data.get("device_id"),
            "source": payload_data.get("source")
        }
        raw_sms = RawSMS.model_validate(data)
    except Exception as exc:  # pydantic validation fails
        logger.warning("Payload validation failed", exc_info=True)
        sentry_capture(exc)
        raise HTTPException(status_code=400, detail="Invalid payload") from exc

    try:
        nuts = await get_nats_connection()
        await publish_raw_sms(nuts, raw_sms)  # публикуем
        logger.info("Queued raw SMS %s", raw_sms.msg_id)
        return JSONResponse(content={"result": "queued"}, status_code=status.HTTP_202_ACCEPTED)
    except Exception as exc:  # pragma: no cover – network
        sentry_capture(exc)
        logger.exception("Failed to push to Redis")
        raise HTTPException(status_code=500, detail="Internal error") from exc


@app.get("/health", status_code=status.HTTP_200_OK, response_model=None)
async def health() -> Dict[str, Any] | JSONResponse:  # noqa: D401
    """Проверка готовности. Легковесна: просто ping к Redis."""
    try:
        # Пытаемся подключиться
        nuts = await get_nats_connection()
        # Если эта строка выполнилась без ошибок, соединение установлено
        print("✅ Успешно подключено к NATS серверу!")
        print(f"   Статус клиента: {'ПОДКЛЮЧЕН' if nuts.is_connected else 'ОТКЛЮЧЕН'}")
        print(f"   Подключено к: {nuts.connected_url.netloc if nuts.connected_url else 'N/A'}")
        return {"status": "ok"}
        # Здесь можно начинать работу с NATS (publish/subscribe)
    except Exception as e:
        # Ловим другие возможные ошибки
        print(f"❌ Произошла непредвиденная ошибка при подключении: {e}")
        sentry_capture(e)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "redis_down"},
        )

# ---------------------------------------------------------------------------#
# Entrypoint                                                                 #
# ---------------------------------------------------------------------------#
if __name__ == "__main__":  # pragma: no cover
    settings = get_settings()

    # ──────────────────────────────────────────────────────────────────────
    # ngrok-туннель (если настроен)
    # ──────────────────────────────────────────────────────────────────────
    _start_ngrok(settings)

    # ──────────────────────────────────────────────────────────────────────
    # Грейсфул-завершение
    # ──────────────────────────────────────────────────────────────────────
    def _graceful_exit(*_sig: object) -> None:  # noqa: D401
        logger.info("SIGTERM/SIGINT caught, shutting down uvicorn…")
        _stop_ngrok()
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _graceful_exit)
    signal.signal(signal.SIGINT, _graceful_exit)

    # ──────────────────────────────────────────────────────────────────────
    # Запуск Uvicorn
    # ──────────────────────────────────────────────────────────────────────
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        lifespan="on",
    )

# tests/api_gateway/test_main.py

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

# ❗ Импортируем TestClient вместо AsyncClient
from fastapi.testclient import TestClient 
from fastapi import status

from libs.models import get_md5_hash
from libs.nats_utils import SUBJECT_RAW, publish_raw_sms
from services.api_gateway.main import app, RawSMS

# ❌ Убираем маркер pytestmark = pytest.mark.asyncio, он больше не нужен


@pytest.fixture
def client() -> TestClient:
    """
    Фикстура теперь создает и возвращает СИНХРОННЫЙ TestClient.
    """
    with TestClient(app) as c:
        yield c


class TestHealthCheck:
    """Тесты для эндпоинта GET /health с использованием TestClient."""

    # ❗ Функция теста теперь синхронная (def), а не асинхронная (async def)
    def test_health_success(self, client: TestClient):
        """
        Тест: проверка успешного ответа от /health, когда NATS доступен.
        """
        # Мокирование остается точно таким же, потому что мы мокируем
        # асинхронную функцию, которую TestClient вызовет внутри себя.
        mock_nats_conn = AsyncMock()
        mock_nats_conn.is_connected = True
        mock_nats_conn.connected_url.netloc = "nats-server:4222"

        with patch("services.api_gateway.main.get_nats_connection", return_value=mock_nats_conn) as mock_get_conn:
            # ❗ Вызов делается без await
            response = client.get("/health")

            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {"status": "ok"}
            # ❗ Проверка мока остается прежней, так как функция была вызвана асинхронно внутри TestClient
            mock_get_conn.assert_awaited_once()

    def test_health_nats_failure(self, client: TestClient):
        """
        Тест: проверка ответа 503, когда не удаётся подключиться к NATS.
        """
        # Мокируем get_nats_connection так, чтобы он вызывал исключение
        with patch("services.api_gateway.main.get_nats_connection", side_effect=Exception("NATS down")) as mock_get_conn, \
             patch("services.api_gateway.main.sentry_capture") as mock_sentry:
            response = client.get("/health")

            assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
            # В вашем коде возвращается "redis_down", хотя проверяется NATS. Тест проверяет фактическое поведение.
            assert response.json() == {"status": "redis_down"}
            
            mock_get_conn.assert_awaited_once()
            mock_sentry.assert_called_once()


@pytest.fixture
def valid_payload() -> dict:
    """Фикстура с валидными данными для SMS."""
    return {
            "device_id": "android-pixel-8a",
            "msg_id": "1718291822123",
            "message": "APPROVED PURCHASE DB SALE: …",
            "sender": "AMTBBANK",
            "timestamp": 1749808562,
            "source": "device"
        }
    
def test_post_sms_raw_and_publish_to_nats(valid_payload: dict, client):
    """
    Тест: POST /sms/raw успешно принимает данные и вызывает публикацию в NATS
    с корректно настроенными моками.
    """
    # --- ARRANGE (Подготовка моков) ---

    # 1. Готовим мок для асинхронного метода `publish`
    mock_publish_method = AsyncMock(return_value=MagicMock(seq=1))

    # 2. Готовим мок для клиента NATS (`nc`) и его СИНХРОННОГО метода .jetstream()
    mock_nc = MagicMock()
    mock_nc.jetstream.return_value.publish = mock_publish_method

    # 3. Мы будем использовать менеджер контекста `patch` для подмены зависимостей
    with patch("services.api_gateway.main.get_nats_connection", return_value=mock_nc) as mock_get_conn, \
         patch("libs.nats_utils.ensure_stream", new_callable=AsyncMock) as mock_ensure_stream:
        
        # --- ACT (Выполнение запроса) ---
        response = client.post("/sms/raw", json=valid_payload)

    # --- ASSERT (Проверки) ---

    # 1. Проверяем HTTP-ответ
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json() == {"result": "queued"}

    # 2. Проверяем, что наш сервис пытался получить соединение с NATS
    mock_get_conn.assert_awaited_once()

    # 3. Проверяем, что внутри `publish_raw_sms` была вызвана `ensure_stream`
    mock_ensure_stream.assert_awaited_once()

    # 4. Проверяем, что был вызван синхронный метод .jetstream() на нашем мок-клиенте
    mock_nc.jetstream.assert_called_once()
    
    # 5. Главная проверка: убеждаемся, что `publish` был вызван с правильными данными
    #    Для этого нам нужно воссоздать ожидаемый объект RawSMS, как это делает эндпоинт.
    expected_msg_id = get_md5_hash(valid_payload["message"])
    expected_raw_sms = RawSMS(
        msg_id=expected_msg_id,
        sender=valid_payload["sender"],
        body=valid_payload["message"],
        date=str(valid_payload["timestamp"]),
        device_id=valid_payload["device_id"],
        source=valid_payload["source"]
    )
    expected_payload_bytes = expected_raw_sms.model_dump_json().encode('utf-8')
    
    mock_publish_method.assert_awaited_once_with(
        SUBJECT_RAW, # Проверяем, что используется тема по умолчанию
        expected_payload_bytes # Проверяем, что тело сообщения было правильно сформировано
    )

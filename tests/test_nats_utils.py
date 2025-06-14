# tests/test_nats_utils.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Импортируем тестируемые функции и модели
from libs.nats_utils import (
    get_nats_connection,
    publish_raw_sms,
    SUBJECT_RAW,
)
from libs.models import RawSMS # Убедитесь, что модель RawSMS доступна для импорта

# Все тесты, использующие async/await, должны быть помечены этим маркером
pytestmark = pytest.mark.asyncio


@pytest.fixture
def sample_sms() -> RawSMS:
    """Фикстура, создающая тестовый объект RawSMS."""
    return RawSMS(
        sender="test_sender",
        text="Hello from pytest!",
        operator="test_op",
        received_at=1234567890.0,
        msg_id="test",
        body="test",
        date=""
    )


@pytest.fixture(autouse=True)
def clear_cache():
    """Автоматическая фикстура для очистки кеша синглтона после каждого теста."""
    yield
    # Очищаем lru_cache, чтобы тесты были изолированы друг от друга
    get_nats_connection.cache_clear()


async def test_get_nats_connection_is_singleton(mocker):
    """
    Проверяем, что get_nats_connection действительно является синглтоном
    и возвращает один и тот же объект при повторных вызовах.
    """
    # Arrange: Мокаем nats.connect, чтобы он не делал реального подключения
    mock_connect = mocker.patch("nats.connect", new_callable=AsyncMock)
    mock_connect.return_value = "fake_connection_object"

    # Act
    conn1 = await get_nats_connection()
    conn2 = await get_nats_connection()

    # Assert
    assert conn1 is conn2  # Объекты должны быть идентичны
    mock_connect.assert_called_once()  # Функция connect была вызвана только один раз



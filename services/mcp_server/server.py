# Этот код теперь должен работать без ошибок
import uvicorn
from mcp.server.fastmcp import FastMCP, Context
from fastapi import FastAPI

from libs.config import get_settings

settings = get_settings()

# Создаём объект-сервер
mcp = FastMCP(
    "PocketBase Connector",
    dependencies=[],
    instructions="""
        This server provides data analysis tools.
        Call get_average() to analyze numerical data.
    """
)


# Например:
from pocketbase import Client # Не забудьте эту строку

# Настройка клиента PocketBase
pb_client = Client(settings.pb_url)
#pb_client.auth_with_password(settings.pb_email, settings.pb_password) # type: ignore

@mcp.tool()
def create_note(content: str) -> str:
    """Создает новую заметку в базе данных."""
    try:
        new_record = pb_client.collection("notes").create({"content": content})
        return f"Заметка успешно создана с ID: {new_record.id}"
    except Exception as e:
        return f"Не удалось создать заметку: {e}"

# Запуск сервера
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="127.0.0.1", port=9000)
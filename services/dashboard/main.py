import asyncio
import httpx
import pandas as pd
import plotly.express as px
import logging
from typing import Any, Mapping, List
from datetime import datetime, timedelta
from libs.config import get_settings

# --- Настройка логирования для отладки ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
settings = get_settings()

# --- Класс для работы с PocketBase ---
class PocketBaseClient:
    """Асинхронный клиент для работы с API PocketBase."""
    def __init__(self, *, base_url: str, email: str, password: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._email = email
        self._password = password
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=30.0)
        self._token: str | None = None

    async def _ensure_token(self) -> None:
        if self._token:
            return
        logger.info("Аутентификация в PocketBase...")
        try:
            resp = await self._client.post(
                "/api/admins/auth-with-password",
                json={"identity": self._email, "password": self._password},
            )
            resp.raise_for_status()
            data = resp.json()
            self._token = data["token"]
            self._client.headers["Authorization"] = f"Bearer {self._token}"
            logger.info("Аутентификация прошла успешно.")
        except httpx.RequestError as exc:
            logger.error(f"Ошибка сети при аутентификации: {exc}. Убедитесь, что PocketBase запущен и доступен по адресу {self._base_url}")
            raise
        except httpx.HTTPStatusError as exc:
            logger.error(f"Ошибка аутентификации: {exc.response.status_code} - {exc.response.text}. Проверьте email и пароль.")
            raise

    async def get_all_records(self, collection: str, filter_query: str | None = None) -> List[Mapping[str, Any]]:
        """
        Получает записи из коллекции, опционально применяя фильтр на стороне базы данных.
        """
        await self._ensure_token()
        all_items = []
        page = 1
        per_page = 500
        
        # Подготовка параметров запроса
        params = {"page": page, "perPage": per_page, "sort": "-created"}
        if filter_query:
            params["filter"] = filter_query
            logger.info(f"Применяю фильтр: {filter_query}")

        logger.info(f"Начинаю выгрузку данных из коллекции '{collection}'...")
        while True:
            # Устанавливаем текущую страницу для пагинации
            params["page"] = page
            
            try:
                resp = await self._client.get(f"/api/collections/{collection}/records", params=params)
                resp.raise_for_status()
                data = resp.json()
                items = data.get("items", [])
                if not items:
                    break
                all_items.extend(items)
                logger.info(f"Загружено {len(items)} записей (всего: {len(all_items)}). Страница {data['page']}/{data['totalPages']}.")
                if data['page'] >= data['totalPages']:
                    break
                page += 1
            except httpx.HTTPStatusError as exc:
                logger.error(f"Ошибка при получении данных из PocketBase: {exc.response.status_code} - {exc.response.text}")
                raise
                
        logger.info(f"Выгрузка завершена. Всего получено {len(all_items)} записей.")
        return all_items


    async def close(self) -> None:
        await self._client.aclose()

def process_data_and_create_chart(records: List[Mapping[str, Any]], title: str):
    """Обрабатывает записи и создает HTML-файл с графиком."""
    if not records:
        logger.warning("Список записей пуст. График не будет построен.")
        return

    df = pd.DataFrame(records)
    logger.info("Данные успешно преобразованы в DataFrame.")
    
    # Обработка данных
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['datetime'] = pd.to_datetime(df.get('datetime'), errors='coerce')
    df.dropna(subset=['amount', 'datetime'], inplace=True)
    df['date'] = df['datetime'].dt.date
    
    # Агрегация данных
    daily_sums = df.groupby(['date', 'merchant'])['amount'].sum().reset_index()
    daily_sums = daily_sums.sort_values(by='date')
    logger.info("Данные успешно агрегированы.")

    # Создание графика
    fig = px.bar(
        daily_sums, x='date', y='amount', color='merchant',
        title=title,
        labels={'date': 'Дата', 'amount': 'Сумма платежей', 'merchant': 'Мерчант'},
        height=600
    )
    fig.update_layout(xaxis_tickangle=-45)
    
    # Сохранение в HTML
    output_filename = 'payments_by_day.html'
    fig.write_html(output_filename)
    fig.write_image('payments_by_day.png')
    logger.info(f"График успешно сохранен в файл: {output_filename}")

async def run_real_mode():
    """Запускает скрипт для подключения к вашей базе PocketBase."""
    logger.info("Запуск в реальном режиме...")
    PB_URL = settings.pb_url  # Адрес вашего сервера PocketBase
    PB_EMAIL = settings.pb_email      # Email администратора
    PB_PASSWORD = settings.pb_password          # Пароль администратора
    COLLECTION_NAME = "sms_data"
    # --- Создаем строку с датой для фильтра ---
    one_week_ago = datetime.now() - timedelta(days=7)
    date_filter_string = one_week_ago.strftime('%Y-%m-%d %H:%M:%S')
    pb_filter = f"datetime  >= '{date_filter_string}'"

    client = PocketBaseClient(base_url=PB_URL, email=PB_EMAIL, password=PB_PASSWORD) # type: ignore
    try:
        records = await client.get_all_records(COLLECTION_NAME, filter_query=pb_filter)
        process_data_and_create_chart(records, 'Сумма платежей по дням из PocketBase')
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(run_real_mode())
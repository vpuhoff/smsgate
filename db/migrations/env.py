# alembic/env.py

import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context

# Импортируем наши настройки и модели
# ==========================================================
from libs.config import get_settings
from db.models import Base
# ==========================================================


# это объект конфигурации Alembic, который получает значения из alembic.ini
config = context.config

# Интерпретируем файл конфигурации для логгирования Python.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# Указываем Alembic, на какие таблицы SQLAlchemy смотреть для автогенерации
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Запускает миграции в 'оффлайн' режиме (генерирует SQL)."""
    # Для оффлайн режима мы все еще можем брать URL из настроек
    settings = get_settings()
    context.configure(
        url=settings.database_url, # Используем URL из настроек
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Общая синхронная функция для запуска миграций."""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Запускает миграции в 'онлайн' режиме для асинхронного приложения."""
    
    # Получаем настройки нашего приложения
    settings = get_settings()

    # Создаем асинхронный движок НАПРЯМУЮ из URL в настройках.
    # Это избавляет от всех проблем с engine_from_config.
    connectable = create_async_engine(
        settings.database_url_async,
        poolclass=pool.NullPool,
    )

    # Устанавливаем асинхронное соединение
    async with connectable.connect() as connection:
        # Запускаем синхронную логику миграций внутри асинхронного контекста
        await connection.run_sync(do_run_migrations)

    # Корректно освобождаем ресурсы движка
    await connectable.dispose()


# Главный блок логики, который определяет, какой режим запускать
if context.is_offline_mode():
    run_migrations_offline()
else:
    # Запускаем асинхронную функцию через asyncio
    asyncio.run(run_migrations_online())
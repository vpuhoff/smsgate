"""
Единая конфигурация для **всех** сервисов проекта.

* Использует Pydantic-BaseSettings – значения берутся из переменных окружения
  (или файла .env, если он есть).
* Функция `get_settings()` отдаёт *кешированный* объект – удобно импортировать
  где угодно, не опасаясь создать дубль.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings

# --------------------------------------------------------------------------- #
# Основные настройки
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    # ── Redis ────────────────────────────────────────────────────────────────
    nats_dsn: str = Field("redis://localhost:6379/0", env="NATS_DSN") # type: ignore

    # ── PocketBase ───────────────────────────────────────────────────────────
    pb_url: str = Field("http://127.0.0.1:8090", env="PB_URL") # type: ignore
    pb_email: str = Field(env="PB_EMAIL") # type: ignore
    pb_password: str = Field(env="PB_PASSWORD") # type: ignore

    # ── Sentry ───────────────────────────────────────────────────────────────
    sentry_dsn: Optional[str] = Field(None, env="SENTRY_DSN") # type: ignore
    gemini_api_key: Optional[str] = Field(None, env="GEMINI_API_KEY") # type: ignore

    # ── XML-backup watcher ───────────────────────────────────────────────────
    backup_dir: Path = Field(Path("./backups"), env="BACKUP_DIR") # type: ignore

    # ── ngrok (для API-шлюза) ────────────────────────────────────────────────
    enable_ngrok: bool = Field(False, env="ENABLE_NGROK") # type: ignore
    ngrok_authtoken: Optional[str] = Field(None, env="NGROK_AUTHTOKEN")  # type: ignore
    ngrok_domain: Optional[str] = Field(None, env="NGROK_DOMAIN")  # type: ignore
    api_host: Optional[str] = Field("0.0.0.0", env="API_HOST")  # type: ignore
    api_port: Optional[int] = Field(9001, env="API_PORT")  # type: ignore

    # ── Метрики (по портам) ──────────────────────────────────────────────────
    api_metrics_port: int = Field(9101, env="API_METRICS_PORT") # type: ignore
    parser_metrics_port: int = Field(9102, env="PARSER_METRICS_PORT") # type: ignore
    pbwriter_metrics_port: int = Field(9103, env="PBWRITER_METRICS_PORT") # type: ignore

    tg_bot_token: str = Field( env="API_METRICS_PORT") # type: ignore
    tg_chat_ids: str = Field( env="API_METRICS_PORT") # type: ignore
    check_interval_seconds: int = Field( env="CHECK_INTERVAL_SECONDS") # type: ignore

    # ── Валидация ────────────────────────────────────────────────────────────
    @model_validator(mode="after")
    def _validate_dirs(self) -> "Settings":
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        return self

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    # ── PostgreSQL ─────────────────────────────────────────────────────────
    # Эти переменные считываются из .env и используются для Docker Compose
    # и для подключения из приложения.
    postgres_user: str = Field(env="POSTGRES_USER") # type: ignore
    postgres_password: str = Field(env="POSTGRES_PASSWORD") # type: ignore
    postgres_db: str = Field(env="POSTGRES_DB") # type: ignore
    postgres_host: str = Field("localhost", env="POSTGRES_HOST") # type: ignore
    postgres_port: int = Field(5432, env="POSTGRES_PORT") # type: ignore

    @computed_field # Используем @computed_field для Pydantic v2
    @property
    def database_url(self) -> str:
        """
        Генерирует URL для подключения к базе данных SQLAlchemy.
        Формат: postgresql+psycopg2://user:password@host:port/db
        """
        return (
            "postgresql+psycopg2://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field # Используем @computed_field для Pydantic v2
    @property
    def database_url_async(self) -> str:
        """
        Генерирует URL для подключения к базе данных SQLAlchemy.
        Формат: postgresql+psycopg2://user:password@host:port/db
        """
        return (
            "postgresql+asyncpg://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

# --------------------------------------------------------------------------- #
# Public helper
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def get_settings() -> Settings:  # noqa: D401
    """Возвращает **singleton** объект Settings."""
    return Settings()


# --------------------------------------------------------------------------- #
# CLI-debug
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import json

    print(json.dumps(get_settings().model_dump(), indent=2, default=str))
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

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings

# --------------------------------------------------------------------------- #
# Основные настройки
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    # ── Redis ────────────────────────────────────────────────────────────────
    redis_dsn: str = Field("redis://localhost:6379/0", env="REDIS_DSN")
    nats_dsn: str = Field("redis://localhost:6379/0", env="NATS_DSN")

    # ── PocketBase ───────────────────────────────────────────────────────────
    pb_url: str = Field("http://127.0.0.1:8090", env="PB_URL")
    pb_email: Optional[str] = Field(None, env="PB_EMAIL")
    pb_password: Optional[str] = Field(None, env="PB_PASSWORD")

    # ── Sentry ───────────────────────────────────────────────────────────────
    sentry_dsn: Optional[str] = Field(None, env="SENTRY_DSN")
    gemini_api_key: Optional[str] = Field(None, env="GEMINI_API_KEY")

    # ── XML-backup watcher ───────────────────────────────────────────────────
    backup_dir: Path = Field(Path("./backups"), env="BACKUP_DIR")

    # ── ngrok (для API-шлюза) ────────────────────────────────────────────────
    enable_ngrok: bool = Field(False, env="ENABLE_NGROK")
    ngrok_authtoken: Optional[str] = Field(None, env="NGROK_AUTHTOKEN")
    ngrok_domain: Optional[str] = Field(None, env="NGROK_DOMAIN") 
    api_host: Optional[str] = Field("0.0.0.0", env="API_HOST") 
    api_port: Optional[int] = Field(9001, env="API_PORT") 

    # ── Метрики (по портам) ──────────────────────────────────────────────────
    api_metrics_port: int = Field(9101, env="API_METRICS_PORT")
    parser_metrics_port: int = Field(9102, env="PARSER_METRICS_PORT")
    pbwriter_metrics_port: int = Field(9103, env="PBWRITER_METRICS_PORT")

    # ── Валидация ────────────────────────────────────────────────────────────
    @model_validator(mode="after")
    def _validate_dirs(self) -> "Settings":
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        return self

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


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
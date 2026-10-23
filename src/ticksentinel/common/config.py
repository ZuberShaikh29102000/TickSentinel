"""Runtime configuration, read from environment variables / .env (never hard-coded)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Production (Render -> Supabase): one secret URL, set in the Render dashboard.
    # Takes precedence over the POSTGRES_* parts below.
    database_url: SecretStr | None = None

    # Local development: the Postgres container from docker-compose.
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_user: str = "ticksentinel"
    postgres_password: SecretStr | None = None
    postgres_db: str = "ticksentinel"

    anthropic_api_key: SecretStr | None = None

    record_dir: Path = Path("data/raw")
    log_dir: Path = Path("logs")
    book_depth_stored: int = 10

    # HTTP port for /health. Render injects PORT and requires the service to bind it.
    port: int = 8080

    @property
    def postgres_dsn(self) -> str:
        if self.database_url is not None:
            return self.database_url.get_secret_value()
        if self.postgres_password is None:
            raise ValueError("set DATABASE_URL, or POSTGRES_PASSWORD for local Postgres")
        pw = self.postgres_password.get_secret_value()
        return (
            f"postgresql://{self.postgres_user}:{pw}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

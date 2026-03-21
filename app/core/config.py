from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from pydantic import field_validator


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str

    # Claude (Anthropic)
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-3-5-sonnet-latest"
    ANTHROPIC_BASE_URL: str = "https://api.anthropic.com"

    # App security
    API_SECRET_KEY: str

    # App general
    BASE_URL: str = "http://localhost:8000"
    LOG_LEVEL: str = "INFO"
    ZOHO_CALLBACK_URL: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("ANTHROPIC_BASE_URL")
    @classmethod
    def normalize_anthropic_base_url(cls, value: str) -> str:
        """
        Avoid https://api.anthropic.com/v1 + /v1/messages => 404.
        Strip trailing /v1 if present (common misconfiguration).
        """
        u = (value or "").strip().rstrip("/")
        if u.endswith("/v1"):
            u = u[:-3].rstrip("/")
        return u

    @field_validator("DATABASE_URL")
    @classmethod
    def ensure_async_database_url(cls, value: str) -> str:
        """
        Ensure SQLAlchemy uses the asyncpg driver.
        Accepts `postgresql://` for convenience and upgrades it to `postgresql+asyncpg://`.
        """
        normalized_value = (value or "").strip()
        if normalized_value.startswith("postgresql://"):
            return normalized_value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return normalized_value


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
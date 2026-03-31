from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from pydantic import field_validator


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str

    # LLM routing: "anthropic" = Claude (ANTHROPIC_*), "openai" = OpenAI Chat (OPENAI_*)
    AI_PROVIDER: str = "anthropic"
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-3-5-sonnet-latest"
    ANTHROPIC_BASE_URL: str = "https://api.anthropic.com"
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_BASE_URL: str = "https://api.openai.com"

    # App security
    API_SECRET_KEY: str

    # App general
    BASE_URL: str = "http://localhost:8000"
    LOG_LEVEL: str = "INFO"
    ZOHO_CALLBACK_URL: str = ""
    # How to POST to ZOHO_CALLBACK_URL: "json" (application/json) or "form" (x-www-form-urlencoded).
    # Many Zoho webhooks / Functions expect form fields, not JSON — use "form" if you get HTTP 400.
    ZOHO_CALLBACK_BODY_FORMAT: str = "json"

    # Zoho CRM V8 — OAuth + optional “attach PDF link” after job completes
    # https://www.zoho.com/crm/developer/docs/api/v8/oauth-overview.html
    ZOHO_CLIENT_ID: str = ""
    ZOHO_CLIENT_SECRET: str = ""
    ZOHO_REFRESH_TOKEN: str = ""
    # e.g. https://accounts.zoho.com (also .eu .in .com.au for other DCs)
    ZOHO_ACCOUNTS_BASE_URL: str = "https://accounts.zoho.com"
    # e.g. https://www.zohoapis.com
    ZOHO_CRM_API_BASE: str = "https://www.zohoapis.com"
    # Legacy default module API name (fallback for both outline/slides when specific vars are unset).
    ZOHO_CRM_MODULE_API_NAME: str = "Course_Outline"
    # Course outline flow module (PDF link attach target).
    ZOHO_CRM_OUTLINE_MODULE_API_NAME: str = ""
    # Slides flow module (source record where File Upload field `outline` is read).
    ZOHO_CRM_SLIDES_MODULE_API_NAME: str = ""
    # When true, after PDF is generated, attach public URL to CRM record (needs OAuth + module)
    ZOHO_ATTACH_PDF_LINK_TO_CRM: bool = False

    # Gamma Public API (PPT generation)
    GAMMA_API_KEY: str = ""
    GAMMA_BASE_URL: str = "https://public-api.gamma.app"

    # Google Apps Script merge endpoint (optional, for single merged editable Slides link)
    GOOGLE_SCRIPT_URL: str = ""
    GOOGLE_SCRIPT_KEY: str = ""
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REFRESH_TOKEN: str = ""
    GOOGLE_DRIVE_FOLDER_ID: str = ""
    # Optional. If empty, course outlines go under My Drive root: ai_automation/course_outline/...
    # If set, that folder is the parent (same hierarchy underneath).
    GOOGLE_DRIVE_COURSE_OUTLINES_PARENT_FOLDER_ID: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator(
        "ZOHO_CLIENT_ID",
        "ZOHO_CLIENT_SECRET",
        "ZOHO_REFRESH_TOKEN",
        "ZOHO_ACCOUNTS_BASE_URL",
        "ZOHO_CRM_API_BASE",
        "ZOHO_CRM_MODULE_API_NAME",
        "ZOHO_CRM_OUTLINE_MODULE_API_NAME",
        "ZOHO_CRM_SLIDES_MODULE_API_NAME",
        mode="before",
    )
    @classmethod
    def strip_zoho_strings(cls, value: str) -> str:
        return (value or "").strip() if isinstance(value, str) else value

    @field_validator("AI_PROVIDER", mode="before")
    @classmethod
    def normalize_ai_provider(cls, value: object) -> str:
        """anthropic → Claude API; openai → OpenAI Chat Completions. Accepts a few aliases."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return "anthropic"
        s = str(value).strip().lower()
        aliases: dict[str, str] = {
            "anthropic": "anthropic",
            "claude": "anthropic",
            "antropic": "anthropic",  # common typo
            "openai": "openai",
            "chatgpt": "openai",
        }
        out = aliases.get(s, s)
        if out not in ("anthropic", "openai"):
            raise ValueError(
                "AI_PROVIDER must be 'anthropic' (Claude) or 'openai' (OpenAI). "
                f"Received: {value!r}"
            )
        return out

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

    @field_validator("OPENAI_BASE_URL")
    @classmethod
    def normalize_openai_base_url(cls, value: str) -> str:
        """
        Keep OpenAI base URL root-only to avoid /v1/v1/chat/completions.
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
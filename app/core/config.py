from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from pydantic import field_validator, model_validator


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str

    # LLM routing: "anthropic" = Claude (ANTHROPIC_*), "openai" = OpenAI Chat (OPENAI_*)
    AI_PROVIDER: str = "anthropic"
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"
    ANTHROPIC_BASE_URL: str = "https://api.anthropic.com"
    # httpx read timeout for Anthropic (long generations + web_search often exceed 2 minutes).
    ANTHROPIC_READ_TIMEOUT_S: float = 600.0
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
    # Field API name in slides module to store module-wise Gamma links text.
    ZOHO_CRM_SLIDES_LINKS_FIELD_API_NAME: str = "Link_for_Courseware"
    # When true, after PDF is generated, attach public URL to CRM record (needs OAuth + module)
    ZOHO_ATTACH_PDF_LINK_TO_CRM: bool = False

    # Gamma Public API (PPT generation)
    GAMMA_API_KEY: str = ""
    GAMMA_BASE_URL: str = "https://public-api.gamma.app"
    # Optional Gamma template flow (POST /v1.0/generations/from-template)
    GAMMA_USE_TEMPLATE: bool = False
    GAMMA_TEMPLATE_ID: str = ""
    # Optional sharing/access controls for generated Gamma docs
    GAMMA_WORKSPACE_ACCESS: str = "edit"
    GAMMA_EXTERNAL_ACCESS: str = "edit"
    # Comma-separated emails to grant edit access, e.g. "a@x.com,b@y.com"
    GAMMA_EMAIL_EDIT_LIST: str = ""

    # Slides multi-bot pipeline configuration
    SLIDES_PLANNER_MODEL: str = ""
    SLIDES_GENERATOR_MODEL: str = ""
    SLIDES_VALIDATOR_MODEL: str = ""
    SLIDES_VALIDATION_MAX_LOOPS: int = 2
    SLIDES_MIN_PER_MODULE: int = 10
    SLIDES_MAX_PER_MODULE: int = 20
    SLIDES_MODULE_PARALLELISM: int = 3
    # How strongly instructor PPT text influences planning/generation: "supplement" (default) or "primary".
    SLIDES_INSTRUCTOR_PPT_PRIORITY: str = "supplement"
    # When true, slides jobs generate pre-assessment MCQs from outline text and post-assessment from validated slides (parallel with Gamma where possible).
    SLIDES_ASSESSMENTS_ENABLED: bool = False
    SLIDES_PRE_ASSESSMENT_QUESTIONS: int = 15
    SLIDES_POST_ASSESSMENT_QUESTIONS: int = 15
    # Default difficulty applied to on-demand courseware assessments when a per-request value is missing.
    COURSEWARE_ASSESSMENT_DEFAULT_DIFFICULTY: str = "intermediate"
    COURSEWARE_ASSESSMENT_DEFAULT_NUM_QUESTIONS: int = 15

    # Vercel frontend URL where /assessment/{zoho_record_id}/pre|post lives.
    # Example: https://learn.example.com (no trailing slash required).
    FRONTEND_BASE_URL: str = ""
    # HMAC secret used to mint short signed tokens (?t=...) on the public assessment URLs.
    # When empty, links are issued without a token (less secure if zoho_record_id is guessable).
    ASSESSMENT_LINK_SECRET: str = ""
    # When true, the courseware-assessments endpoints require a valid t= token; otherwise the
    # token is optional (X-API-Key alone is enough for trusted Vercel server-side proxy).
    ASSESSMENT_LINK_REQUIRE_TOKEN: bool = False
    # Per-IP and per-record-id rate limit (requests / minute) on public assessment generation.
    ASSESSMENT_RATE_LIMIT_PER_MIN: int = 12
    # Zoho CRM field API name used to store pre/post assessment URLs (separate from Gamma links).
    ZOHO_CRM_ASSESSMENT_LINKS_FIELD_API_NAME: str = "Assessment_Links"
    # When true, on slides job completion store a copy of validated_slides.json content into the
    # CourseJob.payload_json (under "validated_slides_blob") so multi-replica deploys can serve
    # post-assessment generation without sharing the on-disk cache.
    COURSEWARE_VALIDATED_BLOB_IN_PAYLOAD: bool = True

    # Published Google Sheet CSV (see app/services/public_course_sheet.py). Public outline jobs only.
    PUBLIC_COURSE_SHEET_CSV_URL: str = ""
    PUBLIC_COURSE_SHEET_LOOKUP_ENABLED: bool = True
    # Optional explicit header names (normalized: spaces → underscores). Empty = fuzzy detect.
    PUBLIC_COURSE_SHEET_COURSE_COLUMN: str = ""
    PUBLIC_COURSE_SHEET_PDF_COLUMN: str = ""
    # Deprecated: if PUBLIC_COURSE_SHEET_CSV_URL is empty, this value is used instead.
    PUBLIC_COURSE_CATALOG_CSV_URL: str = ""
    # Zoho CRM field API name on the outline module for the Final Formatted Curriculum link (public jobs).
    ZOHO_CRM_PUBLIC_FINAL_CURRICULUM_FIELD_API_NAME: str = ""

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

    @model_validator(mode="after")
    def _public_sheet_csv_url_from_legacy(self) -> "Settings":
        if not (self.PUBLIC_COURSE_SHEET_CSV_URL or "").strip():
            legacy = (self.PUBLIC_COURSE_CATALOG_CSV_URL or "").strip()
            if legacy:
                self.PUBLIC_COURSE_SHEET_CSV_URL = legacy
        return self

    @field_validator(
        "ZOHO_CLIENT_ID",
        "ZOHO_CLIENT_SECRET",
        "ZOHO_REFRESH_TOKEN",
        "ZOHO_ACCOUNTS_BASE_URL",
        "ZOHO_CRM_API_BASE",
        "ZOHO_CRM_MODULE_API_NAME",
        "ZOHO_CRM_OUTLINE_MODULE_API_NAME",
        "ZOHO_CRM_SLIDES_MODULE_API_NAME",
        "ZOHO_CRM_SLIDES_LINKS_FIELD_API_NAME",
        "ZOHO_CRM_ASSESSMENT_LINKS_FIELD_API_NAME",
        "ZOHO_CRM_PUBLIC_FINAL_CURRICULUM_FIELD_API_NAME",
        "PUBLIC_COURSE_SHEET_CSV_URL",
        "PUBLIC_COURSE_CATALOG_CSV_URL",
        "PUBLIC_COURSE_SHEET_COURSE_COLUMN",
        "PUBLIC_COURSE_SHEET_PDF_COLUMN",
        "FRONTEND_BASE_URL",
        "ASSESSMENT_LINK_SECRET",
        "SLIDES_PLANNER_MODEL",
        "SLIDES_GENERATOR_MODEL",
        "SLIDES_VALIDATOR_MODEL",
        "GAMMA_TEMPLATE_ID",
        "GAMMA_WORKSPACE_ACCESS",
        "GAMMA_EXTERNAL_ACCESS",
        "GAMMA_EMAIL_EDIT_LIST",
        mode="before",
    )
    @classmethod
    def strip_zoho_strings(cls, value: str) -> str:
        return (value or "").strip() if isinstance(value, str) else value

    @field_validator("ASSESSMENT_LINK_REQUIRE_TOKEN", "COURSEWARE_VALIDATED_BLOB_IN_PAYLOAD", mode="before")
    @classmethod
    def coerce_bool_flags(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ("true", "1", "yes", "on"):
                return True
            if s in ("false", "0", "no", "off", ""):
                return False
        return bool(value)

    @field_validator("FRONTEND_BASE_URL", mode="before")
    @classmethod
    def normalize_frontend_base_url(cls, value: object) -> str:
        s = str(value or "").strip()
        return s.rstrip("/")

    @field_validator("COURSEWARE_ASSESSMENT_DEFAULT_DIFFICULTY", mode="before")
    @classmethod
    def normalize_default_difficulty(cls, value: object) -> str:
        s = str(value or "intermediate").strip().lower()
        if s in ("basic", "beginner", "fundamental", "entry"):
            return "basic"
        if s in ("advanced", "expert"):
            return "advanced"
        return "intermediate"

    @field_validator("GAMMA_USE_TEMPLATE", mode="before")
    @classmethod
    def coerce_gamma_use_template(cls, value: object) -> bool:
        """Railway/.env often provide booleans as strings; accept common variants."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ("true", "1", "yes", "on"):
                return True
            if s in ("false", "0", "no", "off", ""):
                return False
        return bool(value)

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

    @field_validator("SLIDES_INSTRUCTOR_PPT_PRIORITY", mode="before")
    @classmethod
    def normalize_slides_instructor_ppt_priority(cls, value: object) -> str:
        s = str(value or "supplement").strip().lower()
        if s == "primary":
            return "primary"
        return "supplement"

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

    def get_gamma_email_edit_list(self) -> list[str]:
        """Parse comma-separated GAMMA_EMAIL_EDIT_LIST into clean email strings."""
        raw = str(self.GAMMA_EMAIL_EDIT_LIST or "").strip()
        if not raw:
            return []
        return [email.strip() for email in raw.split(",") if email.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
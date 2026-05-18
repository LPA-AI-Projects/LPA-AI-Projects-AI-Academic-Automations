from pydantic import BaseModel


class CourseOutlineIntegrationStatus(BaseModel):
    """Non-secret flags: whether env vars are set for course-outline Drive + Zoho flows."""

    google_drive_oauth_configured: bool
    google_drive_folder_configured: bool
    zoho_webhook_configured: bool
    zoho_crm_attach_configured: bool


class BitrixCourseOutlineIntegrationStatus(BaseModel):
    """Non-secret flags for Bitrix24 incoming webhook + optional callback."""

    bitrix_webhook_configured: bool
    bitrix_application_token_configured: bool
    bitrix_crm_attach_configured: bool
    bitrix_task_attach_configured: bool
    bitrix_completion_callback_configured: bool

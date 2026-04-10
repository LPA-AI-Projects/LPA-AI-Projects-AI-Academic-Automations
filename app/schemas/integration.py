from pydantic import BaseModel


class CourseOutlineIntegrationStatus(BaseModel):
    """Non-secret flags: whether env vars are set for course-outline Drive + Zoho flows."""

    google_drive_oauth_configured: bool
    google_drive_folder_configured: bool
    zoho_webhook_configured: bool
    zoho_crm_attach_configured: bool
    public_course_sheet_configured: bool

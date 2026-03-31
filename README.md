# LPA-AI-Projects-AI-Academic-Automations

All Academic Operations

## Layout

- `app/` — FastAPI application (API, services, models)
- `scripts/` — optional local utilities (not required at runtime)
- `storage/` — generated PDFs, slide cache, and uploads (created on startup; gitignored). Override with `COURSE_AI_STORAGE_ROOT`.

## Docker Setup (Dev/Prod)

### 1) Local development (API + Postgres)

1. Copy env template:
   - `copy .env.dev.example .env.dev`
2. Fill secrets in `.env.dev` (`ANTHROPIC_API_KEY`, etc.)
3. Start stack:
   - `docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build`

API runs on `http://localhost:8000`, Postgres runs on `localhost:5432`.

### 2) Production-style container run (API only)

1. Copy env template:
   - `copy .env.prod.example .env.prod`
2. Set managed Postgres URL in `.env.prod`
3. Start API:
   - `docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d`

Use a managed Postgres service in production (RDS/Azure/Supabase/Neon), not a DB inside the same app container.

## Deploy on Railway (FastAPI backend)

### 1) Create services

1. In Railway, create a new project from this GitHub repo.
2. Add a **PostgreSQL** service in the same project.
3. Keep app service using the repo `Dockerfile` (already configured).

### 2) Set backend environment variables (Railway app service)

Set these variables in Railway:

- `DATABASE_URL` = connection string from Railway Postgres (ensure it uses `postgresql://`; app auto-converts to `postgresql+asyncpg://`)
- `API_SECRET_KEY` = strong random secret
- `BASE_URL` = your Railway public backend URL
- `LOG_LEVEL` = `INFO`
- `ANTHROPIC_API_KEY` = your Claude API key
- `ANTHROPIC_MODEL` = e.g. `claude-3-5-sonnet-latest`
- `ANTHROPIC_BASE_URL` = `https://api.anthropic.com`
- `ZOHO_CALLBACK_URL` = optional callback endpoint

### 3) Deploy

Push to `main`. Railway auto-deploys.
Health check endpoint: `/api/v1/health`

### 4) Important production notes

- Railway provides dynamic `PORT`; container is already configured for it.
- Runtime files live under `storage/` (`storage/pdfs`, `storage/ppts`, `storage/uploads`). On Railway this volume is still ephemeral unless you attach persistent storage or use object storage (S3/R2/GCS).
- Optional: set `COURSE_AI_STORAGE_ROOT` to point uploads/PDFs elsewhere.

## Job API shape (course outline)

- **Default** `POST /api/v1/courses` → **202** with `job_id`, `zoho_record_id`, `status`, `message`, and `polling.by_zoho_record_id` → `GET /api/v1/courses/{zoho_record_id}/outline-job` (slim JSON: no Gamma/slides fields).
- **Poll** `GET /api/v1/courses/{zoho_record_id}/outline-job` (slim JSON for outline jobs).
- **Synchronous** `POST /api/v1/courses?sync=true` → **200** when AI + PDF finish (same slim shape as outline job). May time out from Zoho; prefer async + poll.

**Slides** (separate): `POST /api/v1/slides/` then `GET /api/v1/slides/{zoho_record_id}` for `module_gamma_links`, etc.

## Zoho CRM V8 — OAuth + attach PDF link (optional)

See [OAuth overview](https://www.zoho.com/crm/developer/docs/api/v8/oauth-overview.html) and [Upload attachment (file or link)](https://www.zoho.com/crm/developer/docs/api/v8/upload-attachment.html).

1. Create a Zoho **Self Client** / **Server-based** app, generate **refresh token** with scopes including CRM modules + attachments (e.g. `ZohoCRM.modules.ALL` and attachment scope as required by Zoho).
2. Set environment variables (e.g. Railway):

| Variable | Purpose |
|----------|---------|
| `ZOHO_CLIENT_ID` | OAuth client id |
| `ZOHO_CLIENT_SECRET` | OAuth client secret |
| `ZOHO_REFRESH_TOKEN` | Long-lived refresh token |
| `ZOHO_ACCOUNTS_BASE_URL` | e.g. `https://accounts.zoho.com` (use `.eu` / `.in` etc. for your DC) |
| `ZOHO_CRM_API_BASE` | Usually `https://www.zohoapis.com` |
| `ZOHO_CRM_MODULE_API_NAME` | Legacy fallback module API name (used when specific outline/slides vars are not set). |
| `ZOHO_CRM_OUTLINE_MODULE_API_NAME` | CRM module API name for **course-outline attach** (recommended). |
| `ZOHO_CRM_SLIDES_MODULE_API_NAME` | CRM module API name for **slides input fetch** from file-upload field `outline`. |
| `ZOHO_ATTACH_PDF_LINK_TO_CRM` | `true` to attach the generated public PDF URL to the record after the job completes |

3. **`zoho_record_id` in `POST /courses` must be the outline-module record ID** Zoho sends (the long numeric **Record Id** from CRM, same id used in the URL when you open the record). The backend attaches the generated **PDF public link** to that record via `POST .../crm/v8/{outline_module}/{record_id}/Attachments` (link attachment). Set `ZOHO_CRM_OUTLINE_MODULE_API_NAME` for this flow.

4. Access tokens are refreshed automatically (cached; refresh uses your refresh token before the ~1 hour expiry).

### Where to set Zoho / OAuth variables

| Where | What |
|--------|------|
| **Railway** | Project → your **API service** → **Variables** → add each key/value → **Redeploy**. |
| **Local** | Copy into `.env` next to `API_SECRET_KEY`, `DATABASE_URL`, etc. (never commit `.env`). |

Required for attach-after-job:

- `ZOHO_CLIENT_ID` — Zoho API Console → your **Self Client** / **Server-based application** → Client ID.
- `ZOHO_CLIENT_SECRET` — same app → Client Secret.
- `ZOHO_REFRESH_TOKEN` — generated once via OAuth grant with required CRM scopes (see Zoho OAuth docs); store securely.
- `ZOHO_ACCOUNTS_BASE_URL` — `https://accounts.zoho.com` (US); use `https://accounts.zoho.eu`, `https://accounts.zoho.in`, etc. if your org is in that DC.
- `ZOHO_CRM_API_BASE` — usually `https://www.zohoapis.com`.
- `ZOHO_CRM_OUTLINE_MODULE_API_NAME` — CRM module API name for course-outline attach flow.
- `ZOHO_CRM_SLIDES_MODULE_API_NAME` — CRM module API name for slides source fetch flow (field `outline`).
- `ZOHO_CRM_MODULE_API_NAME` — optional legacy fallback when the two specific vars above are not set.
- `ZOHO_ATTACH_PDF_LINK_TO_CRM` — `true` to attach the generated public PDF URL to the record after the job completes.

### Callback URL (`ZOHO_CALLBACK_URL`) — HTTP 400 / “file not received”

- The callback **does not upload PDF bytes**; it POSTs JSON (or form) with a **`pdf_url`** string. Your Zoho Function / Flow must **download** that URL or **use CRM attach** via OAuth (`ZOHO_ATTACH_PDF_LINK_TO_CRM`).
- If the callback returns **400**, Zoho often expects **`application/x-www-form-urlencoded`** instead of JSON. Set:
  - `ZOHO_CALLBACK_BODY_FORMAT=form`
- After deploy, check logs: `Zoho callback rejected | ... body=...` shows Zoho’s error message.

**If logs show HTML / “Zoho Accounts” / a login page:** `ZOHO_CALLBACK_URL` is **wrong** — you pasted a **browser** URL (login or CRM UI), not a **machine** URL. The backend POSTs **without cookies**, so Zoho returns the Accounts HTML page → **400**. Fix: use a real webhook target, for example:
- **CRM → Functions** → create a function → copy its **Invoke URL** (public URL that accepts POST), or  
- **Zoho Flow** → incoming webhook URL, or  
- Your own **Railway/ngrok** endpoint that receives the callback.

**CRM PDF on the record without a callback URL:** set `ZOHO_ATTACH_PDF_LINK_TO_CRM=true` and OAuth env vars so this API attaches the `pdf_url` via [Upload attachment (link)](https://www.zoho.com/crm/developer/docs/api/v8/upload-attachment.html).

### Zoho OAuth “missing access_token” / attach fails

Logs will now show Zoho’s `error` and `error_description`. Common fixes:

| Symptom | Fix |
|---------|-----|
| `invalid_client` | Wrong `ZOHO_CLIENT_ID` / `ZOHO_CLIENT_SECRET` or extra spaces/newlines in Railway variables. |
| `invalid_grant` | Refresh token revoked, expired, or generated for a **different** client id. Regenerate refresh token in API Console. |
| `invalid_code` (often with **empty** `error_description`) | Refresh token and client don’t match, or **wrong DC**: regenerate refresh token using the **same** API Console app and **same** accounts host (e.g. EU app → `https://accounts.zoho.eu`). Do **not** store a one-time **authorization `code`** in `ZOHO_REFRESH_TOKEN` — only the **`refresh_token`** field from the token exchange JSON. |
| Wrong **data center** | If your org is EU/IN/AU, use `ZOHO_ACCOUNTS_BASE_URL=https://accounts.zoho.eu` (or `.in`, `.com.au`) — must match where the app was created. |
| Token URL must be `POST` to `/oauth/v2/token` | Already handled; do not paste a browser URL into `ZOHO_ACCOUNTS_BASE_URL` (only the origin, e.g. `https://accounts.zoho.com`). |

**Callback HTML / 400:** `ZOHO_CALLBACK_URL` must not be a Zoho login page. Leave **`ZOHO_CALLBACK_URL` empty** until you have a real Function/Flow webhook URL, or attach will still work via OAuth above.

**Verify OAuth outside the app** (replace placeholders; use your DC host):

```bash
curl -sS -X POST "https://accounts.zoho.com/oauth/v2/token" \
  -d "grant_type=refresh_token" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "refresh_token=YOUR_REFRESH_TOKEN"
```

You must see JSON with `"access_token"`. If you see `"error":"invalid_code"`, fix credentials in Zoho API Console before Railway will work.

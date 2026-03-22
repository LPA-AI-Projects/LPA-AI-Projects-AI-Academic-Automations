# LPA-AI-Projects-AI-Academic-Automations

All Academic Operations

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
- `generated_pdfs/` on Railway is ephemeral storage. Files can disappear on restart.
- For production-grade persistence, store PDFs in object storage (S3/R2/GCS) and save the URL in DB.

## Job API shape

- **Default** `POST /api/v1/courses` → **202** with only `{ "job_id", "zoho_record_id" }` (no `"processing"`). Poll `GET /api/v1/jobs/{job_id}` for the full object (`status`, `pdf_url`, `course_id`, …).
- **Synchronous** `POST /api/v1/courses?sync=true` → **200** with the **full** result in one response (same shape as when the job is done). The request **waits** until AI + PDF finish (often **minutes**); many systems (including Zoho) **may time out**. Prefer async + poll, or use CRM attach below.

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
| `ZOHO_CRM_MODULE_API_NAME` | API name of the module, e.g. `Leads`, `Deals`, or custom module name |
| `ZOHO_ATTACH_PDF_LINK_TO_CRM` | `true` to attach the generated public PDF URL to the record after the job completes |

3. **`zoho_record_id` in your webhook must be the CRM record ID** used in the API path (numeric string from Zoho). Custom labels like `ZHO-LEARNQUEST-001` will not work with the Attachments API unless that string is actually the record id in your CRM.

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
- `ZOHO_CRM_MODULE_API_NAME` — CRM **Setup → Developer Space → APIs** (or module settings) → **API Name** (e.g. `Leads`, `Deals`, `Custom_Module_X`).
- `ZOHO_ATTACH_PDF_LINK_TO_CRM` — `true` to attach the generated public PDF URL to the record after the job completes.

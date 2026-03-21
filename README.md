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

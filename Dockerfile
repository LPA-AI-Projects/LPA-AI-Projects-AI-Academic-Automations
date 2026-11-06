# ── Stage 1: build the cost dashboard (served by the API at /cost-dashboard) ──
FROM node:22-slim AS dashboard-build
WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
# A dashboard build failure must not block deploying the API: fall back to an empty dist (not mounted).
RUN npm run build || (echo "WARNING: dashboard build failed; /cost-dashboard will be unavailable" && rm -rf dist && mkdir -p dist)

# ── Stage 2: API ──
FROM python:3.12-slim

# System dependencies for Playwright Chromium
RUN apt-get update && apt-get install -y \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY . .
COPY --from=dashboard-build /dashboard/dist /app/dashboard/dist

RUN mkdir -p storage/pdfs storage/ppts storage/uploads/slides

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
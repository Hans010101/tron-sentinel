# ──────────────────────────────────────────────────────────────────────────────
# TRON Sentinel – Docker image
#
# Uses requirements-prod.txt (NOT the full requirements.txt) to keep the
# image lean.  The full requirements.txt contains heavy ML packages
# (torch, transformers) that are not needed at runtime because we use
# VADER for sentiment analysis, not Hugging-Face transformer models.
# Final compressed image size is roughly 300-400 MB.
#
# Entry point:  entrypoint.py
#   • Starts a minimal HTTP health-check server on $PORT (required by
#     Cloud Run) in a daemon thread.
#   • Calls scheduler.main() in the foreground, which executes the full
#     data pipeline every 30 minutes and writes dashboard/data.json to
#     Google Cloud Storage (when GCS_BUCKET env var is set).
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# ── System layer ───────────────────────────────────────────────────────────────
# gcc is needed to compile the C extension in some transitive dependencies
# (e.g. pyOpenSSL / cryptography used by Telethon).
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libssl-dev \
 && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements-prod.txt ./
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements-prod.txt

# ── Application code ───────────────────────────────────────────────────────────
COPY . .

# Ensure the data directory exists (SQLite DB + Telegram session live here).
# Cloud Run containers have an ephemeral writable filesystem; for true
# persistence mount a Cloud Filestore volume or use Cloud Storage.
RUN mkdir -p /app/data /app/dashboard

# ── Runtime ────────────────────────────────────────────────────────────────────
# Cloud Run injects the PORT env var (default 8080).
# entrypoint.py serves health checks on that port and runs the scheduler.
ENV PORT=8080

CMD ["python", "entrypoint.py"]

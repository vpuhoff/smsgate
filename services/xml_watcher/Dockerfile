# ------------------------------------------------------------------------------
# API-Gateway image
# ------------------------------------------------------------------------------
# • installs dependencies via **uv** for faster, repeatable builds
# • exposes FastAPI on port 8000 and Prometheus metrics on 9101
# ------------------------------------------------------------------------------
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \ 
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# --- install UV package manager ------------------------------------------------
RUN apt-get update && apt-get install -y curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get purge -y --auto-remove curl && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:$PATH"

# --- copy project --------------------------------------------------------------
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir ngrok

COPY libs /app/libs
COPY services/xml_watcher/*.py /app

WORKDIR /app


# --- entrypoint ---------------------------------------------------------------
CMD ["python3", "watcher.py"]

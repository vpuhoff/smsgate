
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \ 
    PYTHONDONTWRITEBYTECODE=1

ENV TZ Asia/Yerevan
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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
COPY services/parser_worker/*.py /app

WORKDIR /app

# --- default ports -------------------------------------------------------------
EXPOSE $PARSER_METRICS_PORT

# --- entrypoint ---------------------------------------------------------------
CMD ["python3", "worker.py"]

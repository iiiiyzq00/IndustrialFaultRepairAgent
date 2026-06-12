FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn==0.34.0 \
    httpx==0.28.1 \
    pydantic==2.10.4 \
    pydantic-settings==2.7.1

# Copy common auth/monitoring modules
COPY services/common /app/services/common

# Copy sandbox service
COPY services/sandbox-service/app /app/app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${SERVICE_PORT:-8500}/health || exit 1

EXPOSE 8500

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${SERVICE_PORT:-8500}"]

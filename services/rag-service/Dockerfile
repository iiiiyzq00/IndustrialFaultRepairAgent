FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY services/rag-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/rag-service/app/ /app/app/
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-large-zh-v1.5', device='cpu')" || true
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 8200
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8200"]

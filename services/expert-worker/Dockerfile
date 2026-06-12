FROM python:3.10-slim
WORKDIR /app
COPY services/expert-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/expert-worker/app/ /app/app/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 8110
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8110"]

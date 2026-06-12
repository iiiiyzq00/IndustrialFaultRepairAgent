FROM python:3.10-slim
WORKDIR /app
COPY services/hitl-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/hitl-gateway/app/ /app/app/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 8300
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8300"]

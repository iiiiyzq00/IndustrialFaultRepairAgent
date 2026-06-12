FROM python:3.11-slim
WORKDIR /app
COPY services/agent-supervisor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/agent-supervisor/app/ /app/app/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 8100
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]

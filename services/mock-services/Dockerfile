FROM python:3.10-slim
WORKDIR /app
COPY services/mock-services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/mock-services/app/ /app/app/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 9002
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9002"]

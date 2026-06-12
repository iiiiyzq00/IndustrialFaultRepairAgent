FROM python:3.10-slim
WORKDIR /app
COPY services/fake-data-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/fake-data-generator/app/ /app/app/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/services
EXPOSE 9005
CMD ["python", "-u", "-m", "app.main"]

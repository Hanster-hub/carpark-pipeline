FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir requests kafka-python
COPY common ./common
COPY producer ./producer
CMD ["python", "-m", "producer.producer"]

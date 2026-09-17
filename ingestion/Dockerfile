FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    requests \
    confluent-kafka \
    python-dotenv

# Copy code
COPY ingestion/ /app/ingestion/

CMD ["python", "ingestion/producer.py"]
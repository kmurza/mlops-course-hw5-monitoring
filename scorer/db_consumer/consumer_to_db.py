import os
import json
import time
import logging
from threading import Thread

import psycopg2
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

db_records_total = Counter(
    "db_records_total",
    "Total number of score records written to PostgreSQL",
)
db_write_seconds = Histogram(
    "db_write_seconds",
    "Time spent writing one record to PostgreSQL",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
db_fraud_total = Counter(
    "db_fraud_total",
    "Total number of fraud records written to PostgreSQL",
)
db_score_histogram = Histogram(
    "db_score",
    "Distribution of fraud scores written to PostgreSQL",
    buckets=(0.0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0),
)


def get_db_connection():
    for attempt in range(10):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
            )
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("DB connection attempt %d/10 failed: %s", attempt + 1, e)
            time.sleep(3)
    raise Exception("Could not connect to PostgreSQL after 10 attempts")


def main():
    logger.info("Starting Kafka -> PostgreSQL consumer...")

    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "db-consumer-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SCORING_TOPIC])
    logger.info("Subscribed to topic: %s", SCORING_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Kafka error: %s", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))

            if isinstance(data, list):
                if len(data) == 0:
                    logger.warning("Empty list in message, skipping")
                    continue
                data = data[0]

            transaction_id = data.get("transaction_id")
            score = data.get("score")
            fraud_flag = data.get("fraud_flag")
            model_version = data.get("model_version")
            us_state = data.get("us_state")
            merch = data.get("merch")
            cat_id = data.get("cat_id")
            amount = data.get("amount")

            if transaction_id is None or score is None or fraud_flag is None:
                logger.warning("Skipping incomplete message: %s", data)
                continue

            started = time.time()
            cursor.execute(
                """
                INSERT INTO scores (transaction_id, score, fraud_flag, model_version,
                                    us_state, merch, cat_id, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (transaction_id, float(score), int(fraud_flag), model_version,
                 us_state, merch, cat_id, amount),
            )
            db_write_seconds.observe(time.time() - started)

            db_records_total.inc()
            db_score_histogram.observe(float(score))
            if int(fraud_flag) == 1:
                db_fraud_total.inc()

            logger.info(
                "Inserted transaction_id=%s, score=%.4f, fraud_flag=%d, version=%s",
                transaction_id, score, fraud_flag, model_version,
            )

        except Exception as e:
            logger.error("Error processing message: %s", e)


if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics server started on port %d", METRICS_PORT)

    main()
import os
import sys
import json
import time
import logging
from threading import Thread

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/service.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

sys.path.append(os.path.abspath("./src"))
from preprocessing import run_preproc
from scorer import make_pred

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

transactions_total = Counter(
    "scoring_transactions_total",
    "Total number of transactions processed by the scoring service",
)
processing_seconds = Histogram(
    "scoring_processing_seconds",
    "Time spent processing one transaction",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
score_histogram = Histogram(
    "scoring_score",
    "Distribution of fraud scores",
    buckets=(0.0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0),
)
fraud_total = Counter(
    "scoring_fraud_total",
    "Total number of transactions flagged as fraud",
)


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ml-scorer",
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    def process_messages(self) -> None:
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                transaction_id = data["transaction_id"]
                raw = data["data"]
                input_df = pd.DataFrame([raw])

                started = time.time()
                processed_df = run_preproc(input_df)
                submission = make_pred(processed_df, "kafka_stream")
                processing_seconds.observe(time.time() - started)

                result_dict = submission.iloc[0].to_dict()
                result_dict["transaction_id"] = transaction_id

                for field in ("us_state", "merch", "cat_id", "amount"):
                    if field in raw:
                        result_dict[field] = raw[field]

                transactions_total.inc()
                score_value = float(result_dict["score"])
                score_histogram.observe(score_value)
                if int(result_dict["fraud_flag"]) == 1:
                    fraud_total.inc()

                self.producer.produce(
                    SCORING_TOPIC,
                    value=json.dumps(result_dict).encode("utf-8"),
                )
                self.producer.flush()

            except Exception as e:
                logger.error("Error processing message: %s", e)


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics server started on port %d", METRICS_PORT)
    
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
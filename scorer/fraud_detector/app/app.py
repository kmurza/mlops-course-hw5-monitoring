import os
import sys
import json
import logging

import pandas as pd
from confluent_kafka import Consumer, Producer

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
                input_df = pd.DataFrame([data["data"]])

                processed_df = run_preproc(input_df)
                submission = make_pred(processed_df, "kafka_stream")
                
                result_dict = submission.iloc[0].to_dict()
                result_dict["transaction_id"] = transaction_id

                self.producer.produce(
                    SCORING_TOPIC,
                    value=json.dumps(result_dict).encode("utf-8"),
                )
                self.producer.flush()

            except Exception as e:
                logger.error("Error processing message: %s", e)


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
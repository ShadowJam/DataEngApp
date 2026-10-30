import os
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use external address for local execution
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC_NAME = "currency_rates"
CURRENCIES = ["EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "CNY"]

def generate_event():
    """Generate a single currency rate event"""
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "currency": random.choice(CURRENCIES),
        "rate": round(random.uniform(0.5, 150.0), 4)
    }

def main():
    # Install dependency locally: pip install kafka-python
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"Producer started. Sending messages to topic '{TOPIC_NAME}'...")
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            logger.info(f"Sent: {event}")
            time.sleep(2) # Send an event every 2 seconds
    except KeyboardInterrupt:
        logger.info("Stopping producer.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
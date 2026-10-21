
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "payments.raw"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "payment-producer",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
})


def on_delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered: partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


def generate_payment():
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC-{random.randint(1, 10):03d}",
        "merchant_id": f"MER-{random.randint(1, 5):03d}",
        "amount_minor": random.randint(100, 100000),
        "currency": "KZT",
        "status": "APPROVED",
        "event_time": datetime.now(
            timezone.utc
        ).isoformat(),
    }


for _ in range(100):
    event = generate_payment()

    producer.produce(
        topic=TOPIC,
        key=event["account_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=on_delivery,
    )

    producer.poll(0)
    time.sleep(0.1)

remaining = producer.flush(timeout=30)

if remaining:
    raise RuntimeError(
        f"{remaining} messages remained undelivered"
    )

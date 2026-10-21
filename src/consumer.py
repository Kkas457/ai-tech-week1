import json

from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "payment-processing-service",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe(["payments.raw"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        event = json.loads(msg.value())

        import time
        time.sleep(0.5)

        print(
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()} "
            f"Account={event['account_id']} "
            f"Amount={event['amount_minor']} "
            f"{event['currency']}"
        )

        # A later milestone will add real
        # validation and durable database writes.
        #
        # Commit only after successful processing.
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    print("Consumer shutting down")

finally:
    consumer.close()

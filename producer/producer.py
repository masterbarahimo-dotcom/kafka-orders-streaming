"""Send simulated shop orders to Kafka topic 'orders'."""
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "orders"
ORDERS = [
    {"order_id": 101, "product": "Laptop", "quantity": 1, "price": 6500},
    {"order_id": 102, "product": "Mouse", "quantity": 2, "price": 150},
    {"order_id": 103, "product": "Keyboard", "quantity": 1, "price": 400},
    {"order_id": 104, "product": "Mouse", "quantity": 3, "price": 150},
    {"order_id": 105, "product": "Laptop", "quantity": 1, "price": 7200},
    {"order_id": 106, "product": "Headphones", "quantity": 2, "price": 350},
]


def delivery_report(error, message):
    if error is not None:
        print(f"FAILED to deliver: {error}")
    else:
        print(f"DELIVERED: partition={message.partition()} offset={message.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    for order in ORDERS:
        event = {**order, "created_at": datetime.now(timezone.utc).isoformat()}
        print(f"SENDING: {event}")
        producer.produce(
            TOPIC,
            key=str(order["order_id"]),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(1)
    outstanding = producer.flush(15)
    if outstanding:
        raise RuntimeError(f"{outstanding} message(s) not delivered; check Kafka logs")
    print("Producer finished. All orders acknowledged.")


if __name__ == "__main__":
    main()

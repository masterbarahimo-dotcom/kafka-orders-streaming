"""Read shop orders from Kafka and append them to a local CSV file."""
import csv
import json
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

TOPIC = "orders"
OUTPUT = Path(__file__).resolve().parents[1] / "orders_received.csv"
FIELDS = ["order_id", "product", "quantity", "price", "total", "created_at"]


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "orders-csv-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print("Listening for orders... Ctrl+C to stop")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            order = json.loads(message.value().decode("utf-8"))
            record = {
                "order_id": order["order_id"],
                "product": order["product"],
                "quantity": order["quantity"],
                "price": order["price"],
                "total": order["quantity"] * order["price"],
                "created_at": order["created_at"],
            }
            # Commit only after writing locally. A crash between write and commit
            # can duplicate a row; production pipelines need idempotent sinks.
            is_new = not OUTPUT.exists() or OUTPUT.stat().st_size == 0
            with OUTPUT.open("a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=FIELDS)
                if is_new:
                    writer.writeheader()
                writer.writerow(record)
                file.flush()
            consumer.commit(message=message, asynchronous=False)
            print(f"RECEIVED: order #{record['order_id']} | {record['product']} | total={record['total']} MAD")
    except KeyboardInterrupt:
        print("\nStopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

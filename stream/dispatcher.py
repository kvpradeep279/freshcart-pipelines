
import json
import random
import time
from datetime import datetime, timezone

import pika

EXCHANGE = "rapido-dispatch"
INTERVAL = 2

CITIES = ["bangalore", "chennai"]
ZONES = ["koramangala", "indiranagar", "whitefield", "tnagar"]
TYPES = ["ride", "food"]


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True
    )

    print(f"[Dispatcher] Connected. Publishing to {EXCHANGE}")

    request_id = 0

    # Known D1 routing example
    test_key = "bangalore.koramangala.food"
    request_id += 1

    test_message = {
        "id": request_id,
        "city": "bangalore",
        "zone": "koramangala",
        "type": "food",
        "ts": datetime.now(timezone.utc).isoformat()
    }

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=test_key,
        body=json.dumps(test_message).encode(),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2
        )
    )

    print(f"[Dispatcher] TEST #{request_id}: {test_key}")

    try:
        while True:
            time.sleep(INTERVAL)

            city = random.choice(CITIES)
            zone = random.choice(ZONES)
            request_type = random.choice(TYPES)

            routing_key = f"{city}.{zone}.{request_type}"

            request_id += 1

            message = {
                "id": request_id,
                "city": city,
                "type": request_type,
                "ts": datetime.now(timezone.utc).isoformat()
            }

            # Every 10th message intentionally misses the zone field
            if request_id % 10 != 0:
                message["zone"] = zone
            else:
                print(
                    f"[Dispatcher] MALFORMED #{request_id}: "
                    "zone field missing"
                )

            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2
                )
            )

            if request_id % 10 != 0:
                print(
                    f"[Dispatcher] Published #{request_id}: "
                    f"{routing_key}"
                )

    except KeyboardInterrupt:
        print("\n[Dispatcher] Stopping.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()

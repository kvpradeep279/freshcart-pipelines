
import argparse
import json
import pika

EXCHANGE = "rapido-dispatch"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("pattern")
    args = parser.parse_args()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True
    )

    result = channel.queue_declare(
        queue="",
        exclusive=True,
        auto_delete=True
    )

    queue_name = result.method.queue

    channel.queue_bind(
        exchange=EXCHANGE,
        queue=queue_name,
        routing_key=args.pattern
    )

    channel.basic_qos(prefetch_count=1)

    print(f"[{args.name}] Listening with pattern: {args.pattern}")
    print(f"[{args.name}] Private queue: {queue_name}")

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))

            if "zone" not in message:
                print(
                    f"[{args.name}] REJECTED: "
                    f"Missing zone | message={message}"
                )

                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
                return

            print(
                f"[{args.name}] RECEIVED: "
                f"{method.routing_key} | message={message}"
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

            print(
                f"[{args.name}] ACKED message ID: "
                f"{message.get('id')}"
            )

        except Exception as error:
            print(f"[{args.name}] Processing error: {error}")

            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False
    )

    try:
        channel.start_consuming()

    except KeyboardInterrupt:
        print(f"\n[{args.name}] Stopping.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()

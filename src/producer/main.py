"""CLI entry point for generating bounded customer-event traffic."""

import argparse

from src.producer.event_generator import generate_events
from src.producer.kafka_producer import EventProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="customer-events")
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()

    producer = EventProducer(args.bootstrap_servers)
    for event in generate_events(args.count):
        producer.publish(args.topic, event)
    remaining = producer.flush()
    if remaining:
        raise RuntimeError(f"{remaining} Kafka messages remained undelivered")


if __name__ == "__main__":
    main()

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from models.schemas import make_event, validate_event
from workers.processor import EVENTS_TOPIC

load_dotenv()

SYNTHETIC_PHRASES = [
    "this phone camera is really impressive and the battery lasts all day",
    "terrible update broke everything and support is not responding",
    "the match was average, nothing special about the team today",
    "budget announcement will hurt savings and prices keep going up",
    "bahut badhiya service mila, delivery bhi time par aayi",
    "the new software release feels slow and the interface is confusing",
    "great tournament, the captain played an amazing innings",
]


def stored_to_message(event):
    keywords = event.get("keywords") or []
    message = {key: value for key, value in event.items() if key not in ("analysis", "keywords", "_id")}
    if keywords:
        message["keyword"] = keywords[0]
    return message


def synthetic_events(count, seed=1):
    rng = random.Random(seed)
    events = []
    for index in range(count):
        phrase = rng.choice(SYNTHETIC_PHRASES)
        event = make_event(
            "telegram",
            "message",
            f"synthetic_channel_{index % 5}",
            f"{phrase} #{rng.randint(0, 10**9)}",
            author_id=f"synthetic_author_{rng.randint(0, 200)}",
            timestamp=None,
            keyword="synthetic",
        )
        if event:
            events.append(event)
    return events


def load_json_events(path):
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, list) else [data]


def publish(events, broker, topic=EVENTS_TOPIC):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )
    sent = 0
    skipped = 0
    for event in events:
        if not validate_event(event):
            skipped += 1
            continue
        producer.send(topic, key=event["event_id"], value=event)
        sent += 1
    producer.flush()
    producer.close()
    return sent, skipped


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--synthetic", type=int, help="publish this many generated test events")
    source.add_argument("--from-json", help="publish normalized events from a JSON file")
    source.add_argument("--from-mongo", action="store_true", help="replay stored events")
    parser.add_argument("--keyword", default=None)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    if args.synthetic:
        events = synthetic_events(args.synthetic)
    elif args.from_json:
        events = load_json_events(args.from_json)
    else:
        from storage.mongodb import fetch_events

        events = [stored_to_message(event) for event in fetch_events(keyword=args.keyword, limit=args.limit)]

    sent, skipped = publish(events, args.broker)
    print(f"Published {sent} events to {EVENTS_TOPIC}, skipped {skipped} invalid.")


if __name__ == "__main__":
    main()
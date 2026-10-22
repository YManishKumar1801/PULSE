import argparse
import json
import os
import signal
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from workers.processor import DEAD_LETTER_TOPIC, EVENTS_TOPIC, EventWorker

load_dotenv()


def build_worker(args):
    from kafka import KafkaConsumer, KafkaProducer

    from intelligence.sentiment import analyze_events
    from storage.mongodb import fetch_analyzed_ids, save_analyses, save_events, save_worker_status

    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=args.broker,
        group_id=args.group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=args.batch_size,
    )
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    worker = EventWorker(
        consumer=consumer,
        dead_letter_producer=producer,
        analyze=analyze_events,
        save_events=save_events,
        save_analyses=save_analyses,
        fetch_analyzed_ids=fetch_analyzed_ids,
        report_status=save_worker_status,
        batch_size=args.batch_size,
        worker_id=args.worker_id,
    )
    return worker, consumer, producer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--group", default="pulse-processor")
    parser.add_argument("--worker-id", default="worker-1")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    worker, consumer, producer = build_worker(args)
    print(f"Worker {args.worker_id} listening on {EVENTS_TOPIC} (group {args.group}). Dead letters go to {DEAD_LETTER_TOPIC}. Press Ctrl+C to stop.")
    try:
        worker.run(should_stop=stop.is_set, max_batches=args.max_batches)
    finally:
        print(json.dumps(worker.snapshot(), indent=2))
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
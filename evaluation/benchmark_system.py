import argparse
import json
import os
import platform
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.metrics import percentile

DEFAULT_OUT = os.path.join("evaluation", "results", "system_benchmark.json")
BENCHMARK_COLLECTION = "pulse_benchmark"
CHUNK_SIZE = 32


def timed_chunks(items, chunk_size, function):
    durations = []
    for start in range(0, len(items), chunk_size):
        chunk = items[start:start + chunk_size]
        begin = time.perf_counter()
        function(chunk)
        durations.append((time.perf_counter() - begin, len(chunk)))
    return durations


def summarize_chunks(durations):
    if not durations:
        return None
    total_seconds = sum(seconds for seconds, _ in durations)
    total_items = sum(count for _, count in durations)
    per_chunk = [seconds for seconds, _ in durations]
    return {
        "items": total_items,
        "chunks": len(durations),
        "total_seconds": round(total_seconds, 3),
        "items_per_second": round(total_items / total_seconds, 2) if total_seconds else None,
        "chunk_latency_avg_seconds": round(sum(per_chunk) / len(per_chunk), 3),
        "chunk_latency_p95_seconds": round(percentile(per_chunk, 95), 3),
    }


def environment():
    info = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        import torch

        info["torch"] = torch.__version__
        info["cuda_available"] = bool(torch.cuda.is_available())
    except Exception:
        info["torch"] = None
    return info


def benchmark_sentiment(events, limit):
    from intelligence.sentiment import analyze_events

    texts = [event for event in events if (event.get("text") or "").strip()][:limit]
    if not texts:
        return {"status": "no_events"}
    analyze_events(texts[:2])
    durations = timed_chunks(texts, CHUNK_SIZE, analyze_events)
    summary = summarize_chunks(durations)
    summary["chunk_size"] = CHUNK_SIZE
    summary["status"] = "ok"
    summary["note"] = "Models are warmed up first. Measures sentiment, emotion and sarcasm together. Database writes excluded."
    return summary


def benchmark_pipeline(events):
    from intelligence.signals import run_pipeline

    begin = time.perf_counter()
    result = run_pipeline(events)
    seconds = time.perf_counter() - begin
    return {
        "status": "ok",
        "events": len(events),
        "seconds": round(seconds, 3),
        "pipeline_status": result["status"],
        "signals": len(result["signals"]),
    }


def benchmark_mongo_writes(count):
    from pymongo import UpdateOne

    from storage.mongodb import DB_NAME, get_client

    collection = get_client()[DB_NAME][BENCHMARK_COLLECTION]
    run_id = uuid.uuid4().hex[:8]
    operations = [
        UpdateOne(
            {"event_id": f"bench_{run_id}_{index}"},
            {"$set": {"event_id": f"bench_{run_id}_{index}", "text": f"benchmark message {index}", "platform": "benchmark"}},
            upsert=True,
        )
        for index in range(count)
    ]
    try:
        durations = []
        for start in range(0, len(operations), 500):
            batch = operations[start:start + 500]
            begin = time.perf_counter()
            collection.bulk_write(batch, ordered=False)
            durations.append((time.perf_counter() - begin, len(batch)))
        summary = summarize_chunks(durations)
        summary["status"] = "ok"
        summary["batch_size"] = 500
        summary["note"] = "Upserts into a temporary collection that is dropped afterwards. Includes network round trips to MongoDB Atlas."
        return summary
    finally:
        collection.drop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sentiment-events", type=int, default=128)
    parser.add_argument("--mongo-writes", type=int, default=2000)
    parser.add_argument("--skip-sentiment", action="store_true")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    from storage.mongodb import fetch_events

    report = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "environment": environment(),
        "not_measured": [
            "Kafka consumer lag: the current app does not run a continuously consuming worker, so there is no lag to measure.",
            "End-to-end latency from a live source to the dashboard.",
        ],
    }
    events = fetch_events(limit=20000)
    report["stored_events"] = len(events)

    print(f"Stored events: {len(events)}")
    report["pipeline"] = benchmark_pipeline(events)
    print("Pipeline:", report["pipeline"])

    if not args.skip_sentiment:
        report["sentiment_inference"] = benchmark_sentiment(events, args.sentiment_events)
        print("Sentiment inference:", report["sentiment_inference"])

    if not args.skip_mongo:
        try:
            report["mongo_writes"] = benchmark_mongo_writes(args.mongo_writes)
        except Exception as error:
            report["mongo_writes"] = {"status": "failed", "error": str(error)}
        print("MongoDB writes:", report["mongo_writes"])

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    print(f"Report saved to {args.out}")
    print("These numbers describe this machine and this network only. Quote them together with the environment block.")


if __name__ == "__main__":
    main()
import argparse
from collections import Counter

from intelligence.sentiment import analyze_events
from storage.mongodb import count_unanalyzed_events, fetch_unanalyzed_events, save_analyses

CHUNK_SIZE = 64


def run(keyword, platform, limit):
    pending = count_unanalyzed_events(keyword, platform)
    print(f"Pending events: {pending}")
    if pending == 0:
        return

    processed = 0
    distribution = Counter()
    target = min(limit, pending)

    while processed < target:
        take = min(CHUNK_SIZE, target - processed)
        events = fetch_unanalyzed_events(keyword, platform, limit=take)
        if not events:
            break
        results = analyze_events(events)
        save_analyses(results)
        for _, analysis in results:
            distribution[analysis["sentiment"]["label"]] += 1
        processed += len(events)
        print(f"Analyzed {processed}/{target}")

    print("Sentiment distribution:", dict(distribution))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", default=None)
    parser.add_argument("--platform", default=None, choices=["youtube", "news", "telegram"])
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    run(args.keyword, args.platform, args.limit)
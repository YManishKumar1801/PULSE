import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.labeling import build_sample, write_sheet
from storage.mongodb import fetch_events

DEFAULT_OUT = os.path.join("evaluation", "data", "labeling_sheet.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    events = fetch_events(limit=50000)
    analyzed = [event for event in events if event.get("analysis")]
    print(f"Stored events: {len(events)}, analyzed: {len(analyzed)}")
    sample = build_sample(analyzed, args.size, args.seed)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    write_sheet(sample, args.out)
    print(f"Wrote {len(sample)} rows to {args.out}")
    print("Fill the human_label column with Positive, Neutral or Negative.")
    print("Optionally fill annotated_language (for example Hinglish) to evaluate by your own language label.")


if __name__ == "__main__":
    main()
import argparse
import json
import os
import random
import statistics
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence.signals import run_pipeline

BASE = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
FLAGGED_STATUSES = ("Emerging", "Rising")
CHECKPOINT_STEP_HOURS = 6
CHECKPOINT_COUNT = 7
PLANT_HOUR = 108
BACKGROUND_HOURS = 144
MIN_TERM_OVERLAP = 2

BACKGROUND_VOCAB = {
    "cricket": ["match", "team", "player", "score", "tournament", "wicket", "captain", "bowling", "innings", "stadium"],
    "finance": ["market", "stock", "price", "budget", "inflation", "rupee", "investment", "economy", "interest", "savings"],
    "weather": ["rain", "monsoon", "forecast", "humidity", "storm", "temperature", "cloud", "winter", "breeze", "drizzle"],
}
PLANTED_VOCAB = ["layoffs", "automation", "engineers", "hiring", "startup", "salary", "agents", "models", "jobs", "tech"]
DEFAULT_OUT = os.path.join("evaluation", "results", "trend_benchmark.json")


def _event(counter, words, hour, rng):
    text = " ".join(rng.sample(words, 5)) + " really interesting discussion"
    stamp = (BASE + timedelta(hours=hour)).replace(microsecond=0).isoformat()
    return {
        "event_id": f"b{counter}",
        "platform": "youtube",
        "text": text,
        "timestamp": stamp,
        "engagement": {},
        "keywords": ["benchmark"],
    }


def make_scenario(seed, planted):
    rng = random.Random(seed)
    events = []
    counter = 0
    for words in BACKGROUND_VOCAB.values():
        hour = rng.uniform(0, 1)
        while hour < BACKGROUND_HOURS:
            events.append(_event(counter, words, hour, rng))
            counter += 1
            hour += rng.expovariate(1 / 0.6)
    if planted:
        hour = PLANT_HOUR
        while hour < BACKGROUND_HOURS:
            progress = (hour - PLANT_HOUR) / (BACKGROUND_HOURS - PLANT_HOUR)
            rate_per_hour = 1.0 + 3.0 * progress
            events.append(_event(counter, PLANTED_VOCAB, hour, rng))
            counter += 1
            hour += rng.expovariate(rate_per_hour)
    return events


def _event_hour(event):
    return (datetime.fromisoformat(event["timestamp"]) - BASE).total_seconds() / 3600


def _count_flags(topics):
    planted_flagged = False
    false_flags = 0
    total_flags = 0
    for topic in topics:
        total_flags += 1
        overlap = len(set(topic["terms"]) & set(PLANTED_VOCAB))
        if overlap >= MIN_TERM_OVERLAP:
            planted_flagged = True
        else:
            false_flags += 1
    return {"planted_flagged": planted_flagged, "false_flags": false_flags, "total_flags": total_flags}


def evaluate_checkpoint(events, checkpoint_hour):
    visible = [event for event in events if _event_hour(event) <= checkpoint_hour]
    result = run_pipeline(visible)
    if result["status"] != "ok":
        empty = {"planted_flagged": False, "false_flags": 0, "total_flags": 0}
        return {"status": result["status"], "trend": dict(empty), "signal": dict(empty)}
    trend_flagged = [topic for topic in result["fused_topics"] if topic["status"] in FLAGGED_STATUSES]
    return {
        "status": "ok",
        "trend": _count_flags(trend_flagged),
        "signal": _count_flags(result["signals"]),
    }


def run_scenario(seed, planted):
    events = make_scenario(seed, planted)
    checkpoints = []
    first_detection = None
    first_signal = None
    for step in range(CHECKPOINT_COUNT):
        hour = PLANT_HOUR + step * CHECKPOINT_STEP_HOURS
        outcome = evaluate_checkpoint(events, hour)
        outcome["hour_after_plant"] = step * CHECKPOINT_STEP_HOURS
        checkpoints.append(outcome)
        if planted and first_detection is None and outcome["trend"]["planted_flagged"]:
            first_detection = step * CHECKPOINT_STEP_HOURS
        if planted and first_signal is None and outcome["signal"]["planted_flagged"]:
            first_signal = step * CHECKPOINT_STEP_HOURS
    return {
        "seed": seed,
        "planted": planted,
        "events": len(events),
        "first_detection_hours": first_detection,
        "first_signal_hours": first_signal,
        "checkpoints": checkpoints,
    }


def _level_summary(planted_runs, control_runs, level, latency_key):
    detected = [run[latency_key] for run in planted_runs if run[latency_key] is not None]
    planted_checkpoints = [c[level] for run in planted_runs for c in run["checkpoints"] if c["status"] == "ok"]
    control_checkpoints = [c[level] for run in control_runs for c in run["checkpoints"] if c["status"] == "ok"]
    everything = planted_checkpoints + control_checkpoints
    true_flags = sum(1 for c in planted_checkpoints if c["planted_flagged"])
    all_flags = sum(c["total_flags"] for c in everything)
    false_flags = sum(c["false_flags"] for c in everything)
    control_false = sum(c["false_flags"] for c in control_checkpoints)
    return {
        "detection_rate": round(len(detected) / len(planted_runs), 3) if planted_runs else None,
        "median_detection_latency_hours": statistics.median(detected) if detected else None,
        "flags_total": all_flags,
        "flags_on_planted_topic": true_flags,
        "false_flags": false_flags,
        "precision_of_flags": round(true_flags / all_flags, 3) if all_flags else None,
        "false_flags_per_control_checkpoint": round(control_false / len(control_checkpoints), 3) if control_checkpoints else None,
    }


def summarize(planted_runs, control_runs):
    return {
        "planted_scenarios": len(planted_runs),
        "control_scenarios": len(control_runs),
        "latency_resolution_hours": CHECKPOINT_STEP_HOURS,
        "trend_detector": _level_summary(planted_runs, control_runs, "trend", "first_detection_hours"),
        "pulse_signals": _level_summary(planted_runs, control_runs, "signal", "first_signal_hours"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenarios", type=int, default=10)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    planted_runs = [run_scenario(seed, True) for seed in range(1, args.scenarios + 1)]
    control_runs = [run_scenario(seed, False) for seed in range(1001, 1001 + args.scenarios)]
    summary = summarize(planted_runs, control_runs)

    notes = [
        "Synthetic benchmark: one topic is planted into simulated background chatter and the detector is asked to find it.",
        "It measures whether the trend detector reacts to a known surge. It does not measure accuracy on real-world data.",
        "A flag counts as correct only if the flagged topic shares at least two terms with the planted vocabulary.",
    ]
    output = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "summary": summary,
        "notes": notes,
        "runs": {"planted": planted_runs, "control": control_runs},
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2)
    print(f"planted scenarios: {summary['planted_scenarios']}, control scenarios: {summary['control_scenarios']}, latency resolution: {summary['latency_resolution_hours']}h")
    for level in ("trend_detector", "pulse_signals"):
        print(f"\n{level}:")
        for key, value in summary[level].items():
            print(f"  {key}: {value}")
    print()
    for note in notes:
        print("NOTE:", note)
    print(f"Full report saved to {args.out}")


if __name__ == "__main__":
    main()
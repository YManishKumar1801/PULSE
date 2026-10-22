import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.labeling import read_labelled_sheet
from evaluation.metrics import LABELS, classification_report, group_reports

DEFAULT_LABELS = os.path.join("evaluation", "data", "labeling_sheet.csv")
DEFAULT_REPORT = os.path.join("evaluation", "results", "sentiment_report.json")
MODEL_KEYS = ("vader", "distilbert", "xlm_roberta")
SMALL_SAMPLE = 100


def join_predictions(labelled, events):
    by_id = {event["event_id"]: event for event in events}
    rows = []
    missing = 0
    unanalyzed = 0
    for item in labelled:
        event = by_id.get(item["event_id"])
        if event is None:
            missing += 1
            continue
        analysis = event.get("analysis")
        if not analysis or not analysis.get("sentiment"):
            unanalyzed += 1
            continue
        sentiment = analysis["sentiment"]
        models = sentiment.get("models") or {}
        row = {
            "event_id": item["event_id"],
            "true": item["true"],
            "pred": sentiment["label"],
            "platform": event.get("platform") or "unknown",
            "language": (analysis.get("language") or {}).get("name") or "Unknown",
            "annotated_language": item["annotated_language"] or None,
            "models": {key: models[key]["label"] for key in MODEL_KEYS if key in models},
        }
        rows.append(row)
    return rows, {"missing_events": missing, "unanalyzed": unanalyzed}


def evaluate(rows):
    report = {
        "ensemble": classification_report([r["true"] for r in rows], [r["pred"] for r in rows]),
        "individual_models": {},
        "by_detected_language": group_reports(rows, "language"),
        "by_platform": group_reports(rows, "platform"),
    }
    for key in MODEL_KEYS:
        subset = [r for r in rows if key in r["models"]]
        if subset:
            report["individual_models"][key] = classification_report(
                [r["true"] for r in subset], [r["models"][key] for r in subset]
            )
    annotated = [dict(r, annotated=r["annotated_language"]) for r in rows if r["annotated_language"]]
    if annotated:
        report["by_annotated_language"] = group_reports(annotated, "annotated")
    return report


def label_distribution(rows):
    return {label: sum(1 for r in rows if r["true"] == label) for label in LABELS}


def print_summary(report, distribution, notes):
    ensemble = report["ensemble"]
    print(f"\nLabelled messages evaluated: {ensemble['n']}")
    print("Human label distribution:", distribution)
    print(f"Ensemble accuracy: {ensemble['accuracy']:.3f}  macro F1: {ensemble['macro_f1']:.3f}")
    for label in LABELS:
        stats = ensemble["per_class"][label]
        print(f"  {label:<9} precision {stats['precision']:.3f}  recall {stats['recall']:.3f}  f1 {stats['f1']:.3f}  support {stats['support']}")
    print("Confusion matrix (rows = human label, columns = prediction):")
    print("            " + "  ".join(f"{label[:8]:>8}" for label in LABELS))
    for label, row in zip(LABELS, ensemble["confusion_matrix"]):
        print(f"  {label:<9} " + "  ".join(f"{value:>8}" for value in row))
    print("\nIndividual models (accuracy / macro F1):")
    for key, stats in report["individual_models"].items():
        print(f"  {key:<12} {stats['accuracy']:.3f} / {stats['macro_f1']:.3f}")
    for title, groups in (("By detected language", report["by_detected_language"]), ("By platform", report["by_platform"])):
        print(f"\n{title}:")
        for name, stats in groups.items():
            flag = "" if stats["reliable"] else "  (too few messages, not reliable)"
            print(f"  {name:<14} n {stats['n']:<5} accuracy {stats['accuracy']:.3f}  macro F1 {stats['macro_f1']:.3f}{flag}")
    if "by_annotated_language" in report:
        print("\nBy annotated language:")
        for name, stats in report["by_annotated_language"].items():
            flag = "" if stats["reliable"] else "  (too few messages, not reliable)"
            print(f"  {name:<14} n {stats['n']:<5} accuracy {stats['accuracy']:.3f}  macro F1 {stats['macro_f1']:.3f}{flag}")
    for note in notes:
        print("NOTE:", note)


def main():
    from storage.mongodb import fetch_events

    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", default=DEFAULT_LABELS)
    parser.add_argument("--out", default=DEFAULT_REPORT)
    args = parser.parse_args()

    labelled, sheet_info = read_labelled_sheet(args.labels)
    if not labelled:
        print("No labelled rows found. Fill the human_label column first.")
        return
    events = fetch_events(limit=50000)
    rows, join_info = join_predictions(labelled, events)
    if not rows:
        print("None of the labelled messages could be matched to analyzed events.")
        return

    report = evaluate(rows)
    distribution = label_distribution(rows)
    notes = []
    if len(rows) < SMALL_SAMPLE:
        notes.append(f"Only {len(rows)} labelled messages. Treat these numbers as indicative, not conclusive.")
    if sheet_info["invalid"]:
        notes.append(f"{len(sheet_info['invalid'])} rows had an invalid label and were skipped.")
    if sheet_info["skipped_blank"]:
        notes.append(f"{sheet_info['skipped_blank']} rows were left unlabelled and skipped.")
    if join_info["missing_events"] or join_info["unanalyzed"]:
        notes.append(f"Skipped {join_info['missing_events']} missing and {join_info['unanalyzed']} unanalyzed events.")
    notes.append("Messages are sampled from collected data and labelled by the project team, so results describe this data only.")

    output = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "labels_file": args.labels,
        "label_distribution": distribution,
        "sheet": {"rows": sheet_info["rows"], "skipped_blank": sheet_info["skipped_blank"], "invalid": len(sheet_info["invalid"])},
        "join": join_info,
        "notes": notes,
        "report": report,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
    print_summary(report, distribution, notes)
    print(f"\nFull report saved to {args.out}")


if __name__ == "__main__":
    main()
import csv
import random

from evaluation.metrics import LABELS

SHEET_COLUMNS = ["event_id", "platform", "language", "text", "human_label", "annotated_language"]
MIN_TEXT_LENGTH = 15
VALID_LABELS = {label.lower(): label for label in LABELS}


def build_sample(events, size, seed=42):
    rng = random.Random(seed)
    seen = set()
    groups = {}
    for event in events:
        text = (event.get("text") or "").strip()
        if len(text) < MIN_TEXT_LENGTH:
            continue
        key = " ".join(text.lower().split())
        if key in seen:
            continue
        seen.add(key)
        analysis = event.get("analysis") or {}
        language = (analysis.get("language") or {}).get("name") or "Unknown"
        platform = event.get("platform") or "unknown"
        groups.setdefault((platform, language), []).append({
            "event_id": event["event_id"],
            "platform": platform,
            "language": language,
            "text": text.replace("\r", " ").replace("\n", " "),
            "human_label": "",
            "annotated_language": "",
        })
    for members in groups.values():
        rng.shuffle(members)
    ordered_keys = sorted(groups)
    sample = []
    while len(sample) < size and any(groups[key] for key in ordered_keys):
        for key in ordered_keys:
            if groups[key] and len(sample) < size:
                sample.append(groups[key].pop())
    rng.shuffle(sample)
    return sample


def write_sheet(rows, path):
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=SHEET_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def read_labelled_sheet(path):
    with open(path, newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    labelled = []
    skipped_blank = 0
    invalid = []
    for row in rows:
        raw = (row.get("human_label") or "").strip().lower()
        if not raw:
            skipped_blank += 1
            continue
        if raw not in VALID_LABELS:
            invalid.append((row.get("event_id"), row.get("human_label")))
            continue
        labelled.append({
            "event_id": row["event_id"],
            "true": VALID_LABELS[raw],
            "annotated_language": (row.get("annotated_language") or "").strip(),
        })
    return labelled, {"rows": len(rows), "skipped_blank": skipped_blank, "invalid": invalid}
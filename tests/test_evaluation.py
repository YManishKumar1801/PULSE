import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from evaluation.benchmark_system import summarize_chunks, timed_chunks
from evaluation.evaluate_sentiment import evaluate, join_predictions
from evaluation.labeling import SHEET_COLUMNS, build_sample, read_labelled_sheet, write_sheet
from evaluation.metrics import classification_report, confusion_matrix, group_reports, percentile
from evaluation.trend_benchmark import make_scenario, run_scenario, summarize

Y_TRUE = ["Positive"] * 5 + ["Neutral"] * 3 + ["Negative"] * 4
Y_PRED = ["Positive", "Positive", "Positive", "Neutral", "Negative", "Neutral", "Neutral", "Positive", "Negative", "Negative", "Negative", "Neutral"]


def test_metrics_hand_checked():
    matrix = confusion_matrix(Y_TRUE, Y_PRED)
    assert matrix == [[3, 1, 1], [1, 2, 0], [0, 1, 3]]
    report = classification_report(Y_TRUE, Y_PRED)
    assert report["n"] == 12
    assert abs(report["accuracy"] - 8 / 12) < 1e-3
    positive = report["per_class"]["Positive"]
    assert abs(positive["precision"] - 0.75) < 1e-3 and abs(positive["recall"] - 0.6) < 1e-3
    assert report["per_class"]["Neutral"]["support"] == 3
    print("metrics hand-checked ok")


def test_metrics_match_sklearn():
    try:
        from sklearn.metrics import accuracy_score, f1_score
    except Exception:
        print("sklearn not available, skipped")
        return
    report = classification_report(Y_TRUE, Y_PRED)
    labels = ["Positive", "Neutral", "Negative"]
    assert abs(report["accuracy"] - accuracy_score(Y_TRUE, Y_PRED)) < 1e-3
    assert abs(report["macro_f1"] - f1_score(Y_TRUE, Y_PRED, labels=labels, average="macro")) < 1e-3
    print("metrics match sklearn")


def test_percentile_and_groups():
    assert percentile([], 95) is None
    assert percentile([5], 95) == 5
    assert abs(percentile([1, 2, 3, 4, 5], 50) - 3) < 1e-9
    assert abs(percentile([1, 2, 3, 4, 5], 95) - 4.8) < 1e-9
    rows = [{"true": "Positive", "pred": "Positive", "g": "a"}] * 3 + [{"true": "Negative", "pred": "Positive", "g": "b"}]
    groups = group_reports(rows, "g", min_support=2)
    assert groups["a"]["reliable"] is True and groups["b"]["reliable"] is False
    print("percentile and groups ok")


def make_events():
    events = []
    for index in range(60):
        language = "English" if index % 2 == 0 else "Hindi"
        platform = "youtube" if index % 3 else "telegram"
        events.append({
            "event_id": f"e{index}",
            "platform": platform,
            "text": f"message number {index} about something interesting",
            "analysis": {
                "language": {"code": "en", "name": language},
                "sentiment": {"label": "Positive", "models": {"vader": {"label": "Positive"}, "xlm_roberta": {"label": "Neutral"}}},
            },
        })
    events.append({"event_id": "dup", "platform": "youtube", "text": "message number 0 about something interesting", "analysis": {}})
    events.append({"event_id": "short", "platform": "youtube", "text": "ok", "analysis": {}})
    return events


def test_sample_and_sheet_roundtrip():
    events = make_events()
    sample = build_sample(events, 20, seed=1)
    assert len(sample) == 20
    ids = [row["event_id"] for row in sample]
    assert len(set(ids)) == 20 and "dup" not in ids and "short" not in ids
    assert {row["language"] for row in sample} == {"English", "Hindi"}
    assert build_sample(events, 20, seed=1) == sample
    assert all("pred" not in row and "sentiment" not in row for row in sample)

    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "sheet.csv")
        write_sheet(sample, path)
        with open(path, newline="", encoding="utf-8-sig") as handle:
            assert list(csv.DictReader(handle).fieldnames) == SHEET_COLUMNS
        rows = list(csv.DictReader(open(path, newline="", encoding="utf-8-sig")))
        rows[0]["human_label"] = "positive"
        rows[1]["human_label"] = "NEGATIVE"
        rows[1]["annotated_language"] = "Hinglish"
        rows[2]["human_label"] = "maybe"
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=SHEET_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        labelled, info = read_labelled_sheet(path)
    assert [item["true"] for item in labelled] == ["Positive", "Negative"]
    assert labelled[1]["annotated_language"] == "Hinglish"
    assert info["rows"] == 20 and info["skipped_blank"] == 17 and len(info["invalid"]) == 1
    print("sample and sheet ok")


def test_join_and_evaluate():
    events = make_events()
    labelled = [{"event_id": f"e{i}", "true": "Positive" if i % 4 else "Negative", "annotated_language": "Hinglish" if i < 10 else ""} for i in range(40)]
    labelled.append({"event_id": "ghost", "true": "Positive", "annotated_language": ""})
    rows, info = join_predictions(labelled, events)
    assert len(rows) == 40 and info["missing_events"] == 1
    report = evaluate(rows)
    assert report["ensemble"]["n"] == 40
    assert set(report["individual_models"]) == {"vader", "xlm_roberta"}
    assert report["individual_models"]["xlm_roberta"]["accuracy"] == 0.0
    assert "Hinglish" in report["by_annotated_language"]
    assert report["by_detected_language"]["English"]["reliable"] is True
    print("join and evaluate ok")


def test_system_helpers():
    durations = timed_chunks(list(range(70)), 32, lambda chunk: None)
    assert [count for _, count in durations] == [32, 32, 6]
    summary = summarize_chunks(durations)
    assert summary["items"] == 70 and summary["chunks"] == 3
    assert summarize_chunks([]) is None
    print("system helpers ok")


def test_trend_benchmark_structure():
    assert len(make_scenario(1, True)) > len(make_scenario(1, False))
    planted = [run_scenario(1, True)]
    control = [run_scenario(1001, False)]
    summary = summarize(planted, control)
    assert summary["planted_scenarios"] == 1 and summary["control_scenarios"] == 1
    for level in ("trend_detector", "pulse_signals"):
        assert summary[level]["detection_rate"] in (0.0, 1.0)
        assert summary[level]["flags_total"] >= summary[level]["false_flags"]
    print("trend benchmark structure ok")


if __name__ == "__main__":
    test_metrics_hand_checked()
    test_metrics_match_sklearn()
    test_percentile_and_groups()
    test_sample_and_sheet_roundtrip()
    test_join_and_evaluate()
    test_system_helpers()
    test_trend_benchmark_structure()
    print("all evaluation tests passed")
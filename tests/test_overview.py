import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timezone

from intelligence.overview import command_center_summary, sentiment_overview, worker_summary
from intelligence.signals import run_pipeline
from test_trends import make_events


def test_sentiment_overview():
    events = make_events()
    overview = sentiment_overview(events)
    assert overview["total_events"] == len(events)
    assert overview["analyzed_events"] == len(events)
    assert sum(overview["distribution"].values()) == len(events)
    assert abs(sum(overview["shares"].values()) - 1.0) < 1e-9
    assert overview["by_platform"]["youtube"]["total"] == len(events)
    assert overview["by_language"][0]["language"] == "English"
    assert overview["timeline_unit"] in ("hour", "day")
    assert sum(sum(row[label] for label in ("Positive", "Neutral", "Negative")) for row in overview["timeline"]) == len(events)
    assert overview["emotions"] == []
    assert overview["sarcasm"]["evaluated"] == 0
    print("sentiment overview ok, timeline unit:", overview["timeline_unit"])


def test_sentiment_overview_extras():
    events = make_events()[:20]
    for position, event in enumerate(events):
        event["analysis"]["emotion"] = {"label": "anger" if position % 2 else "joy", "score": 0.9}
        event["analysis"]["sarcasm"] = {"flag": position < 5, "label": "x", "score": 0.9}
    overview = sentiment_overview(events)
    assert sum(item["count"] for item in overview["emotions"]) == 20
    assert overview["sarcasm"] == {"evaluated": 20, "flagged": 5, "share": 0.25}
    print("emotion and sarcasm ok")


def test_empty_and_unanalyzed():
    empty = sentiment_overview([])
    assert empty["analyzed_events"] == 0 and empty["timeline"] == [] and empty["by_language"] == []
    events = make_events()
    for event in events:
        event.pop("analysis")
    assert sentiment_overview(events)["analyzed_events"] == 0
    print("empty ok")


def test_command_center():
    events = make_events()
    pipeline = run_pipeline(events, created_at="2026-10-09T00:00:00+00:00")
    summary = command_center_summary(events, pipeline)
    assert summary["total_events"] == len(events)
    assert summary["active_topics"] == len(pipeline["fused_topics"])
    assert summary["segments"] >= 2
    assert summary["emerging_signals"] + summary["rising_signals"] == len(pipeline["signals"])
    rates = [row["velocity_per_hour"] for row in summary["velocity"]]
    assert rates == sorted(rates, reverse=True)
    assert len(summary["top_signals"]) <= 3
    assert len(summary["highlights"]) == len(summary["top_signals"])
    for item in summary["highlights"]:
        assert item["title"] and item["kind_label"] and item["what"] and item["why"]
        assert all(row["platform"] and 0 <= row["share"] <= 100 for row in item["platforms"])
        assert all(example["text"] for example in item["examples"])
        assert len({example["text"] for example in item["examples"]}) == len(item["examples"])
        assert isinstance(item["repeated_warning"], bool)
    titles = [row["title"] for row in summary["velocity"]]
    assert len(titles) == len(set(titles))
    for row in summary["velocity"]:
        assert row["status_label"] and "current" in row and "previous" in row and row["growth_text"]
    empty = command_center_summary([], run_pipeline([]))
    assert empty["active_topics"] == 0 and empty["segments"] == 0 and empty["top_signals"] == []
    print("command center ok")


def test_topic_title():
    from intelligence.overview import topic_title
    assert topic_title(["usdt", "inr", "usdt inr"]) == "usdt, inr"
    assert topic_title(["pro max", "iphone", "pro"]) == "pro, max, iphone"
    assert topic_title([], "fallback") == "fallback"
    print("topic title ok")


def test_examples_and_repeated_warning():
    from intelligence.overview import signal_highlight
    signal = {
        "type": "emerging", "priority": "high", "score": 70, "terms": ["usdt", "inr"], "topic": "usdt, inr",
        "what": "w", "why_it_matters": "y",
        "sample_texts": ["first text", "second text"], "sample_platforms": ["telegram", None],
        "duplicate_share": 0.8, "keywords": [{"keyword": "news", "count": 3}], "platform_mix": {"telegram": 2},
    }
    item = signal_highlight(signal)
    assert item["examples"] == [{"text": "first text", "platform": "Telegram"}, {"text": "second text", "platform": ""}]
    assert item["repeated_warning"] is True and item["repeated_share"] == 80
    signal["duplicate_share"] = 0.1
    assert signal_highlight(signal)["repeated_warning"] is False
    del signal["sample_platforms"], signal["duplicate_share"]
    legacy = signal_highlight(signal)
    assert legacy["examples"][0]["platform"] == "" and legacy["repeated_warning"] is False
    print("examples and repeated warning ok")


def test_timeline_window():
    from datetime import datetime, timedelta
    from intelligence.overview import timeline_window
    start = datetime(2026, 1, 1)
    rows = [
        {"bucket": (start + timedelta(days=i)).isoformat(), "Positive": 1, "Neutral": 2, "Negative": 3}
        for i in range(200)
    ]
    last30, unit = timeline_window(rows, "day", 30)
    assert unit == "day" and len(last30) == 30
    assert last30[-1]["bucket"] == rows[-1]["bucket"]
    weekly, unit = timeline_window(rows, "day", None)
    assert unit == "week" and len(weekly) < len(rows)
    assert sum(row["Positive"] for row in weekly) == 200
    assert sum(row["Negative"] for row in weekly) == 600
    hourly, unit = timeline_window(rows[:10], "hour", None)
    assert unit == "hour" and len(hourly) == 10
    assert timeline_window([], "day", 7) == ([], "day")
    print("timeline window ok")


def test_platform_shares():
    from intelligence.overview import platform_shares
    rows = platform_shares({"youtube": 3, "telegram": 1})
    assert rows[0] == {"platform": "YouTube", "share": 75} and rows[1] == {"platform": "Telegram", "share": 25}
    assert platform_shares({}) == []
    print("platform shares ok")


def test_worker_summary():
    now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
    rows = worker_summary([
        {"worker_id": "w1", "updated_at": "2026-10-10T11:59:50+00:00", "stored": 40, "consumer_lag": 3},
        {"worker_id": "w2", "updated_at": "2026-10-10T11:50:00+00:00"},
        {"worker_id": "w3"},
    ], now=now)
    assert rows[0]["alive"] is True and rows[0]["heartbeat_age_seconds"] == 10 and rows[0]["consumer_lag"] == 3
    assert rows[1]["alive"] is False and rows[1]["heartbeat_age_seconds"] == 600
    assert rows[2]["alive"] is False and rows[2]["heartbeat_age_seconds"] is None
    assert worker_summary([]) == []
    print("worker summary ok")


if __name__ == "__main__":
    test_sentiment_overview()
    test_sentiment_overview_extras()
    test_empty_and_unanalyzed()
    test_command_center()
    test_topic_title()
    test_timeline_window()
    test_platform_shares()
    test_examples_and_repeated_warning()
    test_worker_summary()
    print("all overview tests passed")


def test_trends_overview():
    from intelligence.overview import trends_overview
    from intelligence.trends import detect_topic_trends
    result = detect_topic_trends(make_events())
    if result["status"] != "ok":
        return
    overview = trends_overview(result)
    assert overview["total"] == len(result["topics"])
    assert overview["headline"]
    for section in overview["sections"]:
        for card in section["cards"]:
            assert card["explanation"] and card["mood"] and card["change"]
    titles = [card["title"] for section in overview["sections"] for card in section["cards"]]
    assert len(titles) == len(set(titles))
    print("trends overview ok:", overview["headline"])


def test_audience_overview():
    from intelligence.audience import build_audience_segments
    from intelligence.overview import audience_overview
    result = build_audience_segments(make_events())
    if result["status"] != "ok":
        return
    overview = audience_overview(result)
    assert overview["groups"] == len(result["segments"])
    assert overview["headline"]
    titles = [card["title"] for card in overview["cards"]]
    assert len(titles) == len(set(titles))
    for card in overview["cards"]:
        assert card["summary"] and card["tone"] in ("pos", "neu", "neg")
    print("audience overview ok:", overview["headline"])


def test_network_overview():
    from intelligence.network import analyze_network
    from intelligence.overview import community_name, network_overview
    from intelligence.trends import detect_topic_trends
    events = make_events()
    trends = detect_topic_trends(events)
    topics = trends["topics"] if trends["status"] == "ok" else None
    result, _ = analyze_network(events, topics)
    assert community_name("community_03") == "Community 3"
    if result["status"] != "ok":
        return
    overview = network_overview(result)
    assert overview["headline"]
    assert len(overview["communities"]) == len(result["communities"])
    for card in overview["communities"]:
        assert card["tone"] in ("pos", "neu", "neg")
    print("network overview ok:", overview["headline"])


def test_signals_overview():
    from intelligence.overview import signal_card, signals_overview
    result = run_pipeline(make_events())
    overview = signals_overview(result)
    assert overview["headline"]
    assert overview["total"] == len(result["signals"])
    for signal in result["signals"]:
        card = signal_card(signal)
        assert card["happening"] and card["mood"] and card["spread"] and card["speed"]
        assert 0 <= card["confidence"] <= 100
    print("signals overview ok:", overview["headline"])


def test_signals_csv_and_source_label():
    import csv
    import io
    from intelligence.overview import clean_source_label, signals_csv
    assert clean_source_label("Techno Ruhez · 2y5NFKkFHkk") == "Techno Ruhez"
    assert clean_source_label("BBC News India") == "BBC News India"
    result = run_pipeline(make_events())
    rows = list(csv.reader(io.StringIO(signals_csv(result["signals"]))))
    assert rows[0][0] == "Topic"
    assert len(rows) == len(result["signals"]) + 1
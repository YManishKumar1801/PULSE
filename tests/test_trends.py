import os
import random
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence.trends import classify_trend, detect_topic_trends, growth_z_score, velocity_label

BASE = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)

VOCAB = {
    "ai": ["layoffs", "automation", "engineers", "jobs", "startup", "hiring", "models", "agents", "salary", "tech"],
    "cricket": ["match", "team", "player", "score", "tournament", "wicket", "captain", "bowling", "innings", "stadium"],
    "finance": ["market", "stock", "price", "budget", "inflation", "rupee", "investment", "economy", "interest", "savings"],
    "gaming": ["console", "graphics", "multiplayer", "controller", "release", "streaming", "esports", "levels", "quest", "studio"],
    "weather": ["rain", "monsoon", "forecast", "humidity", "storm", "temperature", "cloud", "winter", "breeze", "drizzle"],
}


def make_event(rng, topic, hour, label, counter):
    words = rng.sample(VOCAB[topic], 5)
    text = " ".join(words) + " really interesting discussion"
    stamp = (BASE + timedelta(hours=hour)).replace(microsecond=0).isoformat()
    return {
        "event_id": f"evt{counter}",
        "platform": "youtube",
        "text": text,
        "timestamp": stamp,
        "engagement": {},
        "keywords": [topic],
        "analysis": {
            "cleaned_text": text,
            "language": {"code": "en", "name": "English"},
            "sentiment": {"label": label, "confidence_proxy": 0.8, "agreement": 3, "models": {}},
        },
    }


def make_events(with_analysis=True):
    rng = random.Random(11)
    events = []
    plan = []

    prev_ai = [49.5 + 3.5 * i for i in range(6)]
    cur_ai = [72.5 + 0.9 * i for i in range(24)]
    plan += [("ai", h, ["Positive", "Positive", "Positive", "Positive", "Neutral", "Neutral"][i]) for i, h in enumerate(prev_ai)]
    ai_cur_labels = ["Negative"] * 14 + ["Neutral"] * 6 + ["Positive"] * 4
    plan += [("ai", h, ai_cur_labels[i]) for i, h in enumerate(cur_ai)]

    plan += [("cricket", float(h), "Neutral") for h in range(1, 96, 2)]

    older_fin = [2 + 2.2 * i for i in range(20)]
    prev_fin = [48.5 + 1.5 * i for i in range(15)]
    cur_fin = [80.0, 85.0, 90.0]
    plan += [("finance", h, "Neutral") for h in older_fin + prev_fin + cur_fin]

    plan += [("gaming", 49.2 + 2 * i, "Neutral") for i in range(10)]
    plan += [("gaming", 72.3 + 0.8 * i, "Neutral") for i in range(28)]

    plan += [("weather", 49.7 + 2 * i, "Neutral") for i in range(10)]
    plan += [("weather", 72.7 + 2 * i, "Neutral") for i in range(11)]

    for counter, (topic, hour, label) in enumerate(plan):
        event = make_event(rng, topic, hour, label, counter)
        if not with_analysis:
            event.pop("analysis")
        events.append(event)
    rng.shuffle(events)
    return events


def topic_for(result, vocab_key):
    best = None
    best_hits = 0
    for topic in result["topics"]:
        hits = len(set(" ".join(topic["terms"]).split()) & set(VOCAB[vocab_key]))
        if hits > best_hits:
            best = topic
            best_hits = hits
    assert best is not None and best_hits >= 2, (vocab_key, [t["terms"] for t in result["topics"]])
    return best


def test_classification_rules():
    assert classify_trend(1, 0) == (None, None)
    assert classify_trend(4, 3) == (None, None)
    status, growth = classify_trend(380, 100)
    assert status == "Emerging" and abs(growth - 2.8) < 1e-9
    assert classify_trend(5, 0) == ("Emerging", None)
    assert classify_trend(15, 10)[0] == "Stable"
    assert classify_trend(30, 15)[0] == "Rising"
    assert classify_trend(9, 6)[0] == "Stable"
    assert growth_z_score(30, 15) > 2.0 > growth_z_score(15, 10)
    assert classify_trend(10, 10, persistence=0.9)[0] == "Persistent"
    assert classify_trend(10, 10, persistence=None)[0] == "Stable"
    assert classify_trend(10, 10, persistence=0.5)[0] == "Stable"
    assert classify_trend(3, 10)[0] == "Declining"
    assert classify_trend(20, 5, persistence=0.9)[0] == "Emerging"
    assert velocity_label(None) == "high"
    assert velocity_label(2.8) == "high"
    assert velocity_label(0.5) == "medium"
    assert velocity_label(0.0) == "low"
    assert velocity_label(-0.7) == "falling"
    print("classification rules ok")


def test_topic_trends():
    result = detect_topic_trends(make_events(), n_topics=5)
    assert result["status"] == "ok", result["status"]
    assert result["window_hours"] == 24.0
    assert result["persistence_evaluated"] is True
    print("topics:", [(t["name"], t["status"], t["previous"], t["current"]) for t in result["topics"]])

    ai = topic_for(result, "ai")
    assert ai["status"] == "Emerging"
    assert ai["previous"] == 6 and ai["current"] == 24
    assert ai["growth_pct"] == 300.0
    assert ai["velocity"] == "high"
    assert ai["sentiment"]["status"] == "ok"
    assert ai["sentiment"]["direction"] == "more negative"
    assert ai["sentiment"]["current"]["negative_share"] > 0.5
    assert ai["keywords"][0]["keyword"] == "ai"
    assert result["topics"][0]["status"] == "Emerging"

    gaming = topic_for(result, "gaming")
    assert gaming["status"] == "Rising"
    assert gaming["growth_pct"] == 180.0

    cricket = topic_for(result, "cricket")
    assert cricket["status"] == "Persistent"
    assert cricket["persistence"] >= 0.8

    finance = topic_for(result, "finance")
    assert finance["status"] == "Declining"
    assert finance["growth_pct"] == -80.0
    assert finance["velocity"] == "falling"

    weather = topic_for(result, "weather")
    assert weather["status"] == "Stable"

    ids = [t["topic_id"] for t in result["topics"]]
    assert ids == sorted(ids) and ids[0] == "topic_01"

    for topic in result["topics"]:
        assert len(topic["event_ids"]) == topic["total"]
        assert len(set(topic["event_ids"])) == len(topic["event_ids"])
    all_ids = [event_id for topic in result["topics"] for event_id in topic["event_ids"]]
    assert len(all_ids) == len(set(all_ids))
    print("topic trends ok")
    return result


def test_auto_topic_count_and_no_analysis():
    result = detect_topic_trends(make_events(with_analysis=False))
    assert result["status"] == "ok"
    assert len(result["topics"]) >= 1
    for topic in result["topics"]:
        assert topic["sentiment"]["status"] == "insufficient_data"
    print("auto topics and missing analysis ok")


def test_insufficient_cases():
    assert detect_topic_trends([])["status"] == "insufficient_data"
    few = make_events()[:20]
    assert detect_topic_trends(few)["status"] == "insufficient_data"
    same_time = make_events()
    for event in same_time:
        event["timestamp"] = "2026-10-02T00:00:00+00:00"
    assert detect_topic_trends(same_time)["status"] == "insufficient_history"
    print("insufficient cases ok")


def test_short_span_uses_half_split():
    rng = random.Random(5)
    events = []
    for i in range(60):
        topic = "ai" if i % 2 == 0 else "cricket"
        hour = (i / 60) * 10
        events.append(make_event(rng, topic, hour, "Neutral", i))
    result = detect_topic_trends(events, n_topics=2)
    assert result["status"] == "ok"
    assert result["persistence_evaluated"] is False
    assert 4.0 < result["window_hours"] < 5.5
    for topic in result["topics"]:
        assert topic["status"] != "Persistent"
    print("short span ok")


if __name__ == "__main__":
    test_classification_rules()
    test_topic_trends()
    test_auto_topic_count_and_no_analysis()
    test_insufficient_cases()
    test_short_span_uses_half_split()
    print("all trend tests passed")
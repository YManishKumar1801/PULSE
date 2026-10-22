import os
import random
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence.audience import build_audience_segments, segments_for_keyword

TOPICS = {
    "tech": {
        "words": ["phone", "camera", "battery", "software", "update", "processor", "display", "charging", "gadget", "device"],
        "keyword": "iPhone",
        "sentiment": ["Positive", "Positive", "Neutral", "Negative"],
    },
    "cricket": {
        "words": ["match", "team", "player", "score", "tournament", "wicket", "captain", "bowling", "innings", "stadium"],
        "keyword": "IPL",
        "sentiment": ["Positive", "Negative", "Negative", "Neutral"],
    },
    "finance": {
        "words": ["market", "stock", "price", "budget", "inflation", "rupee", "investment", "economy", "interest", "savings"],
        "keyword": "Budget",
        "sentiment": ["Neutral", "Negative", "Neutral", "Positive"],
    },
}


def make_events(per_topic=40, seed=7):
    rng = random.Random(seed)
    base = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    events = []
    counter = 0
    for topic, spec in TOPICS.items():
        for i in range(per_topic):
            words = rng.sample(spec["words"], 5)
            text = " ".join(words) + " really interesting discussion"
            hours = i * 3 if topic != "finance" else 60 + i
            stamp = (base + timedelta(hours=hours)).replace(microsecond=0).isoformat()
            label = rng.choice(spec["sentiment"])
            events.append({
                "event_id": f"evt{counter}",
                "platform": "youtube" if counter % 2 == 0 else "telegram",
                "text": text,
                "timestamp": stamp,
                "engagement": {"likes": rng.randint(0, 20), "replies": rng.randint(0, 3), "shares": 0, "views": 0},
                "keywords": [spec["keyword"]],
                "analysis": {
                    "cleaned_text": text,
                    "language": {"code": "en", "name": "English"},
                    "sentiment": {"label": label, "confidence_proxy": 0.8, "agreement": 3, "models": {}},
                    "emotion": None,
                    "sarcasm": None,
                },
            })
            counter += 1
    rng.shuffle(events)
    return events


def test_segments():
    events = make_events()
    result = build_audience_segments(events)
    assert result["status"] == "ok", result["status"]
    assert result["method"] == "tfidf+kmeans"
    segments = result["segments"]
    print("segments found:", len(segments), "silhouette:", result["silhouette"])
    assert len(segments) == 3
    assert abs(sum(s["size"] for s in segments) - 1.0) < 0.01

    found = {}
    for segment in segments:
        topic_hits = {}
        for topic, spec in TOPICS.items():
            topic_hits[topic] = len(set(segment["top_topics"]) & set(spec["words"]))
        best = max(topic_hits, key=topic_hits.get)
        found[best] = segment
        assert topic_hits[best] >= 3, (segment["name"], segment["top_topics"])
        sentiment = segment["sentiment"]
        assert abs(sentiment["positive"] + sentiment["neutral"] + sentiment["negative"] - 1.0) < 0.01
        assert segment["languages"][0]["name"] == "English"
        assert segment["segment_id"].startswith("segment_")
        assert len(segment["sample_texts"]) == 3
        print(segment["segment_id"], "|", segment["name"], "| size", segment["size"], "| trend", segment["trend"].get("label"))
    assert set(found) == {"tech", "cricket", "finance"}

    all_ids = []
    for segment in segments:
        assert len(segment["event_ids"]) == segment["count"]
        all_ids.extend(segment["event_ids"])
    assert len(all_ids) == len(set(all_ids)) == 120

    assert found["tech"]["interest"] == "Technology"
    assert found["cricket"]["interest"] == "Sports"
    assert found["finance"]["interest"] == "Finance"
    assert found["finance"]["trend"]["label"] == "growing"
    assert found["tech"]["trend"]["label"] == "shrinking"
    print("segments ok")
    return result, found


def test_keyword_association(result, found):
    ranked = segments_for_keyword(result, "IPL")
    assert ranked[0]["segment_id"] == found["cricket"]["segment_id"]
    assert ranked[0]["share_of_keyword"] > 0.9
    assert ranked[0]["lift"] > 2.0
    assert segments_for_keyword(result, "unknown keyword") == []
    print("keyword association ok:", ranked[0])


def test_insufficient_data():
    result = build_audience_segments(make_events(per_topic=5))
    assert result["status"] == "insufficient_data"
    assert result["segments"] == []
    empty = build_audience_segments([])
    assert empty["status"] == "insufficient_data"
    print("insufficient data ok")


def test_unanalyzed_and_short_texts():
    events = make_events()
    for event in events:
        event.pop("analysis")
    events.append({
        "event_id": "short1", "platform": "news", "text": "ok nice", "timestamp": "2026-10-02T00:00:00+00:00",
        "engagement": {}, "keywords": ["iPhone"],
    })
    result = build_audience_segments(events)
    assert result["status"] == "ok"
    assert result["analyzed_events"] == 0
    assert result["usable_events"] == 120
    assert all(s["sentiment"] is None for s in result["segments"])
    print("unanalyzed and short texts ok")


def test_hinglish_and_hindi_tokens():
    rng = random.Random(3)
    hindi_words = ["फोन", "कैमरा", "बैटरी", "सॉफ्टवेयर", "अपडेट", "डिस्प्ले", "चार्जिंग"]
    english_words = ["match", "team", "player", "score", "tournament", "wicket", "captain"]
    events = []
    for i in range(40):
        for words, kw in ((hindi_words, "Phone"), (english_words, "IPL")):
            text = " ".join(rng.sample(words, 4)) + " bahut badhiya"
            events.append({
                "event_id": f"h{len(events)}", "platform": "telegram", "text": text,
                "timestamp": "2026-10-01T00:00:00+00:00", "engagement": {}, "keywords": [kw],
            })
    result = build_audience_segments(events)
    assert result["status"] == "ok"
    assert len(result["segments"]) == 2
    joined = " ".join(" ".join(s["top_topics"]) for s in result["segments"])
    assert any(word in joined for word in hindi_words)
    print("hindi tokens ok")


if __name__ == "__main__":
    result, found = test_segments()
    test_keyword_association(result, found)
    test_insufficient_data()
    test_unanalyzed_and_short_texts()
    test_hinglish_and_hindi_tokens()
    print("all audience tests passed")
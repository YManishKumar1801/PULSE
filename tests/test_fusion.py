import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from intelligence.fusion import (
    WEIGHTS,
    audience_component,
    clamp01,
    network_component,
    trend_component,
)
from intelligence.signals import (
    _how_it_spreads,
    _why_it_matters,
    confidence_proxy,
    priority_for,
    run_pipeline,
)
from test_trends import make_events


def test_weights_documented():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9
    assert clamp01(-1) == 0.0 and clamp01(2) == 1.0
    print("weights ok")


def test_trend_component():
    base = {"previous": 5, "current": 20, "velocity": "accelerating", "velocity_per_hour": 0.5}
    value, _ = trend_component({**base, "status": "Rising", "growth_pct": 150.0})
    assert abs(value - 0.5) < 1e-9
    value, _ = trend_component({**base, "status": "Emerging", "growth_pct": None})
    assert value == 1.0
    value, _ = trend_component({**base, "status": "Declining", "growth_pct": -50.0})
    assert value == 0.0
    print("trend component ok")


def test_network_requires_eligible_events():
    network = {
        "status": "ok",
        "communities_significant": 3,
        "propagation": [{"topic_id": "t1", "communities_reached": 3, "spread": "high", "origin_community": "c1", "timeline": []}],
    }
    topic = {"topic_id": "t1"}
    news_only = [{"platform": "news", "author_id": "a", "source_id": "s"}] * 5
    value, evidence = network_component(topic, network, news_only)
    assert value is None and evidence["available"] is False
    eligible = [{"platform": "telegram", "author_id": "a", "source_id": "s"}] * 3
    value, evidence = network_component(topic, network, eligible)
    assert value == 1.0 and evidence["available"] is True
    print("network eligibility ok")


def test_audience_unavailable():
    value, evidence = audience_component({"event_ids": []}, None, {})
    assert value is None and evidence["available"] is False
    print("audience unavailable ok")


def test_priority_rules():
    assert priority_for(80, 3, "Stable") is None
    assert priority_for(10, 3, "Emerging") is None
    assert priority_for(30, 0, "Emerging") == "low"
    assert priority_for(45, 1, "Rising") == "medium"
    assert priority_for(45, 0, "Rising") == "low"
    assert priority_for(65, 2, "Emerging") == "high"
    assert priority_for(65, 1, "Emerging") == "medium"
    print("priority rules ok")


def test_text_helpers():
    fused = {
        "evidence": {
            "trend": {"growth_pct": 300.0},
            "sentiment": {"available": True, "shift": 0.3, "direction": "more negative"},
            "audience": {"available": True, "concentration": 0.8, "segment": "Tech"},
            "network": {"available": True, "communities_reached": 2},
        }
    }
    text = _why_it_matters(fused)
    assert ", and " in text and text.endswith(".")
    two = {"evidence": {**fused["evidence"], "sentiment": {"available": False}, "audience": {"available": False}}}
    assert _why_it_matters(two) == "The conversation is accelerating, and it is spreading across 2 communities."
    none_lag = {"evidence": {"network": {
        "available": True, "communities_reached": 2, "total_communities": 3, "origin_community": "c1",
        "timeline": [{"community_id": "c1"}, {"community_id": "c2", "lag_hours": None}],
    }}}
    assert "None" not in _how_it_spreads(none_lag)
    print("text helpers ok")


def test_confidence_proxy_bounds():
    fused = {"current": 100, "available": ["trend", "sentiment", "audience", "network"], "corroborating": ["sentiment", "audience", "network"]}
    assert confidence_proxy(fused) == 1.0
    fused = {"current": 0, "available": ["trend"], "corroborating": []}
    assert 0.0 <= confidence_proxy(fused) < 0.1
    print("confidence proxy ok")


def test_pipeline():
    events = make_events()
    result = run_pipeline(events, created_at="2026-10-09T00:00:00+00:00")
    assert result["status"] == "ok", result["status"]
    print("vectors:", result["vectors"])
    signals = result["signals"]
    assert signals, "expected at least one signal"
    for signal in signals:
        assert signal["type"] in ("emerging", "rising")
        assert signal["priority"] in ("low", "medium", "high")
        assert 0 <= signal["score"] <= 100
        assert 0 <= signal["confidence_proxy"] <= 1
        assert abs(sum(signal["weights_used"].values()) - 1.0) < 0.01
        for key in ("what", "who", "how_they_feel", "how_it_spreads", "how_fast", "why_it_matters"):
            assert signal[key] and "None" not in signal[key], (key, signal[key])
        assert "network" in signal["missing_vectors"]
    scores = [s["score"] for s in signals]
    assert scores == sorted(scores, reverse=True)
    again = run_pipeline(events, created_at="2026-10-09T00:00:00+00:00")
    assert [s["signal_id"] for s in again["signals"]] == [s["signal_id"] for s in signals]
    for signal in signals:
        print(signal["priority"], signal["score"], signal["topic"], "|", signal["why_it_matters"])
    print("pipeline ok")


def test_insufficient():
    result = run_pipeline([])
    assert result["signals"] == []
    assert result["status"] != "ok"
    print("insufficient ok")


if __name__ == "__main__":
    test_weights_documented()
    test_trend_component()
    test_network_requires_eligible_events()
    test_audience_unavailable()
    test_priority_rules()
    test_text_helpers()
    test_confidence_proxy_bounds()
    test_pipeline()
    test_insufficient()
    print("all fusion tests passed")
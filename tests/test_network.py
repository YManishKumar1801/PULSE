import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from intelligence.network import analyze_network, build_graph

BASE = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)


class Builder:
    def __init__(self):
        self.events = []
        self.counter = 0

    def add(self, platform, source_id, author_id, hour, text, label="Neutral", source_name=None, author_name=None):
        event = {
            "event_id": f"evt{self.counter}",
            "platform": platform,
            "source_type": "message",
            "source_id": source_id,
            "source_name": source_name or source_id,
            "author_id": author_id,
            "author_name": author_name,
            "text": text,
            "timestamp": (BASE + timedelta(hours=hour)).replace(microsecond=0).isoformat(),
            "engagement": {},
            "keywords": ["demo"],
            "analysis": {
                "cleaned_text": text,
                "language": {"code": "en", "name": "English"},
                "sentiment": {"label": label, "confidence_proxy": 0.8, "agreement": 3, "models": {}},
            },
        }
        self.counter += 1
        self.events.append(event)
        return event["event_id"]


def build_scenario():
    builder = Builder()
    topic_events = {"t1": [], "t2": [], "t3": []}

    for source in ("tg_alpha", "tg_beta"):
        for a in range(12):
            for m in range(3):
                hour = 1 + a * 0.5 + m
                eid = builder.add("telegram", source, f"a{a}", hour, "layoffs jobs engineers hiring discussion", "Negative")
                topic_events["t1"].append(eid)
    for a in range(4):
        eid = builder.add("telegram", "tg_alpha", f"a{a}", 3 + a, "cricket match score captain wicket", "Positive")
        topic_events["t2"].append(eid)

    for source in ("tg_gamma", "tg_delta"):
        for b in range(12):
            for m in range(3):
                hour = 11 + b * 0.5 + m
                eid = builder.add("telegram", source, f"b{b}", hour, "layoffs jobs engineers startup talk", "Neutral")
                topic_events["t1"].append(eid)
    for b in range(2):
        eid = builder.add("telegram", "tg_gamma", f"b{b}", 14 + b, "cricket match score captain wicket", "Neutral")
        topic_events["t3"].append(eid)

    for c in range(6):
        for m in range(2):
            eid = builder.add("telegram", "tg_epsilon", f"c{c}", 31 + c + m, "layoffs jobs engineers market worry", "Negative")
            topic_events["t1"].append(eid)

    builder.add("telegram", "tg_alpha", "bridge", 2, "bridge says layoffs jobs", "Neutral")
    builder.add("telegram", "tg_alpha", "bridge", 4, "bridge says layoffs jobs again", "Neutral")
    builder.add("telegram", "tg_gamma", "bridge", 12, "bridge says layoffs jobs", "Neutral")
    builder.add("telegram", "tg_gamma", "bridge", 15, "bridge says layoffs jobs again", "Neutral")

    builder.add("telegram", "tg_zeta", "loner", 5, "single message in a tiny corner", "Neutral")

    builder.add("news", "https://example.com/a", "Example News", 6, "news article about layoffs", "Neutral")
    builder.add("telegram", "tg_alpha", None, 7, "post without participant identity", "Neutral")

    topics = [
        {"topic_id": "topic_01", "name": "layoffs, jobs, engineers", "event_ids": topic_events["t1"]},
        {"topic_id": "topic_02", "name": "cricket, match, score", "event_ids": topic_events["t2"]},
        {"topic_id": "topic_03", "name": "cricket minor", "event_ids": topic_events["t3"]},
    ]
    return builder.events, topics


def test_graph_building():
    events, _ = build_scenario()
    graph, records = build_graph(events)
    assert graph.has_node("author::telegram::bridge")
    assert graph.has_node("source::telegram::tg_alpha")
    assert not any("news" in node for node in graph.nodes)
    assert len(records) == len(events) - 2
    assert graph["author::telegram::a0"]["source::telegram::tg_alpha"]["weight"] == 4
    print("graph building ok")


def test_network_analysis():
    events, topics = build_scenario()
    result, graph = analyze_network(events, topics)
    assert result["status"] == "ok", result["status"]
    assert result["excluded_events"] == 2
    assert result["network_events"] == len(events) - 2
    json.dumps(result)

    communities = result["communities"]
    print("communities:", [(c["community_id"], c["authors"], c["sources"], c["messages"]) for c in communities])
    assert result["communities_significant"] == 3
    assert result["communities_total"] >= 4
    assert [c["community_id"] for c in communities] == ["community_01", "community_02", "community_03"]
    assert communities[0]["messages"] >= communities[1]["messages"] >= communities[2]["messages"]

    sources_by_community = [set(item["label"] for item in c["top_sources"]) for c in communities]
    alpha_beta = [s for s in sources_by_community if {"tg_alpha", "tg_beta"} <= s]
    gamma_delta = [s for s in sources_by_community if {"tg_gamma", "tg_delta"} <= s]
    assert len(alpha_beta) == 1 and len(gamma_delta) == 1

    first = communities[0]
    assert first["sentiment"]["analyzed"] == first["messages"]
    assert first["top_terms"]
    assert first["top_topics"][0]["topic_id"] == "topic_01"

    bridges = result["bridge_nodes"]
    assert bridges, "expected at least one bridge node"
    assert len(bridges) == 1, [b["label"] for b in bridges]
    assert bridges[0]["label"] == "User bridge"
    assert len(bridges[0]["communities_connected"]) == 2
    assert bridges[0]["bridge_share"] == 0.5
    assert bridges[0]["betweenness"] > 0
    assert graph.nodes["author::telegram::bridge"]["bridge"] is True
    assert graph.nodes["author::telegram::a1"]["bridge"] is False

    assert len(result["top_authors"]) == 10
    assert result["top_authors"][0]["pagerank"] >= result["top_authors"][-1]["pagerank"]
    assert all(item["type"] == "author" for item in result["top_authors"])
    assert all(item["type"] == "source" for item in result["top_sources"])
    assert any(item["sentiment_score"] is not None for item in result["top_authors"])
    print("network analysis ok")
    return result


def test_propagation(result):
    propagation = {item["topic_id"]: item for item in result["propagation"]}
    t1 = propagation["topic_01"]
    assert t1["communities_reached"] == 3
    assert t1["spread"] == "high"
    timeline = t1["timeline"]
    assert len(timeline) == 3
    assert timeline[0]["lag_hours"] == 0.0
    assert timeline[0]["lag_hours"] <= timeline[1]["lag_hours"] <= timeline[2]["lag_hours"]
    assert timeline[2]["lag_hours"] > 20
    assert t1["origin_community"] == timeline[0]["community_id"]

    assert propagation["topic_02"]["spread"] == "contained"
    assert propagation["topic_02"]["communities_reached"] == 1
    assert "topic_03" not in propagation
    assert result["propagation"][0]["topic_id"] == "topic_01"
    print("propagation ok")


def test_limitations_and_edge_cases():
    events, topics = build_scenario()
    result, _ = analyze_network(events, topics)
    assert any("News" in text for text in result["limitations"])
    assert not any("No participant appears" in text for text in result["limitations"])

    no_topics, _ = analyze_network(events)
    assert no_topics["status"] == "ok"
    assert no_topics["propagation"] == []

    empty, graph = analyze_network([])
    assert empty["status"] == "insufficient_data"
    assert graph.number_of_nodes() == 0

    builder = Builder()
    for i in range(30):
        builder.add("telegram", f"chan{i}", f"chan{i}", i, "channel broadcasting its own post here", "Neutral")
    isolated, _ = analyze_network(builder.events)
    assert isolated["status"] == "ok"
    assert any("No participant appears" in text for text in isolated["limitations"])
    assert isolated["bridge_nodes"] == []
    print("limitations and edge cases ok")


if __name__ == "__main__":
    test_graph_building()
    analysis = test_network_analysis()
    test_propagation(analysis)
    test_limitations_and_edge_cases()
    print("all network tests passed")
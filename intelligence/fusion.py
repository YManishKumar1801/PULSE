from collections import Counter

from intelligence.network import is_network_event

WEIGHTS = {"trend": 0.35, "sentiment": 0.25, "audience": 0.15, "network": 0.25}

TREND_FULL_GROWTH = 3.0
SENTIMENT_FULL_SHIFT = 0.5
MIN_SENTIMENT_EVENTS = 5
MIN_AUDIENCE_EVENTS = 5
MIN_NETWORK_TOPIC_EVENTS = 3
CORROBORATION_THRESHOLD = 0.3
SENTIMENT_LABEL_THRESHOLD = 0.2


def clamp01(value):
    return max(0.0, min(1.0, value))


def sentiment_label(score):
    if score <= -SENTIMENT_LABEL_THRESHOLD:
        return "negative"
    if score >= SENTIMENT_LABEL_THRESHOLD:
        return "positive"
    return "mixed"


def trend_component(topic):
    growth_pct = topic["growth_pct"]
    if topic["status"] == "Declining":
        value = 0.0
    elif growth_pct is None:
        value = 1.0
    else:
        value = clamp01(growth_pct / 100 / TREND_FULL_GROWTH)
    evidence = {
        "status": topic["status"],
        "previous": topic["previous"],
        "current": topic["current"],
        "growth_pct": growth_pct,
        "velocity": topic["velocity"],
        "velocity_per_hour": topic["velocity_per_hour"],
    }
    return value, evidence


def _overall_sentiment(events):
    scores = []
    positives = 0
    negatives = 0
    for event in events:
        analysis = event.get("analysis")
        if not analysis:
            continue
        label = analysis["sentiment"]["label"]
        if label == "Positive":
            scores.append(1)
            positives += 1
        elif label == "Negative":
            scores.append(-1)
            negatives += 1
        else:
            scores.append(0)
    if len(scores) < MIN_SENTIMENT_EVENTS:
        return None
    count = len(scores)
    return {
        "score": round(sum(scores) / count, 3),
        "positive_share": round(positives / count, 3),
        "negative_share": round(negatives / count, 3),
        "analyzed": count,
    }


def sentiment_component(topic, member_events):
    shift = topic.get("sentiment") or {}
    if shift.get("status") == "ok":
        current = shift["current"]
        value = clamp01(abs(shift["shift"]) / SENTIMENT_FULL_SHIFT)
        evidence = {
            "available": True,
            "basis": "latest_window",
            "label": sentiment_label(current["score"]),
            "score": current["score"],
            "negative_share": current["negative_share"],
            "previous_negative_share": shift["previous"]["negative_share"],
            "shift": shift["shift"],
            "direction": shift["direction"],
            "analyzed": current["analyzed"],
        }
        return value, evidence
    overall = _overall_sentiment(member_events)
    if overall:
        value = 0.5 * min(1.0, abs(overall["score"]))
        evidence = {
            "available": True,
            "basis": "all_messages",
            "label": sentiment_label(overall["score"]),
            "score": overall["score"],
            "negative_share": overall["negative_share"],
            "previous_negative_share": None,
            "shift": None,
            "direction": None,
            "analyzed": overall["analyzed"],
        }
        return value, evidence
    return None, {"available": False}


def build_segment_index(audience_result):
    if not audience_result or audience_result.get("status") != "ok":
        return {}
    index = {}
    for segment in audience_result["segments"]:
        for event_id in segment.get("event_ids", []):
            index[event_id] = segment
    return index


def audience_component(topic, audience_result, segment_index):
    if not audience_result or audience_result.get("status") != "ok" or len(audience_result["segments"]) < 2:
        return None, {"available": False}
    counts = Counter()
    mapped = 0
    for event_id in topic["event_ids"]:
        segment = segment_index.get(event_id)
        if segment:
            counts[segment["segment_id"]] += 1
            mapped += 1
    if mapped < MIN_AUDIENCE_EVENTS:
        return None, {"available": False}
    segment_id, count = counts.most_common(1)[0]
    segment = next(item for item in audience_result["segments"] if item["segment_id"] == segment_id)
    segment_count = len(audience_result["segments"])
    share = count / mapped
    concentration = clamp01((share - 1 / segment_count) / (1 - 1 / segment_count))
    lift = share / segment["size"] if segment["size"] else 0.0
    evidence = {
        "available": True,
        "segment_id": segment_id,
        "segment": segment["name"],
        "share": round(share, 3),
        "lift": round(lift, 2),
        "concentration": round(concentration, 3),
        "mapped_events": mapped,
        "segment_count": segment_count,
    }
    return concentration, evidence


def network_component(topic, network_result, member_events=()):
    if not network_result or network_result.get("status") != "ok" or network_result["communities_significant"] < 2:
        return None, {"available": False}
    eligible = sum(1 for event in member_events if is_network_event(event))
    if eligible < MIN_NETWORK_TOPIC_EVENTS:
        return None, {"available": False}
    total = network_result["communities_significant"]
    entry = next((item for item in network_result["propagation"] if item["topic_id"] == topic["topic_id"]), None)
    reached = entry["communities_reached"] if entry else 0
    value = clamp01((reached - 1) / (total - 1)) if reached else 0.0
    evidence = {
        "available": True,
        "communities_reached": reached,
        "total_communities": total,
        "spread": entry["spread"] if entry else "none",
        "origin_community": entry["origin_community"] if entry else None,
        "timeline": entry["timeline"] if entry else [],
    }
    return value, evidence


def fuse_topic(topic, events_by_id, audience_result, segment_index, network_result):
    members = [events_by_id[event_id] for event_id in topic["event_ids"] if event_id in events_by_id]

    trend_value, trend_evidence = trend_component(topic)
    sentiment_value, sentiment_evidence = sentiment_component(topic, members)
    audience_value, audience_evidence = audience_component(topic, audience_result, segment_index)
    network_value, network_evidence = network_component(topic, network_result, members)

    components = {
        "trend": trend_value,
        "sentiment": sentiment_value,
        "audience": audience_value,
        "network": network_value,
    }
    available = [name for name, value in components.items() if value is not None]
    missing = [name for name, value in components.items() if value is None]
    total_weight = sum(WEIGHTS[name] for name in available)
    weights_used = {name: WEIGHTS[name] / total_weight for name in available}
    score = 100 * sum(weights_used[name] * components[name] for name in available)

    corroborating = [
        name for name in ("sentiment", "audience", "network")
        if components[name] is not None and components[name] >= CORROBORATION_THRESHOLD
    ]

    return {
        "topic_id": topic["topic_id"],
        "topic": topic["name"],
        "terms": topic["terms"],
        "status": topic["status"],
        "current": topic["current"],
        "previous": topic["previous"],
        "score": round(score, 1),
        "components": {name: (round(value, 3) if value is not None else None) for name, value in components.items()},
        "weights": WEIGHTS,
        "weights_used": {name: round(value, 4) for name, value in weights_used.items()},
        "available": available,
        "missing": missing,
        "corroborating": corroborating,
        "evidence": {
            "trend": trend_evidence,
            "sentiment": sentiment_evidence,
            "audience": audience_evidence,
            "network": network_evidence,
        },
        "keywords": topic.get("keywords", []),
        "sample_texts": topic.get("sample_texts", []),
    }


def fuse_topics(trend_result, events, audience_result=None, network_result=None):
    if not trend_result or trend_result.get("status") != "ok":
        return []
    events_by_id = {event["event_id"]: event for event in events}
    segment_index = build_segment_index(audience_result)
    fused = [
        fuse_topic(topic, events_by_id, audience_result, segment_index, network_result)
        for topic in trend_result["topics"]
    ]
    fused.sort(key=lambda item: item["score"], reverse=True)
    return fused
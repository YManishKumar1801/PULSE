import hashlib
from datetime import datetime, timezone

from intelligence.audience import build_audience_segments
from intelligence.fusion import fuse_topics
from intelligence.network import analyze_network
from intelligence.trends import MIN_VOLUME, detect_topic_trends

SIGNAL_STATUSES = ("Emerging", "Rising")
MIN_SIGNAL_SCORE = 25.0
MEDIUM_SCORE = 40.0
HIGH_SCORE = 60.0
VOLUME_FULL_MULTIPLE = 4
CONFIDENCE_WEIGHTS = {"volume": 0.4, "availability": 0.3, "corroboration": 0.3}
ACCELERATING_GROWTH_PCT = 100.0
CONCENTRATED_THRESHOLD = 0.5
NEGATIVE_SHIFT_DIRECTIONS = ("more negative", "more positive")


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def priority_for(score, corroborating_count, status):
    if status not in SIGNAL_STATUSES or score < MIN_SIGNAL_SCORE:
        return None
    if score >= HIGH_SCORE and corroborating_count >= 2:
        return "high"
    if score >= MEDIUM_SCORE and corroborating_count >= 1:
        return "medium"
    return "low"


def confidence_proxy(fused):
    volume = min(1.0, fused["current"] / (VOLUME_FULL_MULTIPLE * MIN_VOLUME))
    availability = len(fused["available"]) / 4
    corroboration = len(fused["corroborating"]) / 3
    value = (
        CONFIDENCE_WEIGHTS["volume"] * volume
        + CONFIDENCE_WEIGHTS["availability"] * availability
        + CONFIDENCE_WEIGHTS["corroboration"] * corroboration
    )
    return round(value, 3)


def _signal_id(fused, meta):
    raw = f"{fused['topic']}|{'/'.join(fused['terms'])}|{meta.get('current_window_start')}"
    return "sig_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


def _what(fused, window):
    trend = fused["evidence"]["trend"]
    topic = fused["topic"]
    if trend["growth_pct"] is None:
        return f"'{topic}' is a new conversation: 0 to {trend['current']} mentions in the latest {window:g}h."
    return (
        f"Conversation about '{topic}' went from {trend['previous']} to {trend['current']} mentions "
        f"({trend['growth_pct']:+.0f}%) in the latest {window:g}h."
    )


def _who(fused):
    audience = fused["evidence"]["audience"]
    if not audience["available"]:
        return "Audience segmentation is not available for this topic yet."
    return (
        f"{round(audience['share'] * 100)}% of this topic's messages come from the '{audience['segment']}' segment, "
        f"{audience['lift']}x its share of all messages."
    )


def _how_they_feel(fused):
    sentiment = fused["evidence"]["sentiment"]
    if not sentiment["available"]:
        return "Not enough analyzed messages to describe sentiment yet."
    text = f"{sentiment['label'].capitalize()} sentiment, {round(sentiment['negative_share'] * 100)}% of analyzed messages negative"
    if sentiment["shift"] is not None:
        text += (
            f"; {sentiment['direction']} than the previous window "
            f"(negative share {round(sentiment['previous_negative_share'] * 100)}% to {round(sentiment['negative_share'] * 100)}%)"
        )
    return text + "."


def _how_it_spreads(fused):
    network = fused["evidence"]["network"]
    if not network["available"]:
        return "Network analysis is not available for this topic yet."
    reached = network["communities_reached"]
    total = network["total_communities"]
    if reached == 0:
        return f"Not yet established in any of the {total} detected communities."
    if reached == 1:
        return f"Contained to one community ({network['origin_community']}) out of {total}."
    steps = []
    for position, step in enumerate(network["timeline"]):
        if position == 0:
            steps.append(f"first seen in {step['community_id']}")
        else:
            lag = step.get("lag_hours")
            if lag is None:
                steps.append(f"reached {step['community_id']}")
            else:
                steps.append(f"reached {step['community_id']} after {lag}h")
    return f"Discussed in {reached} of {total} communities: " + ", ".join(steps) + "."


def _how_fast(fused):
    trend = fused["evidence"]["trend"]
    if trend["growth_pct"] is None:
        growth = "new this window"
    else:
        growth = f"{trend['growth_pct']:+.0f}% growth"
    return f"{growth}, {trend['velocity']} velocity ({trend['velocity_per_hour']:+.2f} messages per hour)."


def _why_it_matters(fused):
    trend = fused["evidence"]["trend"]
    sentiment = fused["evidence"]["sentiment"]
    audience = fused["evidence"]["audience"]
    network = fused["evidence"]["network"]

    clauses = []
    if trend["growth_pct"] is None or trend["growth_pct"] >= ACCELERATING_GROWTH_PCT:
        clauses.append("the conversation is accelerating")
    else:
        clauses.append("the conversation is growing")
    if network["available"] and network["communities_reached"] >= 2:
        clauses.append(f"it is spreading across {network['communities_reached']} communities")
    if sentiment["available"] and sentiment["shift"] is not None and sentiment["direction"] in NEGATIVE_SHIFT_DIRECTIONS:
        clauses.append(f"sentiment is turning {sentiment['direction'].replace('more ', '')}")
    if audience["available"] and audience["concentration"] >= CONCENTRATED_THRESHOLD:
        clauses.append(f"it is concentrated in the '{audience['segment']}' segment")

    if len(clauses) == 1:
        return "Volume is rising, but the other signals do not yet corroborate it."
    head = clauses[0].capitalize()
    rest = clauses[1:]
    if len(rest) == 1:
        return f"{head}, and {rest[0]}."
    return head + ", " + ", ".join(rest[:-1]) + ", and " + rest[-1] + "."


def build_signal(fused, meta, created_at):
    priority = priority_for(fused["score"], len(fused["corroborating"]), fused["status"])
    if priority is None:
        return None
    window = meta.get("window_hours") or 0
    return {
        "signal_id": _signal_id(fused, meta),
        "type": "emerging" if fused["status"] == "Emerging" else "rising",
        "topic": fused["topic"],
        "topic_id": fused["topic_id"],
        "terms": fused["terms"],
        "priority": priority,
        "score": fused["score"],
        "confidence_proxy": confidence_proxy(fused),
        "components": fused["components"],
        "weights_used": fused["weights_used"],
        "available_vectors": fused["available"],
        "missing_vectors": fused["missing"],
        "corroborating_vectors": fused["corroborating"],
        "evidence": fused["evidence"],
        "what": _what(fused, window),
        "who": _who(fused),
        "how_they_feel": _how_they_feel(fused),
        "how_it_spreads": _how_it_spreads(fused),
        "how_fast": _how_fast(fused),
        "why_it_matters": _why_it_matters(fused),
        "keywords": fused["keywords"],
        "sample_texts": fused["sample_texts"][:2],
        "sample_platforms": fused.get("sample_platforms", [])[:2],
        "duplicate_share": fused.get("duplicate_share", 0.0),
        "platform_mix": fused.get("platform_mix", {}),
        "window_hours": window,
        "created_at": created_at,
    }


def generate_signals(fused_topics, meta, created_at=None):
    created = created_at or utc_now_iso()
    signals = []
    for fused in fused_topics:
        signal = build_signal(fused, meta, created)
        if signal:
            signals.append(signal)
    signals.sort(key=lambda s: s["score"], reverse=True)
    return signals


def run_pipeline(events, created_at=None):
    trends = detect_topic_trends(events)
    audience = build_audience_segments(events)
    topics = trends["topics"] if trends["status"] == "ok" else None
    network, _ = analyze_network(events, topics)
    fused = fuse_topics(trends, events, audience, network)
    meta = {
        "window_hours": trends.get("window_hours"),
        "current_window_start": trends.get("current_window_start"),
    }
    signals = generate_signals(fused, meta, created_at)
    return {
        "status": trends["status"],
        "total_events": len(events),
        "usable_events": trends["usable_events"],
        "min_required": trends["min_required"],
        "window_hours": trends.get("window_hours"),
        "signals": signals,
        "fused_topics": fused,
        "counts": {
            "topics": len(trends["topics"]) if trends["status"] == "ok" else 0,
            "segments": len(audience["segments"]) if audience["status"] == "ok" else 0,
        },
        "vectors": {
            "trends": trends["status"],
            "audience": audience["status"],
            "network": network["status"],
        },
    }
from collections import Counter
from datetime import datetime, timezone

SENTIMENT_LABELS = ("Positive", "Neutral", "Negative")
MIN_LANGUAGE_EVENTS = 5
MAX_LANGUAGES = 6
HOURLY_SPAN_LIMIT = 72
TOP_SIGNALS_SHOWN = 3
VELOCITY_ROWS = 8


def _parse(timestamp):
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _empty_counts():
    return {label: 0 for label in SENTIMENT_LABELS}


def _share(counts):
    total = sum(counts.values())
    return {label: (counts[label] / total if total else 0.0) for label in SENTIMENT_LABELS}


def sentiment_overview(events):
    analyzed = [event for event in events if event.get("analysis") and event["analysis"].get("sentiment")]
    result = {
        "total_events": len(events),
        "analyzed_events": len(analyzed),
        "distribution": _empty_counts(),
        "shares": _share(_empty_counts()),
        "by_platform": {},
        "by_language": [],
        "timeline": [],
        "timeline_unit": None,
        "emotions": [],
        "sarcasm": {"evaluated": 0, "flagged": 0, "share": 0.0},
    }
    if not analyzed:
        return result

    distribution = _empty_counts()
    platforms = {}
    languages = {}
    emotions = Counter()
    sarcasm_evaluated = 0
    sarcasm_flagged = 0
    stamped = []

    for event in analyzed:
        analysis = event["analysis"]
        label = analysis["sentiment"]["label"]
        if label not in distribution:
            continue
        distribution[label] += 1

        platform = event.get("platform") or "unknown"
        platforms.setdefault(platform, _empty_counts())[label] += 1

        language = (analysis.get("language") or {}).get("name") or "Unknown"
        languages.setdefault(language, _empty_counts())[label] += 1

        emotion = analysis.get("emotion")
        if emotion and emotion.get("label"):
            emotions[emotion["label"]] += 1

        sarcasm = analysis.get("sarcasm")
        if sarcasm is not None:
            sarcasm_evaluated += 1
            if sarcasm.get("flag"):
                sarcasm_flagged += 1

        moment = _parse(event.get("timestamp"))
        if moment:
            stamped.append((moment, label))

    result["distribution"] = distribution
    result["shares"] = _share(distribution)

    result["by_platform"] = {
        name: {**counts, "total": sum(counts.values()), "shares": _share(counts)}
        for name, counts in sorted(platforms.items(), key=lambda item: -sum(item[1].values()))
    }

    language_rows = []
    for name, counts in languages.items():
        total = sum(counts.values())
        if total >= MIN_LANGUAGE_EVENTS:
            language_rows.append({"language": name, "total": total, **counts, "shares": _share(counts)})
    language_rows.sort(key=lambda row: -row["total"])
    result["by_language"] = language_rows[:MAX_LANGUAGES]

    result["emotions"] = [{"emotion": name, "count": count} for name, count in emotions.most_common()]
    result["sarcasm"] = {
        "evaluated": sarcasm_evaluated,
        "flagged": sarcasm_flagged,
        "share": round(sarcasm_flagged / sarcasm_evaluated, 3) if sarcasm_evaluated else 0.0,
    }

    if stamped:
        first = min(moment for moment, _ in stamped)
        last = max(moment for moment, _ in stamped)
        span_hours = (last - first).total_seconds() / 3600
        unit = "hour" if span_hours <= HOURLY_SPAN_LIMIT else "day"
        buckets = {}
        for moment, label in stamped:
            floored = moment.replace(minute=0, second=0, microsecond=0)
            if unit == "day":
                floored = floored.replace(hour=0)
            buckets.setdefault(floored, _empty_counts())[label] += 1
        result["timeline_unit"] = unit
        result["timeline"] = [
            {"bucket": bucket.isoformat(), **counts} for bucket, counts in sorted(buckets.items())
        ]
    return result


def command_center_summary(events, pipeline):
    analyzed = sum(1 for event in events if event.get("analysis"))
    signals = pipeline.get("signals", [])
    counts = pipeline.get("counts", {"topics": 0, "segments": 0})
    velocity = []
    for fused in pipeline.get("fused_topics", []):
        trend = fused["evidence"]["trend"]
        velocity.append({
            "topic": fused["topic"],
            "velocity_per_hour": trend["velocity_per_hour"],
            "growth_pct": trend["growth_pct"],
            "status": fused["status"],
        })
    velocity.sort(key=lambda row: -row["velocity_per_hour"])
    return {
        "total_events": len(events),
        "analyzed_events": analyzed,
        "active_topics": counts["topics"],
        "segments": counts["segments"],
        "emerging_signals": sum(1 for signal in signals if signal["type"] == "emerging"),
        "rising_signals": sum(1 for signal in signals if signal["type"] == "rising"),
        "top_signals": signals[:TOP_SIGNALS_SHOWN],
        "velocity": velocity[:VELOCITY_ROWS],
        "pipeline_status": pipeline.get("status"),
        "vectors": pipeline.get("vectors", {}),
    }


WORKER_ALIVE_SECONDS = 30


def worker_summary(statuses, now=None):
    now = now or datetime.now(timezone.utc)
    rows = []
    for status in statuses:
        updated = _parse(status.get("updated_at"))
        age = (now - updated).total_seconds() if updated else None
        rows.append({
            "worker_id": status.get("worker_id", "unknown"),
            "alive": age is not None and age <= WORKER_ALIVE_SECONDS,
            "heartbeat_age_seconds": round(age) if age is not None else None,
            "stored": status.get("stored", 0),
            "analyzed": status.get("analyzed", 0),
            "malformed": status.get("malformed", 0),
            "analysis_failures": status.get("analysis_failures", 0),
            "consumer_lag": status.get("consumer_lag"),
            "batch_seconds_p95": status.get("batch_seconds_p95"),
            "events_per_busy_second": status.get("events_per_busy_second"),
        })
    return rows
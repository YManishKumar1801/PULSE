from collections import Counter
from datetime import datetime, timedelta, timezone

from ui_sentiment import language_name

SENTIMENT_LABELS = ("Positive", "Neutral", "Negative")
MIN_LANGUAGE_EVENTS = 5
MAX_LANGUAGES = 6
WEEKLY_AFTER_DAYS = 120
TIMELINE_RANGES = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90, "All time": None}
HOURLY_SPAN_LIMIT = 72
TOP_SIGNALS_SHOWN = 3
VELOCITY_ROWS = 8

STATUS_LABELS = {
    "Emerging": "New - just appeared",
    "Rising": "Growing",
    "Persistent": "Steady and long-running",
    "Stable": "Steady",
    "Declining": "Fading",
}
KIND_LABELS = {"emerging": "New topic", "rising": "Growing topic"}
PLATFORM_NAMES = {"youtube": "YouTube", "telegram": "Telegram", "news": "News"}
MAX_TITLE_WORDS = 4
REPEATED_WARNING_SHARE = 0.5


def topic_title(terms, fallback=""):
    words = []
    for term in terms or []:
        for part in str(term).split():
            if part not in words:
                words.append(part)
        if len(words) >= MAX_TITLE_WORDS:
            break
    return ", ".join(words[:MAX_TITLE_WORDS]) or fallback


def platform_shares(mix):
    total = sum((mix or {}).values())
    if not total:
        return []
    rows = [
        {"platform": PLATFORM_NAMES.get(name, str(name).title()), "share": round(100 * count / total)}
        for name, count in mix.items()
    ]
    rows.sort(key=lambda row: -row["share"])
    return rows


def growth_text(growth_pct):
    if growth_pct is None:
        return "New"
    return f"{growth_pct:+.0f}%"


def signal_examples(signal):
    texts = signal.get("sample_texts", [])[:2]
    platforms = signal.get("sample_platforms", [])
    examples = []
    for position, text in enumerate(texts):
        if not text:
            continue
        name = platforms[position] if position < len(platforms) else None
        examples.append({"text": text, "platform": PLATFORM_NAMES.get(name, str(name).title()) if name else ""})
    return examples


def signal_highlight(signal):
    repeated = float(signal.get("duplicate_share") or 0.0)
    return {
        "repeated_share": round(repeated * 100),
        "repeated_warning": repeated >= REPEATED_WARNING_SHARE,
        "title": topic_title(signal.get("terms"), signal.get("topic", "")),
        "kind": signal["type"],
        "kind_label": KIND_LABELS.get(signal["type"], signal["type"].title()),
        "priority": signal["priority"],
        "score": signal["score"],
        "what": signal.get("what", ""),
        "why": signal.get("why_it_matters", ""),
        "examples": signal_examples(signal),
        "searched_for": [item["keyword"] for item in signal.get("keywords", []) if item.get("keyword")],
        "platforms": platform_shares(signal.get("platform_mix")),
    }


def _unique_titles(rows):
    seen = {}
    for row in rows:
        base = row["title"]
        seen[base] = seen.get(base, 0) + 1
        if seen[base] > 1:
            row["title"] = f"{base} ({seen[base]})"
    return rows


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


def timeline_window(rows, unit, days):
    if not rows:
        return [], unit
    stamped = [(datetime.fromisoformat(row["bucket"]), row) for row in rows]
    latest = max(moment for moment, _ in stamped)
    if days is not None:
        cutoff = latest - timedelta(days=days)
        stamped = [(moment, row) for moment, row in stamped if moment > cutoff]
    if not stamped:
        return [], unit
    span_days = (max(m for m, _ in stamped) - min(m for m, _ in stamped)).days
    if unit == "day" and span_days > WEEKLY_AFTER_DAYS:
        weekly = {}
        for moment, row in stamped:
            start = (moment - timedelta(days=moment.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            bucket = weekly.setdefault(start, {"Positive": 0, "Neutral": 0, "Negative": 0})
            for label in bucket:
                bucket[label] += row[label]
        return [{"bucket": start.isoformat(), **counts} for start, counts in sorted(weekly.items())], "week"
    return [row for _, row in sorted(stamped, key=lambda item: item[0])], unit


def command_center_summary(events, pipeline):
    analyzed = sum(1 for event in events if event.get("analysis"))
    signals = pipeline.get("signals", [])
    counts = pipeline.get("counts", {"topics": 0, "segments": 0})
    velocity = []
    for fused in pipeline.get("fused_topics", []):
        trend = fused["evidence"]["trend"]
        velocity.append({
            "topic": fused["topic"],
            "title": topic_title(fused.get("terms"), fused["topic"]),
            "velocity_per_hour": trend["velocity_per_hour"],
            "growth_pct": trend["growth_pct"],
            "growth_text": growth_text(trend["growth_pct"]),
            "status": fused["status"],
            "status_label": STATUS_LABELS.get(fused["status"], fused["status"]),
            "previous": fused["previous"],
            "current": fused["current"],
        })
    velocity.sort(key=lambda row: -row["velocity_per_hour"])
    velocity = _unique_titles(velocity[:VELOCITY_ROWS])
    top_signals = signals[:TOP_SIGNALS_SHOWN]
    return {
        "total_events": len(events),
        "analyzed_events": analyzed,
        "active_topics": counts["topics"],
        "segments": counts["segments"],
        "emerging_signals": sum(1 for signal in signals if signal["type"] == "emerging"),
        "rising_signals": sum(1 for signal in signals if signal["type"] == "rising"),
        "top_signals": top_signals,
        "highlights": [signal_highlight(signal) for signal in top_signals],
        "velocity": velocity,
        "window_hours": pipeline.get("window_hours"),
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


TREND_STYLE = {
    "Emerging": {"icon": "🆕", "tone": "amber", "section": "taking_off"},
    "Rising": {"icon": "📈", "tone": "green", "section": "taking_off"},
    "Persistent": {"icon": "🔵", "tone": "blue", "section": "steady"},
    "Stable": {"icon": "➖", "tone": "grey", "section": "steady"},
    "Declining": {"icon": "📉", "tone": "red", "section": "fading"},
}
TREND_SECTIONS = (
    ("taking_off", "Topics taking off", "These are getting clearly more messages than before."),
    ("steady", "Steady topics", "These get about the same attention as before."),
    ("fading", "Fading topics", "These are getting fewer messages than before."),
)


def _percent(value):
    return round((value or 0) * 100)


def trend_explanation(topic, window):
    previous = topic["previous"]
    current = topic["current"]
    span = f"{window:g} hours"
    status = topic["status"]
    if status == "Emerging":
        if previous == 0:
            return f"Almost nobody talked about this before. In the latest {span} it got {current} messages."
        return f"This jumped from {previous} to {current} messages compared with the {span} before."
    if status == "Rising":
        return f"More people are talking about this now: {previous} messages before, {current} messages in the latest {span}."
    if status == "Persistent":
        return "People have kept talking about this steadily over a long time."
    if status == "Declining":
        return f"Attention is dropping: {previous} messages before, only {current} in the latest {span}."
    return f"Attention is about the same as before ({previous} messages before, {current} now)."


def trend_mood(sentiment):
    if not sentiment or sentiment.get("status") != "ok":
        return "Not enough analysed messages yet to say how people feel about this."
    before = _percent(sentiment["previous"]["negative_share"])
    now = _percent(sentiment["current"]["negative_share"])
    direction = sentiment["direction"]
    if direction == "more positive":
        return f"People are getting more positive. Negative messages went down from {before}% to {now}%."
    if direction == "more negative":
        return f"People are getting more negative. Negative messages went up from {before}% to {now}%."
    return f"The mood is about the same as before. Negative messages: {before}% before, {now}% now."


def trend_card(topic, window):
    style = TREND_STYLE.get(topic["status"], TREND_STYLE["Stable"])
    repeated = float(topic.get("duplicate_share") or 0.0)
    examples = []
    platforms = topic.get("sample_platforms", [])
    for position, text in enumerate(topic.get("sample_texts", [])):
        if not text:
            continue
        name = platforms[position] if position < len(platforms) else None
        examples.append({"text": text, "platform": PLATFORM_NAMES.get(name, str(name).title()) if name else ""})
    return {
        "topic_id": topic["topic_id"],
        "title": topic_title(topic.get("terms"), topic.get("name", "")),
        "status": topic["status"],
        "status_label": STATUS_LABELS.get(topic["status"], topic["status"]),
        "icon": style["icon"],
        "tone": style["tone"],
        "section": style["section"],
        "before": topic["previous"],
        "now": topic["current"],
        "change": growth_text(topic["growth_pct"]),
        "explanation": trend_explanation(topic, window),
        "mood": trend_mood(topic.get("sentiment")),
        "repeated_warning": repeated >= REPEATED_WARNING_SHARE,
        "repeated_share": _percent(repeated),
        "searched_for": [item["keyword"] for item in topic.get("keywords", []) if item.get("keyword")],
        "words": list(topic.get("terms", [])),
        "examples": examples,
    }


def trends_overview(result):
    window = result.get("window_hours") or 0
    cards = _unique_titles([trend_card(topic, window) for topic in result.get("topics", [])])
    counts = Counter(card["status"] for card in cards)
    sections = []
    for key, title, caption in TREND_SECTIONS:
        group = [card for card in cards if card["section"] == key]
        if group:
            sections.append({"key": key, "title": title, "caption": caption, "cards": group})
    new_count = counts.get("Emerging", 0)
    growing_count = counts.get("Rising", 0)
    if new_count or growing_count:
        parts = []
        if new_count:
            parts.append(f"{new_count} new topic{'s' if new_count != 1 else ''}")
        if growing_count:
            parts.append(f"{growing_count} growing topic{'s' if growing_count != 1 else ''}")
        headline = "Right now PULSE sees " + " and ".join(parts) + " in the latest " + f"{window:g} hours."
    elif cards:
        headline = "No topic is taking off right now. Everything looks steady or fading."
    else:
        headline = "No topic had enough messages to be shown."
    return {
        "headline": headline,
        "window_hours": window,
        "total": len(cards),
        "new": new_count,
        "growing": growing_count,
        "fading": counts.get("Declining", 0),
        "sections": sections,
        "messages": result.get("usable_events", 0),
        "dropped": result.get("dropped_low_volume", 0),
        "min_volume": result.get("min_volume", 0),
        "persistence_evaluated": result.get("persistence_evaluated", False),
    }


SEGMENT_TREND_TEXT = {
    "growing": "Growing - this group is taking a bigger share of the conversation over time.",
    "shrinking": "Shrinking - this group is taking a smaller share of the conversation over time.",
    "stable": "Steady - this group's share has stayed about the same.",
}
LOW_QUALITY_SILHOUETTE = 0.1


def _join(items):
    items = [item for item in items if item]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def segment_mood(shares):
    if not shares:
        return "neu", "Not enough analysed messages to describe the mood."
    pos = shares["positive"]
    neu = shares["neutral"]
    neg = shares["negative"]
    if pos >= neu and pos >= neg:
        return "pos", "Mostly positive"
    if neg >= pos and neg >= neu:
        return "neg", "Mostly negative"
    return "neu", "Mostly neutral"


def segment_card(segment, rank):
    shares = segment.get("sentiment")
    tone, mood_text = segment_mood(shares)
    languages = [
        {"name": language_name(item["name"]), "share": round(item["share"] * 100)}
        for item in segment.get("languages", [])
    ]
    platforms = [
        {"platform": PLATFORM_NAMES.get(name, str(name).title()), "share": round(value * 100)}
        for name, value in segment.get("platforms", {}).items()
    ]
    engagement = segment.get("engagement", {})
    reactions = []
    if engagement.get("avg_likes"):
        reactions.append(f"{engagement['avg_likes']:,.0f} likes")
    if engagement.get("avg_replies"):
        reactions.append(f"{engagement['avg_replies']:,.0f} replies")
    if engagement.get("avg_views"):
        reactions.append(f"{engagement['avg_views']:,.0f} views")
    trend = segment.get("trend") or {}
    trend_text = SEGMENT_TREND_TEXT.get(trend.get("label")) if trend.get("status") == "ok" else None
    size_pct = round(segment["size"] * 100)
    words = segment.get("top_topics", [])
    topic = segment.get("interest")
    summary_parts = [f"This group makes up {size_pct}% of all messages."]
    if languages:
        summary_parts.append(f"Most write in {languages[0]['name']}.")
    if platforms:
        summary_parts.append(f"Most come from {platforms[0]['platform']}.")
    summary_parts.append(mood_text + ".")
    return {
        "segment_id": segment["segment_id"],
        "rank": rank,
        "title": ", ".join(words[:3]) or segment.get("name", "Unnamed group"),
        "topic_tag": topic,
        "tone": tone,
        "mood_text": mood_text,
        "shares": shares,
        "size_pct": size_pct,
        "count": segment["count"],
        "summary": " ".join(summary_parts),
        "words": words,
        "languages": languages,
        "platforms": platforms,
        "reactions": _join(reactions),
        "reactions_note": "Average per message" if reactions else "",
        "trend_text": trend_text,
        "examples": [text for text in segment.get("sample_texts", []) if text],
    }


def keyword_links(result, limit=5):
    from intelligence.audience import segments_for_keyword

    rows = []
    ranked_keywords = sorted(result.get("keyword_totals", {}).items(), key=lambda item: item[1], reverse=True)[:limit]
    names = {segment["segment_id"]: ", ".join(segment.get("top_topics", [])[:3]) or segment["name"] for segment in result["segments"]}
    for keyword, _ in ranked_keywords:
        ranked = segments_for_keyword(result, keyword)
        if not ranked:
            continue
        top = ranked[0]
        rows.append({
            "keyword": keyword,
            "group": names.get(top["segment_id"], top["name"]),
            "share": round(top["share_of_keyword"] * 100),
            "lift": top["lift"],
        })
    return rows


def audience_overview(result):
    segments = result.get("segments", [])
    cards = _unique_titles([segment_card(segment, position) for position, segment in enumerate(segments, start=1)])
    growing = sum(1 for segment in segments if (segment.get("trend") or {}).get("label") == "growing")
    biggest = cards[0] if cards else None
    quality = result.get("silhouette")
    if biggest:
        headline = (
            f"PULSE found {len(cards)} groups of conversation. The biggest one makes up {biggest['size_pct']}% of messages "
            f"and is about: {biggest['title']}."
        )
    else:
        headline = "No groups were found."
    return {
        "headline": headline,
        "cards": cards,
        "messages": result.get("usable_events", 0),
        "analysed": result.get("analyzed_events", 0),
        "groups": len(cards),
        "biggest_pct": biggest["size_pct"] if biggest else 0,
        "growing": growing,
        "quality": quality,
        "quality_low": quality is not None and quality < LOW_QUALITY_SILHOUETTE,
        "links": keyword_links(result) if segments else [],
    }


SPREAD_TEXT = {
    "high": ("Spread widely", "🔥"),
    "medium": ("Spread to a few communities", "↗️"),
    "contained": ("Stayed in one community", "📍"),
}
SINGLE_VOICE_PARTICIPANTS = 2


def community_name(community_id):
    if not community_id:
        return "A small community"
    try:
        return f"Community {int(str(community_id).split('_')[1])}"
    except (IndexError, ValueError):
        return str(community_id)


def node_mood_word(score):
    if score is None:
        return "Mood unknown"
    if score >= 0.3:
        return "Mostly positive"
    if score <= -0.3:
        return "Mostly negative"
    return "Mixed or neutral"


def clean_source_label(label):
    text = str(label or "")
    if " · " in text:
        head, tail = text.rsplit(" · ", 1)
        if tail and " " not in tail and len(tail) >= 6 and head:
            return head
    return text


def community_card(community, rank):
    shares = community.get("sentiment")
    tone, mood_text = segment_mood(shares)
    words = community.get("top_terms", [])
    single_voice = community["authors"] <= SINGLE_VOICE_PARTICIPANTS
    platforms = [
        {"platform": PLATFORM_NAMES.get(name, str(name).title()), "share": round(value * 100)}
        for name, value in community.get("platforms", {}).items()
    ]
    return {
        "community_id": community["community_id"],
        "rank": rank,
        "name": community_name(community["community_id"]),
        "title": ", ".join(words[:3]) or "No clear topic words",
        "tone": tone,
        "mood_text": mood_text,
        "shares": shares,
        "authors": community["authors"],
        "sources": community["sources"],
        "messages": community["messages"],
        "single_voice": single_voice,
        "words": words,
        "topics": [{"name": item["name"], "count": item["count"]} for item in community.get("top_topics", [])],
        "main_sources": [{"label": clean_source_label(item["label"]), "messages": item["messages"]} for item in community.get("top_sources", [])],
        "platforms": platforms,
    }


def bridge_row(node):
    names = [community_name(item) for item in node.get("communities_connected", [])]
    return {
        "label": clean_source_label(node["label"]) if node["type"] == "source" else node["label"],
        "kind": "Channel or video" if node["type"] == "source" else "Person",
        "platform": PLATFORM_NAMES.get(node["platform"], str(node["platform"]).title()),
        "text": f"Active in {_join(names)}. {round(node['bridge_share'] * 100)}% of its activity is outside its main community.",
    }


def important_node_row(node, max_rank):
    return {
        "label": clean_source_label(node["label"]) if node.get("type") == "source" else node["label"],
        "level": round(node["pagerank"] / max_rank, 3) if max_rank else 0.0,
        "connections": node["connections"],
        "messages": node["messages"],
        "community": community_name(node["community_id"]),
        "mood": node_mood_word(node["sentiment_score"]),
    }


def spread_card(item):
    label, icon = SPREAD_TEXT.get(item["spread"], ("Spread unknown", "•"))
    steps = []
    for position, step in enumerate(item["timeline"]):
        name = community_name(step["community_id"])
        if position == 0:
            steps.append(f"Started in {name} ({step['mentions']} mentions)")
        elif step.get("lag_hours") is None:
            steps.append(f"reached {name} ({step['mentions']} mentions)")
        else:
            steps.append(f"reached {name} after {step['lag_hours']}h ({step['mentions']} mentions)")
    return {
        "name": item["name"],
        "label": label,
        "icon": icon,
        "tone": {"high": "amber", "medium": "blue", "contained": "grey"}.get(item["spread"], "grey"),
        "reach": f"Reached {item['communities_reached']} of {item['total_communities']} communities, {item['total_mentions']} mentions in total.",
        "steps": steps,
    }


def network_overview(result):
    communities = [community_card(item, position) for position, item in enumerate(result.get("communities", []), start=1)]
    authors = result.get("top_authors", [])
    sources = result.get("top_sources", [])
    author_max = max([node["pagerank"] for node in authors], default=0)
    source_max = max([node["pagerank"] for node in sources], default=0)
    bridges = [bridge_row(node) for node in result.get("bridge_nodes", [])]
    spreads = [spread_card(item) for item in result.get("propagation", [])]
    single = sum(1 for card in communities if card["single_voice"])
    biggest = communities[0] if communities else None
    if biggest:
        headline = (
            f"PULSE found {len(communities)} communities. The biggest one is {biggest['name']} with "
            f"{biggest['authors']:,} people or accounts, talking about: {biggest['title']}."
        )
    else:
        headline = "No community was big enough to be shown."
    return {
        "headline": headline,
        "people": result.get("authors", 0),
        "sources": result.get("sources", 0),
        "connections": result.get("edges", 0),
        "communities": communities,
        "min_messages": result.get("min_community_messages", 0),
        "single_voice_count": single,
        "bridges": bridges,
        "top_people": [important_node_row(node, author_max) for node in authors],
        "top_sources": [important_node_row(node, source_max) for node in sources],
        "spreads": spreads,
        "notes": list(result.get("limitations", [])),
    }


PRIORITY_MEANING = {
    "high": "Several checks agree this matters a lot. Look at it first.",
    "medium": "Worth keeping an eye on. At least one other check agrees.",
    "low": "An early sign. Other checks do not back it up yet.",
}
PRIORITY_NAMES = {"high": "High priority", "medium": "Medium priority", "low": "Low priority"}
CHECK_NAMES = {"trend": "Growth", "sentiment": "Mood", "audience": "Audience", "network": "Spread"}
VELOCITY_TEXT = {
    "high": "Growing very fast",
    "medium": "Growing at a steady pace",
    "low": "Growing slowly",
    "falling": "Getting smaller",
}


def _signal_happening(evidence, window):
    trend = evidence["trend"]
    span = f"{window:g} hours"
    if trend["growth_pct"] is None:
        return f"This is a brand new conversation. It had no messages before and got {trend['current']} in the latest {span}."
    return (
        f"Messages about this went from {trend['previous']} to {trend['current']} in the latest {span} "
        f"({trend['growth_pct']:+.0f}%)."
    )


def _signal_people(evidence):
    audience = evidence["audience"]
    if not audience["available"]:
        return "Not enough information yet to say which group is talking about this."
    return f"Most of these messages ({round(audience['share'] * 100)}%) come from the group \"{audience['segment']}\"."


def _signal_mood(evidence):
    sentiment = evidence["sentiment"]
    if not sentiment["available"]:
        return "Not enough analysed messages yet to say how people feel."
    overall = {"positive": "positive", "negative": "negative", "mixed": "mixed"}.get(sentiment["label"], sentiment["label"])
    text = f"The overall mood is {overall}."
    if sentiment.get("shift") is not None and sentiment.get("previous_negative_share") is not None:
        before = _percent(sentiment["previous_negative_share"])
        now = _percent(sentiment["negative_share"])
        if sentiment["direction"] == "more positive":
            text += f" People are getting more positive: negative messages went down from {before}% to {now}%."
        elif sentiment["direction"] == "more negative":
            text += f" People are getting more negative: negative messages went up from {before}% to {now}%."
        else:
            text += f" It is about the same as before: {now}% of messages are negative."
    else:
        text += f" {_percent(sentiment['negative_share'])}% of analysed messages are negative."
    return text


def _signal_spread(evidence):
    network = evidence["network"]
    if not network["available"]:
        return "Not enough information yet to say how far this has spread."
    reached = network["communities_reached"]
    total = network["total_communities"]
    label = SPREAD_TEXT.get(network["spread"], ("", ""))[0]
    if reached == 0:
        return f"It has not taken hold in any of the {total} communities yet."
    text = f"{label}: people in {reached} of {total} communities are talking about it."
    origin = network.get("origin_community")
    if origin:
        text += f" It was first seen in {community_name(origin)}."
    return text


def _signal_speed(evidence):
    trend = evidence["trend"]
    label = VELOCITY_TEXT.get(trend["velocity"], "Speed unknown")
    return f"{label}: about {abs(trend['velocity_per_hour']):.1f} {'more' if trend['velocity_per_hour'] >= 0 else 'fewer'} messages per hour than before."


def signal_card(signal):
    base = signal_highlight(signal)
    evidence = signal["evidence"]
    window = signal.get("window_hours") or 0
    corroborating = [CHECK_NAMES[name] for name in signal.get("corroborating_vectors", []) if name in CHECK_NAMES]
    missing = [CHECK_NAMES[name] for name in signal.get("missing_vectors", []) if name in CHECK_NAMES]
    base.update({
        "signal_id": signal["signal_id"],
        "title": topic_title(signal.get("terms"), signal.get("topic", "")),
        "icon": KIND_ICONS_TEXT.get(signal["type"], ""),
        "priority_name": PRIORITY_NAMES.get(signal["priority"], signal["priority"].title()),
        "priority_meaning": PRIORITY_MEANING.get(signal["priority"], ""),
        "confidence": round(signal["confidence_proxy"] * 100),
        "agree_count": len(corroborating),
        "agree_names": corroborating,
        "missing_names": missing,
        "happening": _signal_happening(evidence, window),
        "people": _signal_people(evidence),
        "mood": _signal_mood(evidence),
        "spread": _signal_spread(evidence),
        "speed": _signal_speed(evidence),
        "why": signal.get("why_it_matters", ""),
        "words": list(signal.get("terms", [])),
    })
    return base


def signals_overview(result):
    signals = result.get("signals", [])
    cards = _unique_titles([signal_card(signal) for signal in signals])
    counts = Counter(card["priority"] for card in cards)
    kinds = Counter(card["kind"] for card in cards)
    if cards:
        top = cards[0]
        headline = (
            f"PULSE found {len(cards)} signal{'s' if len(cards) != 1 else ''} worth your attention. "
            f"The strongest is about: {top['title']}."
        )
    else:
        headline = "No signal is strong enough right now."
    vectors = result.get("vectors", {})
    return {
        "headline": headline,
        "cards": cards,
        "total": len(cards),
        "high": counts.get("high", 0),
        "new": kinds.get("emerging", 0),
        "growing": kinds.get("rising", 0),
        "messages": result.get("total_events", 0),
        "window_hours": result.get("window_hours") or 0,
        "checks": {
            "Growth": vectors.get("trends"),
            "Audience": vectors.get("audience"),
            "Spread": vectors.get("network"),
        },
    }


KIND_ICONS_TEXT = {"emerging": "🆕", "rising": "📈"}


SIGNAL_CSV_COLUMNS = [
    "Topic", "Type", "Priority", "Score", "Rough confidence %", "Other checks agree",
    "What is happening", "How fast", "Who is talking", "How people feel", "How far it has spread", "Why it matters",
]


def signals_csv(signals):
    import csv
    import io

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(SIGNAL_CSV_COLUMNS)
    for signal in signals:
        card = signal_card(signal)
        writer.writerow([
            card["title"], card["kind_label"], card["priority_name"], f"{card['score']:.0f}", card["confidence"],
            f"{card['agree_count']} of 3", card["happening"], card["speed"], card["people"], card["mood"],
            card["spread"], card["why"],
        ])
    return buffer.getvalue()
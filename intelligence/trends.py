import re
from collections import Counter
from datetime import timedelta, timezone

import numpy as np
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import TfidfVectorizer

from intelligence.audience import _event_text as event_text
from intelligence.audience import _parse_time as parse_time
from intelligence.audience import tokenize

MIN_EVENTS = 40
MIN_TOKENS = 3
MIN_VOLUME = 5
DEFAULT_WINDOW_HOURS = 24
MIN_SPAN_HOURS = 2
BUCKETS = 6
MIN_PER_BUCKET = 2
PERSISTENT_RATIO = 0.8
EMERGING_GROWTH = 2.0
RISING_GROWTH = 0.5
MIN_GROWTH_Z = 2.0
DECLINING_GROWTH = -0.3
HIGH_VELOCITY_GROWTH = 1.0
MEDIUM_VELOCITY_GROWTH = 0.3
MIN_SENTIMENT_EVENTS = 5
SENTIMENT_SHIFT_THRESHOLD = 0.1
MIN_TOPICS = 3
MAX_TOPICS = 8
EVENTS_PER_TOPIC = 30
CANDIDATE_TERMS = 10
TOP_TERMS = 4
SAMPLE_TEXTS = 3
RANDOM_STATE = 42

STATUS_ORDER = {"Emerging": 0, "Rising": 1, "Persistent": 2, "Stable": 3, "Declining": 4}


def _to_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _analyzer(tokens):
    bigrams = [f"{first}_{second}" for first, second in zip(tokens, tokens[1:])]
    return tokens + bigrams


def growth_z_score(current, previous):
    total = current + previous
    if total == 0:
        return 0.0
    return (current - previous) / (total ** 0.5)


def classify_trend(current, previous, persistence=None, min_volume=MIN_VOLUME):
    if max(current, previous) < min_volume:
        return None, None
    significant = growth_z_score(current, previous) >= MIN_GROWTH_Z
    if previous == 0:
        if significant:
            return "Emerging", None
        return "Stable", None
    growth = (current - previous) / previous
    if significant and current >= min_volume and growth >= EMERGING_GROWTH:
        return "Emerging", growth
    if significant and current >= min_volume and growth >= RISING_GROWTH:
        return "Rising", growth
    if growth <= DECLINING_GROWTH:
        return "Declining", growth
    if persistence is not None and persistence >= PERSISTENT_RATIO:
        return "Persistent", growth
    return "Stable", growth


def velocity_label(growth):
    if growth is None:
        return "high"
    if growth >= HIGH_VELOCITY_GROWTH:
        return "high"
    if growth >= MEDIUM_VELOCITY_GROWTH:
        return "medium"
    if growth <= DECLINING_GROWTH:
        return "falling"
    return "low"


def _sentiment_window(events):
    scores = []
    negatives = 0
    for event in events:
        analysis = event.get("analysis")
        if not analysis:
            continue
        label = analysis["sentiment"]["label"]
        if label == "Positive":
            scores.append(1)
        elif label == "Negative":
            scores.append(-1)
            negatives += 1
        else:
            scores.append(0)
    count = len(scores)
    if count < MIN_SENTIMENT_EVENTS:
        return None
    return {
        "score": round(sum(scores) / count, 3),
        "negative_share": round(negatives / count, 3),
        "analyzed": count,
    }


def _sentiment_shift(current_events, previous_events):
    current = _sentiment_window(current_events)
    previous = _sentiment_window(previous_events)
    if current is None or previous is None:
        return {"status": "insufficient_data", "current": current, "previous": previous}
    shift = current["score"] - previous["score"]
    if shift >= SENTIMENT_SHIFT_THRESHOLD:
        direction = "more positive"
    elif shift <= -SENTIMENT_SHIFT_THRESHOLD:
        direction = "more negative"
    else:
        direction = "steady"
    return {
        "status": "ok",
        "current": current,
        "previous": previous,
        "shift": round(shift, 3),
        "direction": direction,
    }


def _persistence(times, earliest, span_seconds):
    counts = [0] * BUCKETS
    for moment in times:
        position = (moment - earliest).total_seconds() / span_seconds
        counts[min(int(position * BUCKETS), BUCKETS - 1)] += 1
    present = sum(1 for count in counts if count >= MIN_PER_BUCKET)
    return round(present / BUCKETS, 3)


def _topic_terms(weights, feature_names):
    ordered = np.argsort(weights)[::-1][:CANDIDATE_TERMS]
    chosen = []
    for index in ordered:
        if weights[index] <= 0:
            continue
        term = str(feature_names[index])
        if "_" not in term and any(term in other.split("_") for other in chosen):
            continue
        chosen.append(term)
        if len(chosen) == TOP_TERMS:
            break
    return [term.replace("_", " ") for term in chosen]


def _text_key(text):
    return re.sub(r"\W+", " ", text.lower()).strip()[:80]


def _distinct_samples(order, documents, limit):
    chosen = []
    seen = set()
    for index in order:
        key = _text_key(documents[index][1])
        if key in seen:
            continue
        seen.add(key)
        chosen.append(index)
        if len(chosen) == limit:
            break
    return chosen


def _duplicate_share(members, documents):
    keys = [_text_key(documents[i][1]) for i in members]
    if not keys:
        return 0.0
    return round(1 - len(set(keys)) / len(keys), 3)


def _event_keywords(event):
    keywords = event.get("keywords")
    if keywords:
        return list(keywords)
    keyword = event.get("keyword")
    return [keyword] if keyword else []


def detect_topic_trends(events, window_hours=None, n_topics=None):
    result = {
        "status": "insufficient_data",
        "method": "tfidf+nmf",
        "total_events": len(events),
        "usable_events": 0,
        "min_required": MIN_EVENTS,
        "min_volume": MIN_VOLUME,
        "window_hours": None,
        "topics": [],
        "dropped_low_volume": 0,
    }

    documents = []
    for event in events:
        text = event_text(event)
        tokens = tokenize(text)
        moment = _to_utc(parse_time(event.get("timestamp")))
        if len(tokens) >= MIN_TOKENS and moment is not None:
            documents.append((event, text, tokens, moment))
    result["usable_events"] = len(documents)
    if len(documents) < MIN_EVENTS:
        return result

    times = [moment for _, _, _, moment in documents]
    earliest = min(times)
    latest = max(times)
    span_seconds = (latest - earliest).total_seconds()
    span_hours = span_seconds / 3600
    if span_hours < MIN_SPAN_HOURS:
        result["status"] = "insufficient_history"
        return result

    if window_hours:
        window = float(window_hours)
    elif span_hours >= 2 * DEFAULT_WINDOW_HOURS:
        window = float(DEFAULT_WINDOW_HOURS)
    else:
        window = span_hours / 2
    window_delta = timedelta(hours=window)
    current_start = latest - window_delta
    previous_start = latest - 2 * window_delta

    vectorizer = TfidfVectorizer(analyzer=_analyzer, min_df=3, max_df=0.5)
    try:
        matrix = vectorizer.fit_transform([tokens for _, _, tokens, _ in documents])
    except ValueError:
        vectorizer = TfidfVectorizer(analyzer=_analyzer, min_df=1)
        matrix = vectorizer.fit_transform([tokens for _, _, tokens, _ in documents])
    if matrix.shape[1] < MIN_TOPICS:
        return result

    topic_count = n_topics or max(MIN_TOPICS, min(MAX_TOPICS, len(documents) // EVENTS_PER_TOPIC))
    topic_count = min(topic_count, matrix.shape[1])
    model = NMF(n_components=topic_count, init="nndsvd", random_state=RANDOM_STATE, max_iter=500)
    weights = model.fit_transform(matrix)
    feature_names = vectorizer.get_feature_names_out()
    assignment = np.argmax(weights, axis=1)
    strength = weights.max(axis=1)

    window_totals = {"current": 0, "previous": 0}
    for moment in times:
        if moment > current_start:
            window_totals["current"] += 1
        elif moment > previous_start:
            window_totals["previous"] += 1

    persistence_enabled = span_hours >= 3 * window
    topics = []
    dropped = 0

    for topic in range(topic_count):
        members = [i for i in range(len(documents)) if assignment[i] == topic and strength[i] > 0]
        if not members:
            continue
        current_events = []
        previous_events = []
        for i in members:
            moment = documents[i][3]
            if moment > current_start:
                current_events.append(documents[i][0])
            elif moment > previous_start:
                previous_events.append(documents[i][0])
        current = len(current_events)
        previous = len(previous_events)

        persistence = None
        if persistence_enabled:
            persistence = _persistence([documents[i][3] for i in members], earliest, span_seconds)

        status, growth = classify_trend(current, previous, persistence)
        if status is None:
            dropped += 1
            continue

        terms = _topic_terms(model.components_[topic], feature_names)
        if not terms:
            dropped += 1
            continue

        keyword_counts = Counter()
        for i in members:
            for keyword in _event_keywords(documents[i][0]):
                keyword_counts[keyword] += 1

        ranked = _distinct_samples(sorted(members, key=lambda i: weights[i][topic], reverse=True), documents, SAMPLE_TEXTS)

        topics.append({
            "name": ", ".join(terms[:3]),
            "terms": terms,
            "status": status,
            "current": current,
            "previous": previous,
            "total": len(members),
            "growth_pct": round(growth * 100, 1) if growth is not None else None,
            "velocity": velocity_label(growth),
            "velocity_per_hour": round((current - previous) / window, 3),
            "share_current": round(current / window_totals["current"], 3) if window_totals["current"] else 0.0,
            "share_previous": round(previous / window_totals["previous"], 3) if window_totals["previous"] else 0.0,
            "persistence": persistence,
            "sentiment": _sentiment_shift(current_events, previous_events),
            "keywords": [{"keyword": keyword, "count": count} for keyword, count in keyword_counts.most_common(3)],
            "sample_texts": [documents[i][1][:160] for i in ranked],
            "sample_platforms": [documents[i][0].get("platform") for i in ranked],
            "duplicate_share": _duplicate_share(members, documents),
            "event_ids": [documents[i][0]["event_id"] for i in members],
        })

    topics.sort(key=lambda t: (STATUS_ORDER[t["status"]], -t["current"], -t["previous"]))
    for position, topic in enumerate(topics, start=1):
        topic["topic_id"] = f"topic_{position:02d}"

    result.update({
        "status": "ok",
        "window_hours": round(window, 2),
        "anchor": latest.replace(microsecond=0).isoformat(),
        "current_window_start": current_start.replace(microsecond=0).isoformat(),
        "previous_window_start": previous_start.replace(microsecond=0).isoformat(),
        "span_hours": round(span_hours, 1),
        "persistence_evaluated": persistence_enabled,
        "topics_found": topic_count,
        "topics": topics,
        "dropped_low_volume": dropped,
    })
    return result
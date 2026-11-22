import html
import re
from collections import Counter
from datetime import datetime

import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score

MIN_EVENTS = 30
MIN_CLUSTER_SIZE = 8
MAX_SEGMENTS = 6
MIN_TEXT_TOKENS = 3
TOP_TERMS = 5
SAMPLE_TEXTS = 3
RANDOM_STATE = 42
INTEREST_SHARE_THRESHOLD = 0.3
TREND_MIN_PER_HALF = 10
TREND_SHIFT_THRESHOLD = 0.03
MIN_SPAN_SECONDS = 3600
MIN_ASSOCIATION_COUNT = 3

TOKEN_PATTERN = re.compile(r"[a-zA-Zऀ-ॿ]{3,}")

STOPWORDS = set("""
about above after again against all also and any are aren't because been before being below between both but can
cannot could did does doing don't down during each few for from further had has have having her here hers him his
how into isn't its just let's more most my myself nor not off once only other ought our ours out over own same she
should some such than that that's the their theirs them then there these they this those through too under until
very was wasn't were what when where which while who whom why will with won't would you your yours yourself
video comment channel https http www com get got one like really much even still say said
href amp nbsp quot rel nofollow target blank br span
hai hain ka ki ke ko se mein par aur yeh wo woh nahi nahin bhi toh kya ek hota tha thi hota hoga hii main hum tum
aap kar karo kiya kuch bahut bhot jo jis tha the kaise kyun lekin magar abhi ab phir sab koi
है हैं का की के को से में पर और यह वो नहीं भी तो क्या एक था थी ही मैं हम तुम आप ने कर बहुत जो कुछ लेकिन अब सब कोई
""".split())

INTEREST_CATEGORIES = {
    "Technology": ["tech", "phone", "app", "software", "ai", "computer", "internet", "gadget", "device", "code"],
    "Sports": ["cricket", "match", "team", "player", "score", "goal", "tournament", "ipl", "football", "kohli"],
    "Politics": ["government", "minister", "election", "party", "policy", "parliament", "vote", "modi", "gandhi"],
    "Entertainment": ["movie", "film", "actor", "actress", "song", "music", "show", "trailer", "release", "bollywood"],
    "Finance": ["price", "market", "stock", "money", "economy", "tax", "inflation", "budget", "rupee", "investment"],
    "Automobile": ["car", "bike", "vehicle", "petrol", "diesel", "mileage", "engine", "fuel", "e20", "ethanol"],
}

INTEREST_PATTERNS = {
    category: [re.compile(r"\b" + re.escape(word) + r"\b") for word in words]
    for category, words in INTEREST_CATEGORIES.items()
}


def _identity(tokens):
    return tokens


def tokenize(text):
    return [word for word in TOKEN_PATTERN.findall(text.lower()) if word not in STOPWORDS]


def _event_text(event):
    analysis = event.get("analysis")
    if analysis and analysis.get("cleaned_text"):
        return analysis["cleaned_text"]
    text = html.unescape(event.get("text") or "")
    text = re.sub(r"http\S+|www\S+", " ", text)
    text = re.sub(r"<.*?>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tag_interests(text):
    lowered = text.lower()
    matched = []
    for category, patterns in INTEREST_PATTERNS.items():
        if any(pattern.search(lowered) for pattern in patterns):
            matched.append(category)
    return matched


def _parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _event_keywords(event):
    keywords = event.get("keywords")
    if keywords:
        return list(keywords)
    keyword = event.get("keyword")
    return [keyword] if keyword else []


def _choose_cluster_count(matrix, forced):
    if forced:
        return forced, None
    upper = min(MAX_SEGMENTS, matrix.shape[0] // MIN_CLUSTER_SIZE)
    upper = max(upper, 2)
    best_k = 2
    best_score = -1.0
    for k in range(2, upper + 1):
        labels = KMeans(n_clusters=k, n_init=10, random_state=RANDOM_STATE).fit_predict(matrix)
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(matrix, labels, metric="cosine", sample_size=min(2000, matrix.shape[0]), random_state=RANDOM_STATE)
        if score > best_score:
            best_score = score
            best_k = k
    return best_k, best_score


def _sentiment_shares(events):
    counts = Counter()
    for event in events:
        analysis = event.get("analysis")
        if analysis:
            counts[analysis["sentiment"]["label"]] += 1
    total = sum(counts.values())
    if total == 0:
        return None
    return {
        "positive": round(counts["Positive"] / total, 3),
        "neutral": round(counts["Neutral"] / total, 3),
        "negative": round(counts["Negative"] / total, 3),
        "analyzed": total,
    }


def _language_shares(events, top_n=3):
    counts = Counter()
    for event in events:
        analysis = event.get("analysis")
        name = analysis["language"]["name"] if analysis else "unknown"
        counts[name] += 1
    total = len(events)
    return [{"name": name, "share": round(count / total, 3)} for name, count in counts.most_common(top_n)]


def _platform_shares(events):
    counts = Counter(event["platform"] for event in events)
    total = len(events)
    return {platform: round(count / total, 3) for platform, count in counts.most_common()}


def _engagement_means(events):
    totals = {"likes": 0, "replies": 0, "shares": 0, "views": 0}
    for event in events:
        engagement = event.get("engagement") or {}
        for key in totals:
            totals[key] += int(engagement.get(key) or 0)
    count = len(events)
    return {f"avg_{key}": round(value / count, 2) for key, value in totals.items()}


def _segment_interest(texts):
    counts = Counter()
    for text in texts:
        for category in tag_interests(text):
            counts[category] += 1
    if not counts:
        return None, {}
    category, count = counts.most_common(1)[0]
    shares = {name: round(value / len(texts), 3) for name, value in counts.most_common()}
    if count / len(texts) >= INTEREST_SHARE_THRESHOLD:
        return category, shares
    return None, shares


def _time_split(times):
    valid = [t for t in times if t is not None]
    if len(valid) < 2:
        return None
    earliest = min(valid)
    latest = max(valid)
    span = (latest - earliest).total_seconds()
    if span < MIN_SPAN_SECONDS:
        return None
    return earliest + (latest - earliest) / 2


def _segment_trend(member_times, all_times, midpoint):
    if midpoint is None:
        return {"status": "insufficient_history"}
    all_valid = [t for t in all_times if t is not None]
    early_total = sum(1 for t in all_valid if t < midpoint)
    late_total = len(all_valid) - early_total
    if early_total < TREND_MIN_PER_HALF or late_total < TREND_MIN_PER_HALF:
        return {"status": "insufficient_history"}
    member_valid = [t for t in member_times if t is not None]
    early_count = sum(1 for t in member_valid if t < midpoint)
    late_count = len(member_valid) - early_count
    share_early = early_count / early_total
    share_late = late_count / late_total
    shift = share_late - share_early
    if shift >= TREND_SHIFT_THRESHOLD:
        label = "growing"
    elif shift <= -TREND_SHIFT_THRESHOLD:
        label = "shrinking"
    else:
        label = "stable"
    return {
        "status": "ok",
        "label": label,
        "share_early": round(share_early, 3),
        "share_late": round(share_late, 3),
        "shift_points": round(shift * 100, 1),
    }


def _segment_name(interest, terms):
    if interest:
        return f"{interest}-focused: {', '.join(terms[:2])}"
    return ", ".join(terms[:3]) or "Unnamed segment"


def build_audience_segments(events, k=None):
    documents = []
    for event in events:
        text = _event_text(event)
        tokens = tokenize(text)
        if len(tokens) >= MIN_TEXT_TOKENS:
            documents.append((event, text, tokens))

    base = {
        "status": "insufficient_data",
        "method": "tfidf+kmeans",
        "unit": "messages",
        "total_events": len(events),
        "usable_events": len(documents),
        "analyzed_events": sum(1 for event, _, _ in documents if event.get("analysis")),
        "min_required": MIN_EVENTS,
        "segments": [],
        "keyword_totals": {},
        "silhouette": None,
    }
    if len(documents) < MIN_EVENTS:
        return base

    token_lists = [tokens for _, _, tokens in documents]
    vectorizer = TfidfVectorizer(analyzer=_identity, min_df=2, max_df=0.6)
    try:
        matrix = vectorizer.fit_transform(token_lists)
    except ValueError:
        vectorizer = TfidfVectorizer(analyzer=_identity, min_df=1)
        matrix = vectorizer.fit_transform(token_lists)
    if matrix.shape[1] < 2:
        return base

    cluster_count, score = _choose_cluster_count(matrix, k)
    model = KMeans(n_clusters=cluster_count, n_init=10, random_state=RANDOM_STATE)
    labels = model.fit_predict(matrix)
    if score is None and len(set(labels)) > 1:
        score = silhouette_score(matrix, labels, metric="cosine", sample_size=min(2000, matrix.shape[0]), random_state=RANDOM_STATE)

    terms_array = vectorizer.get_feature_names_out()
    all_times = [_parse_time(event.get("timestamp")) for event, _, _ in documents]
    midpoint = _time_split(all_times)
    total = len(documents)

    keyword_totals = Counter()
    for event, _, _ in documents:
        for keyword in _event_keywords(event):
            keyword_totals[keyword] += 1

    segments = []
    for cluster in range(cluster_count):
        member_idx = [i for i, label in enumerate(labels) if label == cluster]
        if not member_idx:
            continue
        members = [documents[i] for i in member_idx]
        member_events = [event for event, _, _ in members]
        member_texts = [text for _, text, _ in members]

        centroid = model.cluster_centers_[cluster]
        top_idx = np.argsort(centroid)[::-1][:TOP_TERMS]
        top_terms = [str(terms_array[i]) for i in top_idx if centroid[i] > 0]

        similarity = np.asarray(matrix[member_idx].dot(centroid)).ravel()
        closest = np.argsort(similarity)[::-1][:SAMPLE_TEXTS]
        samples = [member_texts[i][:160] for i in closest]

        interest, interest_shares = _segment_interest(member_texts)

        keyword_counts = Counter()
        for event in member_events:
            for keyword in _event_keywords(event):
                keyword_counts[keyword] += 1

        segments.append({
            "name": _segment_name(interest, top_terms),
            "size": round(len(member_idx) / total, 3),
            "count": len(member_idx),
            "top_topics": top_terms,
            "interest": interest,
            "interest_shares": interest_shares,
            "sentiment": _sentiment_shares(member_events),
            "languages": _language_shares(member_events),
            "platforms": _platform_shares(member_events),
            "engagement": _engagement_means(member_events),
            "keywords": [{"keyword": kw, "count": count} for kw, count in keyword_counts.most_common(3)],
            "keyword_counts": dict(keyword_counts),
            "trend": _segment_trend([all_times[i] for i in member_idx], all_times, midpoint),
            "sample_texts": samples,
            "event_ids": [event["event_id"] for event in member_events],
        })

    segments.sort(key=lambda s: s["count"], reverse=True)
    for position, segment in enumerate(segments, start=1):
        segment["segment_id"] = f"segment_{position:02d}"

    base.update({
        "status": "ok",
        "segments": segments,
        "keyword_totals": dict(keyword_totals),
        "silhouette": round(float(score), 3) if score is not None else None,
    })
    return base


def segments_for_keyword(result, keyword):
    keyword_total = result.get("keyword_totals", {}).get(keyword, 0)
    if not keyword_total:
        return []
    ranked = []
    for segment in result["segments"]:
        count = segment["keyword_counts"].get(keyword, 0)
        if count == 0:
            continue
        share_of_keyword = count / keyword_total
        lift = share_of_keyword / segment["size"] if segment["size"] else 0.0
        ranked.append({
            "segment_id": segment["segment_id"],
            "name": segment["name"],
            "count": count,
            "share_of_keyword": round(share_of_keyword, 3),
            "lift": round(lift, 2),
        })
    qualified = [item for item in ranked if item["count"] >= MIN_ASSOCIATION_COUNT]
    pool = qualified or ranked
    pool.sort(key=lambda item: item["lift"], reverse=True)
    return pool
from pymongo import MongoClient
from collections import Counter
from datetime import datetime, timezone, timedelta
import re
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["pulse_db"]

STOPWORDS = set("""
a about above after again against all am an and any are aren't as at be because been
before being below between both but by can't cannot could couldn't did didn't do does
doesn't doing don't down during each few for from further had hadn't has hasn't have
haven't having he he'd he'll he's her here here's hers herself him himself his how
how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most
mustn't my myself no nor not of off on once only or other ought our ours ourselves out
over own same shan't she she'd she'll she's should shouldn't so some such than that
that's the their theirs them themselves then there there's these they they'd they'll
they're they've this those through to too under until up very was wasn't we we'd we'll
we're we've were weren't what what's when when's where where's which while who who's
whom why why's with won't would wouldn't you you'd you'll you're you've your yours
yourself yourselves is are was were video comment channel https http www com
""".split())


def extract_documents():
    documents = []

    for doc in db["sentiment_results"].find():
        fetched_at = doc.get("fetched_at")
        for c in doc.get("comments", []):
            documents.append({
                "text": c.get("cleaned_text") or c.get("original_text", ""),
                "date": fetched_at,
                "platform": doc.get("platform", "YouTube")
            })

    for doc in db["telegram_sentiment_results"].find():
        for m in doc.get("messages", []):
            documents.append({
                "text": m.get("cleaned_text") or m.get("original_text", ""),
                "date": m.get("date"),
                "platform": "Telegram"
            })

    return documents


def tokenize(text):
    words = re.findall(r"[a-zA-Z\u0900-\u097F]{3,}", text.lower())
    return [w for w in words if w not in STOPWORDS]


def parse_date(date_value):
    if isinstance(date_value, datetime):
        return date_value
    if isinstance(date_value, str):
        try:
            return datetime.fromisoformat(date_value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def detect_trends(documents, recent_hours=24, top_n=15):
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(hours=recent_hours)
    older_cutoff = recent_cutoff - timedelta(hours=recent_hours)

    recent_counter = Counter()
    older_counter = Counter()
    overall_counter = Counter()

    for doc in documents:
        words = tokenize(doc["text"])
        overall_counter.update(words)

        date = parse_date(doc["date"])
        if date is None:
            continue
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)

        if date >= recent_cutoff:
            recent_counter.update(words)
        elif date >= older_cutoff:
            older_counter.update(words)

    trending = []
    for word, recent_count in recent_counter.items():
        older_count = older_counter.get(word, 0)
        growth = recent_count - older_count
        trending.append((word, recent_count, older_count, growth))

    trending.sort(key=lambda x: x[3], reverse=True)

    return {
        "most_mentioned_overall": overall_counter.most_common(top_n),
        "trending_now": trending[:top_n]
    }


if __name__ == "__main__":
    print("Loading data from MongoDB...\n")
    documents = extract_documents()
    print(f"Loaded {len(documents)} text documents across all platforms.\n")

    if not documents:
        print("No data found. Run the fetch scripts first to collect some data.")
    else:
        results = detect_trends(documents)

        print("=" * 60)
        print("MOST MENTIONED KEYWORDS (all-time, in collected data)")
        print("=" * 60)
        for word, count in results["most_mentioned_overall"]:
            print(f"{word:20s} {count}")

        print("\n" + "=" * 60)
        print("TRENDING NOW (rising in the last 24 hours)")
        print("=" * 60)
        for word, recent, older, growth in results["trending_now"]:
            print(f"{word:20s} recent={recent:4d}  previous={older:4d}  growth={growth:+d}")
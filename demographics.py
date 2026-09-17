from pymongo import MongoClient
from collections import Counter
from datetime import datetime, timezone
from langdetect import detect, LangDetectException
import re
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["pulse_db"]
demographics_collection = db["demographics_results"]

LANGUAGE_NAMES = {
    "en": "English", "hi": "Hindi", "mr": "Marathi", "ta": "Tamil",
    "te": "Telugu", "bn": "Bengali", "gu": "Gujarati", "kn": "Kannada",
    "ml": "Malayalam", "pa": "Punjabi", "ur": "Urdu", "ne": "Nepali"
}

INTEREST_CATEGORIES = {
    "Technology": ["tech", "phone", "app", "software", "ai", "computer", "internet", "gadget", "device", "code"],
    "Sports": ["cricket", "match", "team", "player", "score", "goal", "tournament", "ipl", "football", "kohli"],
    "Politics": ["government", "minister", "election", "party", "policy", "parliament", "vote", "modi", "gandhi"],
    "Entertainment": ["movie", "film", "actor", "actress", "song", "music", "show", "trailer", "release", "bollywood"],
    "Finance": ["price", "market", "stock", "money", "economy", "tax", "inflation", "budget", "rupee", "investment"],
    "Automobile": ["car", "bike", "vehicle", "petrol", "diesel", "mileage", "engine", "fuel", "e20", "ethanol"]
}


def extract_documents():
    documents = []

    for doc in db["sentiment_results"].find():
        for c in doc.get("comments", []):
            documents.append({
                "text": c.get("cleaned_text") or c.get("original_text", ""),
                "platform": doc.get("platform", "YouTube"),
                "keyword": doc.get("keyword", "")
            })

    for doc in db["telegram_sentiment_results"].find():
        for m in doc.get("messages", []):
            documents.append({
                "text": m.get("cleaned_text") or m.get("original_text", ""),
                "platform": "Telegram",
                "keyword": doc.get("keyword", "")
            })

    return documents


def detect_language(text):
    if not text or len(text.strip()) < 10:
        return "unknown"
    try:
        code = detect(text)
        return LANGUAGE_NAMES.get(code, code)
    except LangDetectException:
        return "unknown"


def tag_interests(text):
    text_lower = text.lower()
    matched = []
    for category, keywords in INTEREST_CATEGORIES.items():
        if any(re.search(r"\b" + re.escape(kw) + r"\b", text_lower) for kw in keywords):
            matched.append(category)
    return matched


def analyze_demographics(documents):
    language_counter = Counter()
    interest_counter = Counter()
    platform_language = {}

    for doc in documents:
        text = doc["text"]
        if not text:
            continue

        lang = detect_language(text)
        language_counter[lang] += 1

        platform = doc["platform"]
        if platform not in platform_language:
            platform_language[platform] = Counter()
        platform_language[platform][lang] += 1

        for category in tag_interests(text):
            interest_counter[category] += 1

    total = sum(language_counter.values())
    language_distribution = {
        lang: {"count": count, "percent": round(count / total * 100, 1)}
        for lang, count in language_counter.most_common()
    } if total > 0 else {}

    return {
        "language_distribution": language_distribution,
        "interest_distribution": dict(interest_counter.most_common()),
        "platform_language_breakdown": {p: dict(c.most_common()) for p, c in platform_language.items()},
        "total_documents_analyzed": total
    }


if __name__ == "__main__":
    print("Loading data from MongoDB...\n")
    documents = extract_documents()
    print(f"Loaded {len(documents)} text documents.\n")

    if not documents:
        print("No data found. Run the fetch scripts first to collect some data.")
    else:
        print("Analyzing language and interest patterns (this may take a moment)...\n")
        results = analyze_demographics(documents)

        print("=" * 60)
        print("LANGUAGE DISTRIBUTION (aggregate, anonymized)")
        print("=" * 60)
        for lang, stats in results["language_distribution"].items():
            print(f"{lang:15s} {stats['count']:5d} messages  ({stats['percent']}%)")

        print("\n" + "=" * 60)
        print("INTEREST / TOPIC DISTRIBUTION")
        print("=" * 60)
        for category, count in results["interest_distribution"].items():
            print(f"{category:15s} {count} mentions")

        print("\n" + "=" * 60)
        print("LANGUAGE BY PLATFORM")
        print("=" * 60)
        for platform, langs in results["platform_language_breakdown"].items():
            print(f"\n{platform}:")
            for lang, count in langs.items():
                print(f"  {lang:15s} {count}")

        demographics_collection.update_one(
            {"_id": "latest_snapshot"},
            {"$set": {**results, "generated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        print("\nSaved snapshot to MongoDB (pulse_db.demographics_results)")
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from pymongo import MongoClient
from datetime import datetime, timezone
import json
import re
import html
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["pulse_db"]
telegram_collection = db["telegram_sentiment_results"]

try:
    mongo_client.admin.command("ping")
    print("Connected to MongoDB Atlas successfully.\n")
except Exception as e:
    print(f"MongoDB connection failed: {e}")
    exit(1)

vader_analyzer = SentimentIntensityAnalyzer()

print("Loading sentiment models...")
bert_classifier = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
xlm_classifier = pipeline("sentiment-analysis", model="cardiffnlp/twitter-xlm-roberta-base-sentiment")
print("Models ready.\n")


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF]", "", text)
    text = re.sub(r"[^a-zA-Z0-9\u0900-\u097F\s.,!?']", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_vader_sentiment(text):
    if not text:
        return "Neutral", 0.0
    scores = vader_analyzer.polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "Positive"
    elif compound <= -0.05:
        label = "Negative"
    else:
        label = "Neutral"
    return label, compound


def get_bert_sentiment(text):
    if not text or len(text.strip()) == 0:
        return "Neutral", 0.0
    result = bert_classifier(text[:512])[0]
    label = "Positive" if result["label"] == "POSITIVE" else "Negative"
    score = result["score"]
    if score < 0.75:
        label = "Neutral"
    return label, round(score, 3)


def get_xlm_sentiment(text):
    if not text or len(text.strip()) == 0:
        return "Neutral", 0.0
    try:
        result = xlm_classifier(text[:512])[0]
        label_map = {"positive": "Positive", "neutral": "Neutral", "negative": "Negative"}
        label = label_map.get(result["label"].lower(), "Neutral")
        return label, round(result["score"], 3)
    except Exception:
        return "Neutral", 0.0


def ensemble_sentiment(v_label, b_label, x_label):
    votes = [v_label, b_label, x_label]
    counts = {}
    for v in votes:
        counts[v] = counts.get(v, 0) + 1
    best_label = max(counts, key=counts.get)
    if counts[best_label] >= 2:
        return best_label
    return x_label


def process_channel(channel_data):
    print(f"\n{'='*80}")
    print(f"Channel: {channel_data['channel_title']} (@{channel_data['channel_username']})")
    print(f"Keyword: {channel_data['keyword']}")
    print(f"{'='*80}")

    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    message_docs = []

    for msg in channel_data["messages"]:
        cleaned = clean_text(msg["text"])
        v_label, v_score = get_vader_sentiment(cleaned)
        b_label, b_score = get_bert_sentiment(cleaned)
        x_label, x_score = get_xlm_sentiment(cleaned)
        final = ensemble_sentiment(v_label, b_label, x_label)

        counts[final] += 1
        message_docs.append({
            "original_text": msg["text"],
            "cleaned_text": cleaned,
            "final_sentiment": final,
            "date": msg["date"],
            "sender_id": msg["sender_id"],
            "views": msg.get("views")
        })

        print(f"[{final:>8}] {cleaned[:70]}")

    total = len(message_docs)
    if total > 0:
        print(f"\nSummary: Positive {counts['Positive']}/{total} | "
              f"Negative {counts['Negative']}/{total} | "
              f"Neutral {counts['Neutral']}/{total}")

    document = {
        "channel_title": channel_data["channel_title"],
        "channel_username": channel_data["channel_username"],
        "keyword": channel_data["keyword"],
        "platform": "Telegram",
        "fetched_at": datetime.now(timezone.utc),
        "sentiment_counts": counts,
        "total_messages": total,
        "messages": message_docs
    }

    telegram_collection.update_one(
        {"channel_username": channel_data["channel_username"], "keyword": channel_data["keyword"]},
        {"$set": document},
        upsert=True
    )
    print("Saved to MongoDB (pulse_db.telegram_sentiment_results)")


if __name__ == "__main__":
    with open("telegram_data.json", "r", encoding="utf-8") as f:
        all_channels = json.load(f)

    for channel_data in all_channels:
        process_channel(channel_data)

    print(f"\nDone. Processed {len(all_channels)} channels.")
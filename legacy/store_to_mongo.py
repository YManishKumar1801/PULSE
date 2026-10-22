from kafka import KafkaConsumer
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

KAFKA_TOPIC = "pulse-youtube"
KAFKA_BROKER = "localhost:9092"
MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["pulse_db"]
posts_collection = db["posts"]
sentiment_collection = db["sentiment_results"]

try:
    mongo_client.admin.command("ping")
    print("Connected to MongoDB Atlas successfully.\n")
except Exception as e:
    print(f"MongoDB connection failed: {e}")
    exit(1)

vader_analyzer = SentimentIntensityAnalyzer()

print("Loading DistilBERT model...")
bert_classifier = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english"
)
print("DistilBERT ready.\n")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    text = re.sub(r"[^a-zA-Z0-9\s.,!?']", "", text)
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
    truncated = text[:512]
    result = bert_classifier(truncated)[0]
    label = "Positive" if result["label"] == "POSITIVE" else "Negative"
    score = result["score"]
    if score < 0.65:
        label = "Neutral"
    return label, round(score, 3)


def ensemble_sentiment(vader_label, bert_label):
    if vader_label == bert_label:
        return vader_label
    else:
        return bert_label


def process_and_store_video(data):
    print(f"\n{'='*80}")
    print(f"Video: {data['title']}")
    print(f"Keyword: {data['keyword']} | Channel: {data['channel']}")
    print(f"{'='*80}")

    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    comment_docs = []

    for comment in data["comments"]:
        raw_text = comment["text"]
        cleaned = clean_text(raw_text)

        vader_label, vader_score = get_vader_sentiment(cleaned)
        bert_label, bert_score = get_bert_sentiment(cleaned)
        final_label = ensemble_sentiment(vader_label, bert_label)

        counts[final_label] += 1

        comment_docs.append({
            "original_text": raw_text,
            "cleaned_text": cleaned,
            "vader_label": vader_label,
            "vader_score": vader_score,
            "bert_label": bert_label,
            "bert_score": bert_score,
            "final_sentiment": final_label,
            "author": comment.get("author"),
            "likes": comment.get("likes")
        })

        print(f"[{final_label:>8}] {cleaned[:60]}")

    total = len(comment_docs)
    print(f"\nSummary: Positive {counts['Positive']}/{total} | "
          f"Negative {counts['Negative']}/{total} | "
          f"Neutral {counts['Neutral']}/{total}")

    document = {
        "video_id": data["video_id"],
        "keyword": data["keyword"],
        "title": data["title"],
        "channel": data["channel"],
        "platform": "YouTube",
        "views": data.get("views"),
        "likes": data.get("likes"),
        "fetched_at": datetime.now(timezone.utc),
        "sentiment_counts": counts,
        "total_comments": total,
        "comments": comment_docs
    }

    sentiment_collection.update_one(
        {"video_id": data["video_id"], "keyword": data["keyword"]},
        {"$set": document},
        upsert=True
    )
    print(f"Saved to MongoDB (pulse_db.sentiment_results)")


if __name__ == "__main__":
    print(f"Listening on Kafka topic '{KAFKA_TOPIC}'... (Ctrl+C to stop)\n")

    try:
        for message in consumer:
            data = message.value
            process_and_store_video(data)
    except KeyboardInterrupt:
        print("\n\nStopped by user.")

    print("\nDone. Check MongoDB Atlas -> pulse_db -> sentiment_results collection.")
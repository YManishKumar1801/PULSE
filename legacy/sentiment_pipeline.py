from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import json
import re
import html

KAFKA_TOPIC = "pulse-youtube"
KAFKA_BROKER = "localhost:9092"

vader_analyzer = SentimentIntensityAnalyzer()

print("Loading DistilBERT model... (first run downloads it, may take a minute)")
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
    """URLs, HTML entities, emojis, special characters hatao aur normalize karo"""
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
    """DistilBERT only gives POSITIVE/NEGATIVE (no neutral) with a confidence score"""
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
    """Simple rule: if both agree, use that. If they disagree, prefer DistilBERT (context-aware)."""
    if vader_label == bert_label:
        return vader_label, "agreement"
    else:
        return bert_label, "disagreement (used DistilBERT)"




def process_video(data):
    print(f"\n{'='*80}")
    print(f"Video: {data['title']}")
    print(f"Keyword: {data['keyword']} | Channel: {data['channel']}")
    print(f"{'='*80}")

    results = []
    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}

    for comment in data["comments"]:
        raw_text = comment["text"]
        cleaned = clean_text(raw_text)

        vader_label, vader_score = get_vader_sentiment(cleaned)
        bert_label, bert_score = get_bert_sentiment(cleaned)
        final_label, agreement = ensemble_sentiment(vader_label, bert_label)

        counts[final_label] += 1
        results.append({
            "original_text": raw_text,
            "cleaned_text": cleaned,
            "vader": {"label": vader_label, "score": vader_score},
            "distilbert": {"label": bert_label, "score": bert_score},
            "final_sentiment": final_label,
            "agreement": agreement
        })

        print(f"[{final_label:>8}] VADER:{vader_label[:3]}({vader_score:+.2f}) "
              f"BERT:{bert_label[:3]}({bert_score:.2f}) | {cleaned[:60]}")

    total = len(results)
    if total > 0:
        print(f"\nFinal Summary: Positive {counts['Positive']}/{total} | "
              f"Negative {counts['Negative']}/{total} | "
              f"Neutral {counts['Neutral']}/{total}")

    return results




if __name__ == "__main__":
    print(f"Listening on Kafka topic '{KAFKA_TOPIC}'... (Ctrl+C to stop)\n")

    all_results = []

    try:
        for message in consumer:
            data = message.value
            results = process_video(data)
            all_results.append({
                "video_id": data["video_id"],
                "title": data["title"],
                "keyword": data["keyword"],
                "comments_analyzed": results
            })
    except KeyboardInterrupt:
        print("\n\nStopped by user.")

    with open("sentiment_results.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved sentiment results to sentiment_results.json")
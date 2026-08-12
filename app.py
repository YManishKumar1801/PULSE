import streamlit as st
from googleapiclient.discovery import build
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, timezone
import re
import html
import requests
import json
import uuid
import math
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "pulse-youtube"

FIREBASE_SIGNIN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_API_KEY}"
FIREBASE_SIGNUP_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={FIREBASE_API_KEY}"

st.set_page_config(page_title="PULSE - Live Sentiment Dashboard", layout="wide")

class PulseError(Exception):
    pass

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "auth_mode" not in st.session_state:
    st.session_state.auth_mode = "login"


def firebase_login(email, password):
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(FIREBASE_SIGNIN_URL, json=payload)
    if r.status_code == 200:
        return True, r.json()
    return False, r.json().get("error", {}).get("message", "Login failed")


def firebase_signup(email, password):
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(FIREBASE_SIGNUP_URL, json=payload)
    if r.status_code == 200:
        return True, r.json()
    return False, r.json().get("error", {}).get("message", "Signup failed")



st.markdown("""
<style>
header[data-testid="stHeader"] { display: none !important; }
div[data-testid="stToolbar"] { display: none !important; }
div[data-testid="stHeaderActionElements"] { display: none !important; }
.stApp h1 a, .stApp h2 a, .stApp h3 a, .stApp h4 a { display: none !important; }
.stApp { background-color: #FAF9F6; }
.block-container { padding-top: 2rem; max-width: 1100px; }

h1, h2, h3, h4, h5, h6 { color: #2C2C2A !important; }
p, span, label, .stMarkdown, .stCaption, div[data-testid="stCaptionContainer"] { color: #2C2C2A !important; }
div[data-testid="stCaptionContainer"] p { color: #5F5E5A !important; }

.pulse-brand { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.pulse-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }

.metric-card {
    background: #fff; border-radius: 12px; padding: 20px 18px;
    border: 0.5px solid #E5E2D9; text-align: center;
    box-shadow: 0 1px 3px rgba(44,44,42,0.04);
    border-top: 3px solid var(--accent-color, #E5E2D9);
}
.metric-label { color: #5F5E5A !important; font-size: 12px; font-weight: 500; letter-spacing: 0.3px; margin-bottom: 8px; }
.metric-value { font-size: 30px; font-weight: 700; margin: 0; }

.reason-card {
    background: #fff; border-radius: 12px; padding: 18px 20px;
    border: 0.5px solid #E5E2D9; box-shadow: 0 1px 3px rgba(44,44,42,0.04);
    border-left: 3px solid var(--accent-color, #E5E2D9);
}
.reason-title { font-size: 13px; font-weight: 600; margin: 0 0 8px 0; }
.reason-text { color: #5F5E5A; font-size: 13px; line-height: 1.65; margin: 0; }

.keyword-pill {
    display: inline-block; background: #EAF3DE; color: #3B6D11;
    padding: 4px 14px; border-radius: 20px; font-size: 13px; font-weight: 600;
    margin-bottom: 4px;
}

div[data-testid="stTextInput"] input {
    background-color: #fff !important;
    color: #2C2C2A !important;
    border: 0.5px solid #D3D1C7 !important;
    border-radius: 10px !important;
    height: 46px !important;
}

div.stButton > button[kind="primary"], div.stButton > button[kind="primary"] * {
    color: white !important;
}
div.stButton > button[kind="primary"] {
    background-color: #3B6D11; border-radius: 10px; border: none;
    font-weight: 600; height: 46px;
    box-shadow: 0 1px 3px rgba(59,109,17,0.25);
}
div.stButton > button[kind="primary"]:hover { background-color: #305a0d; }

div.stButton > button[kind="secondary"], div.stButton > button[kind="secondary"] * {
    color: #3B6D11 !important;
}
div.stButton > button[kind="secondary"] {
    background-color: transparent;
    border: none;
    border-radius: 6px;
    font-weight: 500;
    font-size: 13px;
    height: 32px;
    box-shadow: none;
    text-align: left;
    justify-content: flex-start;
    padding-left: 4px;
}
div.stButton > button[kind="secondary"]:hover {
    background-color: transparent;
    text-decoration: underline;
}

div[data-testid="stExpander"] {
    background-color: #fff;
    border: 0.5px solid #E5E2D9;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(44,44,42,0.04);
}
div[data-testid="stExpander"] summary {
    color: #3B6D11 !important;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)


try:
    google_logged_in = st.user.is_logged_in
except Exception:
    google_logged_in = False

if not (st.session_state.logged_in or google_logged_in):

    st.markdown("""
    <style>
    div[data-testid="stTextInput"] button {
        background-color: transparent !important;
        border: none !important;
    }
    div[data-testid="stTextInput"] button svg {
        fill: #8A8F98 !important;
    }
    input[type="password"]::-ms-reveal,
    input[type="password"]::-ms-clear {
        display: none;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #fff;
        border-radius: 16px !important;
        padding: 8px 12px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    spacer1, center, spacer2 = st.columns([1, 1.2, 1])

    with center:
        st.markdown("<div style='height:5rem;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

            if st.session_state.auth_mode == "login":
                heading = "Welcome to PULSE"
                subheading = "Log in to continue"
            else:
                heading = "Create your account"
                subheading = "Sign up to get started"

            st.markdown(f"""
            <div style="width:100%;text-align:center;padding-bottom:1.5rem;">
                <div style="display:flex;align-items:center;justify-content:center;gap:6px;margin-bottom:14px;">
                    <span style="width:7px;height:7px;border-radius:50%;background:#639922;"></span>
                    <span style="width:7px;height:7px;border-radius:50%;background:#B4B2A9;"></span>
                    <span style="width:7px;height:7px;border-radius:50%;background:#E24B4A;"></span>
                    <span style="font-weight:600;font-size:14px;letter-spacing:1px;margin-left:6px;">PULSE</span>
                </div>
                <h2 style="text-align:center;width:100%;box-sizing:border-box;margin:0 0 6px 0;font-weight:700;font-size:21px;letter-spacing:-0.2px;">{heading}</h2>
                <p style="text-align:center;color:#888780;font-size:14px;margin:0;">{subheading}</p>
            </div>
            """, unsafe_allow_html=True)

            email = st.text_input("Email", placeholder="name@example.com", label_visibility="collapsed")
            password = st.text_input("Password", placeholder="Password", type="password", label_visibility="collapsed")

            st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

            if st.session_state.auth_mode == "login":
                if st.button("Log In", use_container_width=True, type="primary"):
                    if not email or not password:
                        st.error("Enter your email and password.")
                    else:
                        ok, result = firebase_login(email, password)
                        if ok:
                            st.session_state.logged_in = True
                            st.session_state.user_email = email
                            st.rerun()
                        else:
                            st.error(f"Couldn't sign in. {result}")

                st.markdown("""
                <div style="display:flex;align-items:center;gap:12px;margin:18px 0;">
                    <div style="flex:1;height:1px;background:#E5E2D9;"></div>
                    <span style="color:#B4B2A9;font-size:12px;">or</span>
                    <div style="flex:1;height:1px;background:#E5E2D9;"></div>
                </div>
                """, unsafe_allow_html=True)

                if st.button("Continue with Google", use_container_width=True, type="primary"):
                    try:
                        st.login("google")
                    except Exception as e:
                        st.error(f"Google login isn't configured yet: {e}")

                st.markdown("<div style='height:1.25rem;'></div>", unsafe_allow_html=True)
            else:
                if st.button("Sign Up", use_container_width=True, type="primary"):
                    if not email or not password:
                        st.error("Enter an email and password.")
                    else:
                        ok, result = firebase_signup(email, password)
                        if ok:
                            st.success("Account created. You can log in now.")
                            st.session_state.auth_mode = "login"
                            st.rerun()
                        else:
                            st.error(f"Couldn't sign up. {result}")

                if st.button("Back to login", use_container_width=True, type="primary"):
                    st.session_state.auth_mode = "login"
                    st.rerun()

            st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

    st.stop()

if google_logged_in and not st.session_state.logged_in:
    st.session_state.logged_in = True
    st.session_state.user_email = st.user.email



@st.cache_resource
def get_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)

@st.cache_resource
def get_vader():
    return SentimentIntensityAnalyzer()

@st.cache_resource
def get_bert():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

@st.cache_resource
def get_emotion_classifier():
    return pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base")

@st.cache_resource
def get_sarcasm_classifier():
    return pipeline("text-classification", model="helinivan/english-sarcasm-detector")

@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client["pulse_db"]["sentiment_results"]

@st.cache_resource
def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

@st.cache_resource
def get_summarizer():
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    tokenizer = AutoTokenizer.from_pretrained("csebuetnlp/mT5_multilingual_XLSum")
    model = AutoModelForSeq2SeqLM.from_pretrained("csebuetnlp/mT5_multilingual_XLSum")
    return tokenizer, model

with st.spinner("Loading models... (first run takes longer)"):
    youtube = get_youtube_client()
    vader_analyzer = get_vader()
    bert_classifier = get_bert()
    emotion_classifier = get_emotion_classifier()
    sarcasm_classifier = get_sarcasm_classifier()
    sentiment_collection = get_mongo_collection()
    kafka_producer = get_kafka_producer()
    summarizer_tokenizer, summarizer_model = get_summarizer()



@st.cache_data(ttl=3600)
def fetch_trending_topics(region="IN", max_results=5):
    try:
        request = youtube.videos().list(
            part="snippet", chart="mostPopular", regionCode=region, maxResults=max_results
        )
        response = request.execute()
    except Exception:
        return []

    topics = []
    for item in response.get("items", []):
        title = item["snippet"]["title"]
        short = " ".join(title.split()[:5])
        thumbnail = item["snippet"]["thumbnails"].get("default", {}).get("url", "")
        topics.append({"topic": short, "thumbnail": thumbnail})
    return topics


def search_videos_by_keyword(keyword, max_results=12):
    try:
        request = youtube.search().list(
            part="snippet", q=keyword, type="video",
            order="relevance", maxResults=max_results, regionCode="IN"
        )
        response = request.execute()
    except Exception as e:
        error_text = str(e)
        if "quotaExceeded" in error_text or "quota" in error_text.lower():
            raise PulseError("YouTube API daily quota has been used up. Try again tomorrow, or use a different API key.")
        if "blocked" in error_text.lower() or "forbidden" in error_text.lower():
            raise PulseError("YouTube API key isn't authorized for this request. Check API key restrictions in Google Cloud Console.")
        raise PulseError("Couldn't reach YouTube right now. Check your internet connection and try again.")

    video_ids = [item["id"]["videoId"] for item in response["items"] if "videoId" in item["id"]]
    if not video_ids:
        return []

    try:
        stats_request = youtube.videos().list(part="snippet,statistics", id=",".join(video_ids))
        stats_response = stats_request.execute()
    except Exception:
        return []

    videos = []
    for item in stats_response["items"]:
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "views": item["statistics"].get("viewCount"),
        })
    return videos


def get_video_comments(video_id, max_results=40):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=max_results, order="relevance"
        )
        response = request.execute()
        for item in response["items"]:
            c = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({"text": c["textDisplay"], "author": c["authorDisplayName"], "likes": c["likeCount"]})
    except Exception:
        pass
    return comments


def fetch_news_by_keyword(keyword, max_results=15):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": keyword,
        "apiKey": NEWS_API_KEY,
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": max_results
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            return []
        data = response.json()
    except Exception:
        return []

    articles = []
    for item in data.get("articles", []):
        title = item.get("title") or ""
        description = item.get("description") or ""
        text = f"{title}. {description}".strip()
        source = (item.get("source") or {}).get("name") or "Unknown source"

        if len(text) < 15:
            continue

        articles.append({
            "video_id": item.get("url") or f"news-{uuid.uuid4()}",
            "title": title,
            "channel": source,
            "views": None,
            "comments": [{"text": text, "author": source, "likes": 0}]
        })
    return articles


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
    label = "Positive" if compound >= 0.05 else "Negative" if compound <= -0.05 else "Neutral"
    return label, compound


def get_bert_sentiment(text):
    if not text or len(text.strip()) == 0:
        return "Neutral", 0.0
    result = bert_classifier(text[:512])[0]
    label = "Positive" if result["label"] == "POSITIVE" else "Negative"
    score = result["score"]
    if score < 0.75:  # raised from 0.65 — fewer low-confidence comments get labeled Positive/Negative
        label = "Neutral"
    return label, round(score, 3)


def ensemble_sentiment(v_label, b_label):
    return v_label if v_label == b_label else b_label


def is_sarcastic(text):
    if not text or len(text) < 10:
        return False
    try:
        result = sarcasm_classifier(text[:512])[0]
        return result["label"].lower() in ("sarcastic", "label_1", "1") and result["score"] >= 0.6
    except Exception:
        return False


def compute_emotion_breakdown(comments, sample_size=40):
    candidates = [c for c in comments if c["text"] and len(c["text"]) >= 15]
    candidates.sort(key=lambda c: c.get("confidence", 0), reverse=True)
    sample = candidates[:sample_size]

    if not sample:
        return {}

    emotion_counts = {}
    for c in sample:
        try:
            result = emotion_classifier(c["text"][:512])[0]
            label = result["label"]
            emotion_counts[label] = emotion_counts.get(label, 0) + 1
        except Exception:
            continue

    total = sum(emotion_counts.values())
    if total == 0:
        return {}

    return {label: round(count / total * 100) for label, count in sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True)}


def generate_reason_summary(comments, sentiment_label, keyword, max_input_comments=8):
    candidates = [
        c for c in comments
        if c["sentiment"] == sentiment_label and c["text"] and len(c["text"]) >= 25
    ]

    if not candidates:
        return f"Not enough {sentiment_label.lower()} comments to summarize."

    candidates.sort(key=lambda c: c.get("confidence", 0), reverse=True)
    texts = [c["text"] for c in candidates[:max_input_comments]]
    texts = [t if t.rstrip().endswith((".", "!", "?")) else t.rstrip() + "." for t in texts]

    combined = " ".join(texts)
    combined = combined[:900]

    if len(combined.split()) < 8:
        return f"Not enough {sentiment_label.lower()} comments to summarize."

    try:
        input_ids = summarizer_tokenizer(
            combined, return_tensors="pt", truncation=True, max_length=512
        ).input_ids
        output_ids = summarizer_model.generate(
            input_ids,
            max_length=70,
            min_length=12,
            num_beams=4,
            length_penalty=1.2,
            no_repeat_ngram_size=2
        )
        summary = summarizer_tokenizer.decode(output_ids[0], skip_special_tokens=True).strip()

        if summary and not summary.endswith((".", "!", "?", "।")):
            last_punct = max(summary.rfind("."), summary.rfind("!"), summary.rfind("?"), summary.rfind("।"))
            if last_punct > 15:
                summary = summary[:last_punct + 1]

        return summary.capitalize() if summary else f"Could not generate a summary for {sentiment_label.lower()} comments."
    except Exception:
        return f"Could not generate a summary for {sentiment_label.lower()} comments."


def publish_to_kafka(request_id, keyword, item, comments, platform):
    message = {
        "request_id": request_id,
        "keyword": keyword,
        "platform": platform,
        "video_id": item["video_id"],
        "title": item["title"],
        "channel": item["channel"],
        "views": item.get("views"),
        "comments": comments
    }
    kafka_producer.send(KAFKA_TOPIC, value=message)


def read_messages_from_kafka(request_id, expected_count, timeout_seconds=45):
    group_id = f"pulse-dashboard-{uuid.uuid4()}"
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=group_id,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_seconds * 1000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
    except Exception:
        raise PulseError("Couldn't connect to Kafka. Make sure Docker is running (docker-compose up -d) and try again.")

    messages = []
    for message in consumer:
        if message.value.get("request_id") == request_id:
            messages.append(message.value)
            if len(messages) >= expected_count:
                break

    consumer.close()
    return messages


def analyze_keyword(keyword):
    videos = search_videos_by_keyword(keyword)
    news_articles = fetch_news_by_keyword(keyword)

    if not videos and not news_articles:
        return None

    request_id = str(uuid.uuid4())

    try:
        for video in videos:
            comments = get_video_comments(video["video_id"])
            publish_to_kafka(request_id, keyword, video, comments, platform="YouTube")

        for article in news_articles:
            publish_to_kafka(request_id, keyword, article, article["comments"], platform="News")

        kafka_producer.flush()
    except PulseError:
        raise
    except Exception:
        raise PulseError("Couldn't connect to Kafka. Make sure Docker is running (docker-compose up -d) and try again.")

    total_items = len(videos) + len(news_articles)
    consumed_items = read_messages_from_kafka(request_id, expected_count=total_items)

    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    platform_counts = {"YouTube": 0, "News": 0}
    all_comments = []

    for item in consumed_items:
        platform = item.get("platform", "YouTube")
        for c in item["comments"]:
            cleaned = clean_text(c["text"])
            v_label, v_score = get_vader_sentiment(cleaned)
            b_label, b_score = get_bert_sentiment(cleaned)
            final = ensemble_sentiment(v_label, b_label)
            counts[final] += 1
            platform_counts[platform] = platform_counts.get(platform, 0) + 1
            confidence = abs(v_score) if final == v_label else b_score
            all_comments.append({
                "text": cleaned, "sentiment": final, "video_title": item["title"],
                "platform": platform, "confidence": confidence
            })

        try:
            sentiment_collection.update_one(
                {"video_id": item["video_id"], "keyword": keyword},
                {"$set": {
                    "video_id": item["video_id"], "keyword": keyword, "title": item["title"],
                    "channel": item["channel"], "platform": platform, "views": item.get("views"),
                    "fetched_at": datetime.now(timezone.utc),
                    "total_comments": len(item["comments"])
                }},
                upsert=True
            )
        except Exception:
            pass

    total = len(all_comments)
    return {
        "keyword": keyword,
        "videos": videos,
        "news_articles": news_articles,
        "counts": counts,
        "platform_counts": platform_counts,
        "total": total,
        "comments": all_comments
    }



def render_result(result, key_prefix):
    counts = result["counts"]
    total = result["total"]

    st.markdown(f"<span class='keyword-pill'>{result['keyword']}</span>", unsafe_allow_html=True)
    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    st.markdown(f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 20px 0;'>{total} data points · {len(result['videos'])} YouTube videos · {len(result['news_articles'])} news articles</p>", unsafe_allow_html=True)

    pos_pct = round(counts["Positive"] / total * 100) if total else 0
    neu_pct = round(counts["Neutral"] / total * 100) if total else 0
    neg_pct = round(counts["Negative"] / total * 100) if total else 0

    radius = 58
    circumference = 2 * 3.14159265 * radius

    def donut_segment(percent, offset_percent):
        length = circumference * percent / 100
        dash = f"{length:.2f} {circumference - length:.2f}"
        dashoffset = -(circumference * offset_percent / 100)
        return dash, dashoffset

    pos_dash, pos_offset = donut_segment(pos_pct, 0)
    neu_dash, neu_offset = donut_segment(neu_pct, pos_pct)
    neg_dash, neg_offset = donut_segment(neg_pct, pos_pct + neu_pct)

    dominant_pct = max(pos_pct, neu_pct, neg_pct)
    dominant_label = "positive" if pos_pct == dominant_pct else "negative" if neg_pct == dominant_pct else "neutral"
    dominant_color = "#3B6D11" if dominant_label == "positive" else "#A32D2D" if dominant_label == "negative" else "#5F5E5A"

    st.markdown(f"""
    <div style="background:#fff;border-radius:14px;padding:22px;border:0.5px solid #E5E2D9;box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:16px;display:grid;grid-template-columns:180px 1fr;gap:24px;align-items:center;">
        <div style="display:flex;justify-content:center;">
            <svg width="140" height="140" viewBox="0 0 140 140">
                <circle cx="70" cy="70" r="{radius}" fill="none" stroke="#F0EEE8" stroke-width="16"/>
                <circle cx="70" cy="70" r="{radius}" fill="none" stroke="#639922" stroke-width="16" stroke-dasharray="{pos_dash}" stroke-dashoffset="{pos_offset}" transform="rotate(-90 70 70)"/>
                <circle cx="70" cy="70" r="{radius}" fill="none" stroke="#B4B2A9" stroke-width="16" stroke-dasharray="{neu_dash}" stroke-dashoffset="{neu_offset}" transform="rotate(-90 70 70)"/>
                <circle cx="70" cy="70" r="{radius}" fill="none" stroke="#E24B4A" stroke-width="16" stroke-dasharray="{neg_dash}" stroke-dashoffset="{neg_offset}" transform="rotate(-90 70 70)"/>
                <text x="70" y="65" text-anchor="middle" font-size="26" font-weight="700" fill="{dominant_color}">{dominant_pct}%</text>
                <text x="70" y="85" text-anchor="middle" font-size="11" fill="#5F5E5A">{dominant_label}</text>
            </svg>
        </div>
        <div style="display:flex;flex-direction:column;justify-content:center;gap:10px;">
            <div style="display:flex;align-items:center;gap:10px;">
                <span style="width:10px;height:10px;border-radius:50%;background:#639922;"></span>
                <span style="color:#2C2C2A;font-size:14px;flex:1;">Positive</span>
                <span style="font-weight:700;color:#3B6D11;">{pos_pct}%</span>
            </div>
            <div style="display:flex;align-items:center;gap:10px;">
                <span style="width:10px;height:10px;border-radius:50%;background:#B4B2A9;"></span>
                <span style="color:#2C2C2A;font-size:14px;flex:1;">Neutral</span>
                <span style="font-weight:700;color:#5F5E5A;">{neu_pct}%</span>
            </div>
            <div style="display:flex;align-items:center;gap:10px;">
                <span style="width:10px;height:10px;border-radius:50%;background:#E24B4A;"></span>
                <span style="color:#2C2C2A;font-size:14px;flex:1;">Negative</span>
                <span style="font-weight:700;color:#A32D2D;">{neg_pct}%</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    reason_key = f"reason_summaries_{key_prefix}_{result['keyword']}"
    if reason_key not in st.session_state:
        with st.spinner("Summarizing why people feel this way..."):
            pos_reason = generate_reason_summary(result["comments"], "Positive", result["keyword"])
            neg_reason = generate_reason_summary(result["comments"], "Negative", result["keyword"])
        st.session_state[reason_key] = (pos_reason, neg_reason)
    else:
        pos_reason, neg_reason = st.session_state[reason_key]

    st.markdown(f"""
    <div style="background:#fff;border-radius:14px;padding:6px;border:0.5px solid #E5E2D9;box-shadow:0 1px 3px rgba(44,44,42,0.05);">
        <div style="padding:14px 16px;border-bottom:0.5px solid #F0EEE8;">
            <p style="font-size:13px;font-weight:600;margin:0 0 6px;color:#3B6D11;">Positive because</p>
            <p style="color:#5F5E5A;font-size:13px;line-height:1.6;margin:0;">{pos_reason}</p>
        </div>
        <div style="padding:14px 16px;">
            <p style="font-size:13px;font-weight:600;margin:0 0 6px;color:#A32D2D;">Negative because</p>
            <p style="color:#5F5E5A;font-size:13px;line-height:1.6;margin:0;">{neg_reason}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

    platform_data = {}
    for platform in ["YouTube", "News"]:
        platform_comments = [c for c in result["comments"] if c["platform"] == platform]
        p_total = len(platform_comments)
        if p_total == 0:
            continue
        p_pos = sum(1 for c in platform_comments if c["sentiment"] == "Positive")
        p_neu = sum(1 for c in platform_comments if c["sentiment"] == "Neutral")
        p_neg = sum(1 for c in platform_comments if c["sentiment"] == "Negative")
        platform_data[platform] = {
            "total": p_total,
            "pos": round(p_pos / p_total * 100),
            "neu": round(p_neu / p_total * 100),
            "neg": round(p_neg / p_total * 100)
        }

    if len(platform_data) >= 1:
        rows_html = ""
        for platform, stats in platform_data.items():
            rows_html += (
                f'<div style="margin-bottom:16px;">'
                f'<div style="display:flex;justify-content:space-between;margin-bottom:6px;">'
                f'<span style="font-size:13px;font-weight:600;color:#2C2C2A;">{platform}</span>'
                f'<span style="font-size:12px;color:#5F5E5A;">{stats["total"]} items</span>'
                f'</div>'
                f'<div style="display:flex;width:100%;height:10px;border-radius:6px;overflow:hidden;background:#F0EEE8;">'
                f'<div style="width:{stats["pos"]}%;background:#639922;"></div>'
                f'<div style="width:{stats["neu"]}%;background:#B4B2A9;"></div>'
                f'<div style="width:{stats["neg"]}%;background:#E24B4A;"></div>'
                f'</div>'
                f'<div style="display:flex;justify-content:space-between;margin-top:4px;">'
                f'<span style="font-size:11px;color:#3B6D11;">{stats["pos"]}% positive</span>'
                f'<span style="font-size:11px;color:#A32D2D;">{stats["neg"]}% negative</span>'
                f'</div>'
                f'</div>'
            )

        st.markdown(
            f'<div style="background:#fff;border-radius:14px;padding:20px;border:0.5px solid #E5E2D9;box-shadow:0 1px 3px rgba(44,44,42,0.05);">'
            f'<p style="font-size:13px;font-weight:600;margin:0 0 16px;color:#2C2C2A;">Platform comparison</p>'
            f'{rows_html}'
            f'</div>',
            unsafe_allow_html=True
        )

    st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

    with st.expander("Show emotion breakdown (optional)", key=f"{key_prefix}_emotion_exp"):
        emotion_key = f"emotions_{key_prefix}_{result['keyword']}"
        if emotion_key not in st.session_state:
            with st.spinner("Analyzing emotions in a sample of comments..."):
                st.session_state[emotion_key] = compute_emotion_breakdown(result["comments"])

        emotions = st.session_state[emotion_key]

        if not emotions:
            st.caption("Not enough data to compute an emotion breakdown.")
        else:
            emotion_colors = {
                "joy": "#639922", "surprise": "#3B8AD9", "neutral": "#B4B2A9",
                "anger": "#E24B4A", "disgust": "#B15FC9", "fear": "#D98A2B", "sadness": "#5F7EA6"
            }
            for label, pct in emotions.items():
                color = emotion_colors.get(label, "#5F5E5A")
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px;'>"
                    f"<span style='width:80px;font-size:13px;color:#2C2C2A;text-transform:capitalize;'>{label}</span>"
                    f"<div style='flex:1;background:#F0EEE8;border-radius:6px;height:8px;overflow:hidden;'>"
                    f"<div style='width:{pct}%;background:{color};height:100%;'></div>"
                    f"</div>"
                    f"<span style='font-size:12px;color:#5F5E5A;width:36px;text-align:right;'>{pct}%</span>"
                    f"</div>",
                    unsafe_allow_html=True
                )

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    with st.expander("Show sample comments", key=f"{key_prefix}_comments_exp"):
        for c in result["comments"][:15]:
            color = "#3B6D11" if c["sentiment"] == "Positive" else "#A32D2D" if c["sentiment"] == "Negative" else "#5F5E5A"
            sarcastic = is_sarcastic(c["text"])
            sarcasm_badge = "<span style='color:#B15FC9;font-weight:600;font-size:11px;margin-left:8px;'>SARCASM?</span>" if sarcastic else ""
            st.markdown(
                f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;'>"
                f"<span style='color:{color};font-weight:600;font-size:12px;'>{c['sentiment'].upper()}</span>{sarcasm_badge}<br>"
                f"<span style='color:#2C2C2A;font-size:13px;'>{c['text'][:150]}</span>"
                f"</div>",
                unsafe_allow_html=True
            )


hcol1, hcol2 = st.columns([4, 1])
with hcol1:
    st.markdown("""
    <div class="pulse-brand">
        <span class="pulse-dot" style="background:#639922;"></span>
        <span class="pulse-dot" style="background:#B4B2A9;"></span>
        <span class="pulse-dot" style="background:#E24B4A;"></span>
        <span style="color:#2C2C2A;font-weight:600;font-size:18px;margin-left:6px;">PULSE</span>
    </div>
    """, unsafe_allow_html=True)
    st.caption("Real-time public sentiment monitoring — YouTube & News")
with hcol2:
    if st.button("Log out", use_container_width=True, type="primary"):
        st.session_state.logged_in = False
        if hasattr(st, "user") and st.user.is_logged_in:
            st.logout()
        st.rerun()


if "keyword_input" not in st.session_state:
    st.session_state["keyword_input"] = ""

if "pending_trending_keyword" in st.session_state:
    st.session_state["keyword_input"] = st.session_state.pop("pending_trending_keyword")
    st.session_state["trigger_analyze"] = True

col1, col2 = st.columns([4, 1])
with col1:
    keyword = st.text_input("Enter any topic or keyword", placeholder="e.g. Budget 2026, IPL, iPhone 17...", label_visibility="collapsed", key="keyword_input")
with col2:
    search_clicked = st.button("Analyze", use_container_width=True, type="primary")

trending_topics = fetch_trending_topics()
if trending_topics:
    st.markdown("""
    <div style="background:#fff;border-radius:14px;padding:20px 22px;border:0.5px solid #E5E2D9;box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-top:16px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
            <div style="width:40px;height:40px;border-radius:10px;background:#EAF3DE;display:flex;align-items:center;justify-content:center;font-size:18px;">🔥</div>
            <div>
                <div style="display:flex;align-items:center;gap:8px;">
                    <span style="font-size:16px;font-weight:700;color:#2C2C2A;">Trending on YouTube</span>
                    <span style="background:#EAF3DE;color:#3B6D11;font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;letter-spacing:0.5px;">LIVE</span>
                </div>
                <p style="color:#888780;font-size:12px;margin:2px 0 0 0;">What's hot and popular right now</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    card_cols = st.columns(len(trending_topics))
    for i, item in enumerate(trending_topics):
        with card_cols[i]:
            short_title = item["topic"] if len(item["topic"]) <= 20 else item["topic"][:18] + "..."
            thumb = item["thumbnail"]
            st.markdown(f"""
            <div style="background:#fff;border:0.5px solid #E5E2D9;border-radius:10px;padding:10px;margin-top:8px;display:flex;align-items:center;gap:8px;">
                <span style="background:#EAF3DE;color:#3B6D11;font-size:11px;font-weight:700;width:20px;height:20px;border-radius:6px;display:flex;align-items:center;justify-content:center;flex-shrink:0;">{i+1}</span>
                <img src="{thumb}" style="width:28px;height:28px;border-radius:50%;object-fit:cover;flex-shrink:0;">
                <span style="font-size:12px;color:#2C2C2A;font-weight:500;line-height:1.3;">{short_title}</span>
            </div>
            """, unsafe_allow_html=True)

            if st.button("● Analyze this", key=f"trending_chip_{i}", use_container_width=True, type="secondary"):
                st.session_state["pending_trending_keyword"] = item["topic"]
                st.rerun()

auto_triggered = st.session_state.pop("trigger_analyze", False)

if (search_clicked or auto_triggered) and not keyword.strip():
    st.warning("Enter a keyword first, then click Analyze.")

if (search_clicked or auto_triggered) and keyword.strip():
    try:
        with st.spinner(f"Fetching live YouTube data for '{keyword}'..."):
            result = analyze_keyword(keyword.strip())

        if result is None or result["total"] == 0:
            st.warning("No videos or comments found for this topic. Try a more popular or recent keyword.")
        else:
            st.session_state["last_result"] = result
            st.session_state.pop(f"reason_summaries_main_{result['keyword']}", None)
            st.session_state.pop(f"emotions_main_{result['keyword']}", None)

    except PulseError as e:
        st.error(str(e))
    except Exception:
        st.error("Something went wrong while analyzing this topic. Please try again in a moment.")

st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

compare_mode = st.checkbox("Compare two keywords instead")

if compare_mode:
    cc1, cc2, cc3 = st.columns([2, 2, 1])
    with cc1:
        keyword_a = st.text_input("Keyword A", placeholder="e.g. iPhone 17", label_visibility="collapsed")
    with cc2:
        keyword_b = st.text_input("Keyword B", placeholder="e.g. Samsung S26", label_visibility="collapsed")
    with cc3:
        compare_clicked = st.button("Compare", use_container_width=True, type="primary")

    if compare_clicked:
        if not keyword_a.strip() or not keyword_b.strip():
            st.warning("Enter both keywords to compare.")
        else:
            try:
                with st.spinner(f"Comparing '{keyword_a}' and '{keyword_b}'..."):
                    result_a = analyze_keyword(keyword_a.strip())
                    result_b = analyze_keyword(keyword_b.strip())

                if (result_a is None or result_a["total"] == 0) or (result_b is None or result_b["total"] == 0):
                    st.warning("Not enough data found for one or both keywords. Try different topics.")
                else:
                    st.session_state.pop(f"reason_summaries_a_{result_a['keyword']}", None)
                    st.session_state.pop(f"emotions_a_{result_a['keyword']}", None)
                    st.session_state.pop(f"reason_summaries_b_{result_b['keyword']}", None)
                    st.session_state.pop(f"emotions_b_{result_b['keyword']}", None)
                    st.session_state["compare_results"] = (result_a, result_b)

            except PulseError as e:
                st.error(str(e))
            except Exception:
                st.error("Something went wrong while comparing these topics. Please try again in a moment.")
if "last_result" in st.session_state and not compare_mode:
    render_result(st.session_state["last_result"], key_prefix="main")

if compare_mode and "compare_results" in st.session_state:
    result_a, result_b = st.session_state["compare_results"]
    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
    comp_col1, comp_col2 = st.columns(2)
    with comp_col1:
        render_result(result_a, key_prefix="a")
    with comp_col2:
        render_result(result_b, key_prefix="b")

elif not compare_mode and "last_result" not in st.session_state:
    st.info("Enter a keyword above and click Analyze to see live sentiment.")
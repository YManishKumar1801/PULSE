import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from telethon.sync import TelegramClient
from pyvis.network import Network as PyvisNetwork
from datetime import datetime, timezone
import html
import re
import requests
import json
import uuid
import math
import time
import os
import asyncio
from dotenv import load_dotenv
from ingestion.normalizer import normalize
from intelligence.audience import build_audience_segments, segments_for_keyword
from config.env import describe_missing, get_int_env
from ingestion.sources import SourceError, fetch_news, fetch_telegram_messages, fetch_youtube_comments, search_telegram_channels as search_channels, search_youtube_videos
from intelligence.explain import explain_signal
from intelligence.gemini_client import generate_text
from intelligence.network import analyze_network
from intelligence.overview import TIMELINE_RANGES, audience_overview, command_center_summary, network_overview, signal_card, signals_csv, signals_overview, sentiment_overview, timeline_window, trends_overview, worker_summary
from intelligence.sentiment import analyze_events, analyze_text
from intelligence.signals import run_pipeline
from intelligence.trends import detect_topic_trends
from ui_sentiment import (
    mood_bar_html,
    compare_summary_html,
    language_name,
    overall_headline,
    ranked_bars_html,
    single_summary_html,
    stacked_bars_html,
)
from storage.mongodb import fetch_events, fetch_signals, fetch_worker_status, save_analyses, save_events, save_signals

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_API_ID = get_int_env("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "pulse-youtube"

FIREBASE_SIGNIN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_API_KEY}"
FIREBASE_SIGNUP_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={FIREBASE_API_KEY}"

st.set_page_config(page_title="PULSE - Live Audience Intelligence", page_icon="📡", layout="wide", initial_sidebar_state="expanded")

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
.stApp h1 a, .stApp h2 a, .stApp h3 a, .stApp h4 a { display: none !important; }
.stApp { background-color: #FAF9F6; }
.block-container { padding-top: 5rem; max-width: 1200px; }

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

section[data-testid="stSidebar"] { border-right: 1px solid #E5E2D9; }
section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h1 * {
    color: #3B6D11 !important; font-weight: 800; letter-spacing: 3px; font-size: 1.9rem;
}
section[data-testid="stSidebar"] div[role="radiogroup"] { gap: 2px; }
section[data-testid="stSidebar"] div[role="radiogroup"] label {
    padding: 8px 12px; border-radius: 10px; width: 100%;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:hover { background: #E8E5DA; }
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
    background: #E1EDD2; font-weight: 600;
}

div[data-testid="stMetricValue"] { font-weight: 800; }
div[data-testid="stMetricLabel"] p { color: #5F5E5A !important; font-weight: 600; }

h2, h3 { letter-spacing: -0.2px; }
h3 { color: #2A4D0C !important; margin-top: 1.2rem; }

.st-key-hero {
    background: linear-gradient(135deg, #EAF3DE 0%, #F7F4EA 100%);
    border: 1px solid #D9E6C6; border-radius: 16px; padding: 18px 24px;
}
.st-key-hero h2 { color: #2A4D0C !important; }

[class*="st-key-kpi_"] { border-radius: 14px; border-top-width: 4px; box-shadow: 0 1px 3px rgba(44,44,42,0.05); }
.st-key-kpi_green { background: #F2F8EA; border-top-color: #3B6D11; }
.st-key-kpi_blue { background: #EDF4FB; border-top-color: #378ADD; }
.st-key-kpi_amber { background: #FDF5E6; border-top-color: #D98A2B; }
.st-key-kpi_purple { background: #F4EFF9; border-top-color: #7F5AA2; }
.st-key-kpi_grey { background: #F5F4F0; border-top-color: #B4B2A9; }
.st-key-kpi_red { background: #FEF4F3; border-top-color: #E24B4A; }

[class*="st-key-sig_"] { border-radius: 14px; border-left-width: 5px; box-shadow: 0 1px 3px rgba(44,44,42,0.05); }
[class*="st-key-sig_high"] { background: #FEF6F5; border-left-color: #C0392B; }
[class*="st-key-sig_medium"] { background: #FEF9EE; border-left-color: #D98A2B; }
[class*="st-key-sig_low"] { background: #F7F6F2; border-left-color: #888780; }

[class*="st-key-mood_"] { border-radius: 14px; box-shadow: 0 1px 3px rgba(44,44,42,0.05); }
[class*="st-key-mood_pos"] { background: #F2F8EA; border-color: #CFE0B8; }
[class*="st-key-mood_neu"] { background: #F5F4F0; border-color: #DEDBD0; }
[class*="st-key-mood_neg"] { background: #FEF4F3; border-color: #F0CBC7; }

[class*="st-key-trend_"] { border-radius: 14px; border-left-width: 5px; box-shadow: 0 1px 3px rgba(44,44,42,0.05); }
[class*="st-key-trend_amber"] { background: #FEF9EE; border-left-color: #D98A2B; }
[class*="st-key-trend_green"] { background: #F2F8EA; border-left-color: #3B6D11; }
[class*="st-key-trend_blue"] { background: #EDF4FB; border-left-color: #378ADD; }
[class*="st-key-trend_grey"] { background: #F7F6F2; border-left-color: #888780; }
[class*="st-key-trend_red"] { background: #FEF4F3; border-left-color: #C0392B; }
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
def get_telegram_loop():
    return asyncio.new_event_loop()

@st.cache_resource
def get_telegram_client():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError("TELEGRAM_API_ID or TELEGRAM_API_HASH is not set")
    loop = get_telegram_loop()
    asyncio.set_event_loop(loop)
    client = TelegramClient("pulse_session", TELEGRAM_API_ID, TELEGRAM_API_HASH, loop=loop)
    client.start()
    return client

@st.cache_resource
def warm_up_sentiment_engine():
    analyze_text("This is a short sentence used to load the sentiment models.")
    return True

@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client["pulse_db"]["sentiment_results"]

@st.cache_resource
def get_telegram_collection():
    client = MongoClient(MONGO_URI)
    return client["pulse_db"]["telegram_sentiment_results"]

@st.cache_resource
def get_demographics_collection():
    client = MongoClient(MONGO_URI)
    return client["pulse_db"]["demographics_results"]


@st.cache_resource
def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def safe_init(name, factory):
    try:
        return factory(), None
    except Exception as error:
        return None, f"{name}: {error}"


with st.spinner("Loading models... (first run takes longer)"):
    startup_errors = []
    youtube, error = safe_init("YouTube", get_youtube_client)
    if error:
        startup_errors.append(error)
    telegram_client, error = safe_init("Telegram", get_telegram_client)
    if error:
        startup_errors.append(error)
    sentiment_ready, error = safe_init("Sentiment models", warm_up_sentiment_engine)
    if error:
        startup_errors.append(error)
    sentiment_collection, error = safe_init("MongoDB", get_mongo_collection)
    if error:
        startup_errors.append(error)
    telegram_collection, error = safe_init("MongoDB", get_telegram_collection)
    demographics_collection, error = safe_init("MongoDB", get_demographics_collection)
    kafka_producer, error = safe_init("Kafka", get_kafka_producer)
    if error:
        startup_errors.append("Kafka: the broker is not reachable. Start it with docker-compose up -d.")

missing_notes = [
    describe_missing("YouTube search", ["YOUTUBE_API_KEY"]),
    describe_missing("News search", ["NEWS_API_KEY"]),
    describe_missing("Gemini explanations", ["GEMINI_API_KEY"]),
    describe_missing("Database features", ["MONGO_URI"]),
    describe_missing("Telegram search", ["TELEGRAM_API_ID", "TELEGRAM_API_HASH"]),
]
missing_notes = [note for note in missing_notes if note]
if startup_errors or missing_notes:
    with st.expander("Some services are unavailable. The rest of PULSE still works.", expanded=False):
        for note in missing_notes:
            st.markdown(f"- {note}")
        for note in startup_errors:
            st.markdown(f"- {note}")


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
        return search_youtube_videos(youtube, keyword, max_results)
    except SourceError as error:
        raise PulseError(str(error))


def get_video_comments(video_id, max_results=40):
    return fetch_youtube_comments(youtube, video_id, max_results)


def fetch_news_by_keyword(keyword, max_results=15):
    articles, warning = fetch_news(NEWS_API_KEY, keyword, max_results)
    if warning:
        st.warning(warning)
    return articles


PLATFORM_LABELS = {"youtube": "YouTube", "news": "News", "telegram": "Telegram"}


def compute_emotion_breakdown(comments):
    emotion_counts = {}
    for c in comments:
        emotion = c.get("emotion")
        if emotion:
            label = emotion["label"]
            emotion_counts[label] = emotion_counts.get(label, 0) + 1

    total = sum(emotion_counts.values())
    if total == 0:
        return {}

    ordered = sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True)
    return {label: round(count / total * 100) for label, count in ordered}


def analyze_and_store_events(events):
    if not events:
        return []
    try:
        save_events(events)
    except Exception as e:
        print("Event storage failed:", e)
    results = analyze_events(events)
    try:
        save_analyses(results)
    except Exception as e:
        print("Analysis storage failed:", e)
    return results


def build_comment_records(events, results):
    analysis_by_id = dict(results)
    records = []
    for event in events:
        analysis = analysis_by_id.get(event["event_id"])
        if analysis is None:
            continue
        sentiment = analysis["sentiment"]
        records.append({
            "event_id": event["event_id"],
            "text": analysis["cleaned_text"],
            "sentiment": sentiment["label"],
            "confidence": sentiment["confidence_proxy"],
            "platform": PLATFORM_LABELS.get(event["platform"], event["platform"]),
            "video_title": event.get("source_name"),
            "language": analysis["language"]["name"],
            "emotion": analysis.get("emotion"),
            "sarcasm": analysis.get("sarcasm")
        })
    return records


def generate_gemini_summary(prompt):
    result = generate_text(prompt, GEMINI_API_KEY)
    if result["error"]:
        print("Gemini error:", result["error"])
    return result["text"]


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

    gemini_prompt = (
        f"These are real comments about '{keyword}'. In one clear, complete sentence, "
        f"explain the main reason people feel {sentiment_label.lower()} about it, "
        f"based on this content: {combined}"
    )
    gemini_result = generate_gemini_summary(gemini_prompt)
    if gemini_result:
        return gemini_result.strip()

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
        "raw": item.get("raw"),
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


def search_telegram_channels(keyword, max_results=5):
    if telegram_client is not None:
        asyncio.set_event_loop(get_telegram_loop())
    return search_channels(telegram_client, keyword, max_results)


def fetch_channel_messages(username, limit=50):
    if telegram_client is None:
        return []
    asyncio.set_event_loop(get_telegram_loop())
    messages, error = fetch_telegram_messages(telegram_client, username, limit)
    if error:
        st.warning(error)
    return messages


def analyze_telegram_keyword(keyword):
    channels, search_error = search_telegram_channels(keyword)
    if not channels:
        return None, search_error

    all_results = []
    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}

    for ch in channels:
        messages = fetch_channel_messages(ch["username"])

        telegram_events = normalize(
            "telegram",
            [{
                "channel_title": ch["title"],
                "channel_username": ch["username"],
                "messages": messages
            }],
            keyword
        )
        results = analyze_and_store_events(telegram_events)
        analysis_by_id = dict(results)

        message_docs = []

        for event in telegram_events:
            analysis = analysis_by_id.get(event["event_id"])
            if analysis is None:
                continue
            label = analysis["sentiment"]["label"]
            counts[label] += 1
            message_docs.append({
                "text": analysis["cleaned_text"],
                "sentiment": label,
                "date": event["timestamp"],
                "sender_id": event["author_id"]
            })

        document = {
            "channel_title": ch["title"],
            "channel_username": ch["username"],
            "keyword": keyword,
            "platform": "Telegram",
            "fetched_at": datetime.now(timezone.utc),
            "messages": message_docs,
            "total_messages": len(message_docs)
        }

        try:
            telegram_collection.update_one(
                {"channel_username": ch["username"], "keyword": keyword},
                {"$set": document},
                upsert=True
            )
        except Exception as error:
            st.warning(f"Couldn't save @{ch['username']} to the database: {error}")

        all_results.append(document)

    total = sum(len(r["messages"]) for r in all_results)
    return {"channels": all_results, "counts": counts, "total": total}, None


COMMUNITY_COLORS = [
    "#639922", "#3B8AD9", "#D98A2B", "#B15FC9", "#E24B4A",
    "#2BA6A0", "#8A6D3B", "#D4537E", "#5F7EA6", "#7A8B2E"
]
NETWORK_MAX_NODES = 300


def community_color(community_id):
    if not community_id:
        return "#B4B2A9"
    return COMMUNITY_COLORS[(int(community_id.split("_")[1]) - 1) % len(COMMUNITY_COLORS)]


def generate_network_html(G, output_file="network_graph.html"):
    if G.number_of_nodes() > NETWORK_MAX_NODES:
        keep = sorted(G.nodes, key=lambda n: G.nodes[n].get("pagerank", 0), reverse=True)[:NETWORK_MAX_NODES]
        G = G.subgraph(keep)

    max_rank = max([data.get("pagerank", 0) for _, data in G.nodes(data=True)], default=0) or 1
    net = PyvisNetwork(height="600px", width="100%", bgcolor="#FAF9F6", font_color="#2C2C2A")
    for node, data in G.nodes(data=True):
        color = community_color(data.get("community"))
        is_bridge = bool(data.get("bridge"))
        title = (
            f"{html.escape(str(data.get('label', node)))}<br>{data.get('type')} · {data.get('messages', 0)} messages"
            f"<br>{data.get('community') or 'minor community'}{' · bridge node' if is_bridge else ''}"
        )
        net.add_node(
            node,
            label=str(data.get("label", node)),
            title=title,
            shape="dot" if data.get("type") == "author" else "square",
            size=10 + 30 * data.get("pagerank", 0) / max_rank,
            borderWidth=4 if is_bridge else 1,
            color={"background": color, "border": "#2C2C2A" if is_bridge else color}
        )
    for source, target, data in G.edges(data=True):
        net.add_edge(source, target, value=data.get("weight", 1))
    net.write_html(output_file)
    return output_file


def analyze_keyword(keyword):
    if kafka_producer is None:
        raise PulseError("Kafka is not running. Start it with docker-compose up -d and restart the app.")
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

    events = []
    for item in consumed_items:
        if item.get("platform") == "News" and item.get("raw"):
            events.extend(normalize("news", [item["raw"]], keyword))
        elif item.get("platform", "YouTube") == "YouTube":
            events.extend(normalize("youtube", [item], keyword))

    results = analyze_and_store_events(events)
    all_comments = build_comment_records(events, results)

    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    platform_counts = {"YouTube": 0, "News": 0}
    for c in all_comments:
        counts[c["sentiment"]] += 1
        platform_counts[c["platform"]] = platform_counts.get(c["platform"], 0) + 1

    for item in consumed_items:
        platform = item.get("platform", "YouTube")
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



def render_result(result, key_prefix, show_summary=True):
    counts = result["counts"]
    total = result["total"]

    st.markdown(f"<span class='keyword-pill'>{result['keyword']}</span>", unsafe_allow_html=True)
    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
    st.markdown(f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 20px 0;'>{total} data points · {len(result['videos'])} YouTube videos · {len(result['news_articles'])} news articles</p>", unsafe_allow_html=True)

    if show_summary:
        st.subheader("How do people feel?")
        st.markdown(single_summary_html(counts, total), unsafe_allow_html=True)
        st.caption("Each comment or news item is classified as positive, neutral or negative by PULSE's sentiment models.")

    reason_key = f"reason_summaries_{key_prefix}_{result['keyword']}"
    if reason_key not in st.session_state:
        with st.spinner("Summarizing why people feel this way..."):
            pos_reason = generate_reason_summary(result["comments"], "Positive", result["keyword"])
            neg_reason = generate_reason_summary(result["comments"], "Negative", result["keyword"])
        if not (pos_reason.startswith("Could not generate") or neg_reason.startswith("Could not generate")):
            st.session_state[reason_key] = (pos_reason, neg_reason)
    else:
        pos_reason, neg_reason = st.session_state[reason_key]

    st.subheader("Why do people feel this way?")
    st.caption("AI-written summaries of what the positive and the negative comments are about.")
    with st.container(border=True, key=f"mood_pos_{key_prefix}"):
        st.markdown("**🟢 Positive because**")
        st.markdown(md_safe(pos_reason))
    with st.container(border=True, key=f"mood_neg_{key_prefix}"):
        st.markdown("**🔴 Negative because**")
        st.markdown(md_safe(neg_reason))

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
        st.subheader("How do the sources compare?")
        st.caption("Share of positive, neutral and negative messages on each source.")
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
            sarcastic = bool(c.get("sarcasm") and c["sarcasm"]["flag"])
            sarcasm_badge = "<span style='color:#B15FC9;font-weight:600;font-size:11px;margin-left:8px;'>SARCASM?</span>" if sarcastic else ""
            st.markdown(
                f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;'>"
                f"<span style='color:{color};font-weight:600;font-size:12px;'>{c['sentiment'].upper()}</span>{sarcasm_badge}<br>"
                f"<span style='color:#2C2C2A;font-size:13px;'>{c['text'][:150]}</span>"
                f"</div>",
                unsafe_allow_html=True
            )


def render_segment_card(card):
    with st.container(border=True, key=f"mood_{card['tone']}_seg{card['rank']}"):
        st.markdown(f"#### 👥 Group {card['rank']}: {md_safe(card['title'])}")
        tag = f"Topic area: {card['topic_tag']} · " if card["topic_tag"] else ""
        st.caption(f"{tag}{card['size_pct']}% of all messages · {card['count']:,} messages")
        st.markdown(md_safe(card["summary"]))
        st.markdown("**How do they feel?**")
        if card["shares"]:
            st.markdown(mood_bar_html(card["shares"]), unsafe_allow_html=True)
        else:
            st.caption("Not enough analysed messages in this group yet.")
        col_lang, col_src = st.columns(2)
        with col_lang:
            st.markdown("**Languages**")
            for item in card["languages"]:
                st.markdown(f"{md_safe(item['name'])} · {item['share']}%")
        with col_src:
            st.markdown("**Where they write**")
            for item in card["platforms"]:
                st.markdown(f"{md_safe(item['platform'])} · {item['share']}%")
        if card["reactions"]:
            st.markdown(f"**How much people react:** {card['reactions']}")
            st.caption(card["reactions_note"])
        if card["trend_text"]:
            st.markdown(f"**Over time:** {card['trend_text']}")
        with st.expander("Words and example messages"):
            if card["words"]:
                st.caption("Words that define this group: " + ", ".join(md_safe(word) for word in card["words"]))
            for text in card["examples"]:
                st.markdown(f"> {md_safe(text)}")


def render_audience_segments(result):
    if result["status"] != "ok":
        st.warning(
            f"Not enough data yet. PULSE found {result['usable_events']} usable messages and needs at least {result['min_required']}. "
            "Open Live Search and collect more messages first."
        )
        return

    overview = audience_overview(result)
    st.subheader("Who is talking")
    st.markdown(f"**{overview['headline']}**")
    st.caption("Groups are made from similar words and topics. They describe conversations, not individual people.")

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card("Messages grouped", f"{overview['messages']:,}", "Messages PULSE looked at", "Messages with enough text to be grouped.", "🟢", "green")
    with kpi2:
        metric_card("Groups found", overview["groups"], "Different kinds of conversation", "Each group is a set of messages that use similar words.", "🔵", "blue")
    with kpi3:
        metric_card("Biggest group", f"{overview['biggest_pct']}%", "Share of all messages", "How much of the conversation the largest group covers.", "🟠", "amber")
    with kpi4:
        metric_card("Growing groups", overview["growing"], "Getting a bigger share", "Groups whose share grew between the earlier and later half of the data.", "🟣", "purple")

    st.subheader("The groups")
    st.caption("Biggest group first. The card colour shows the overall mood: green positive, grey neutral, red negative.")
    for card in overview["cards"]:
        render_segment_card(card)

    if overview["links"]:
        st.subheader("Which group talks about which keyword")
        st.caption("For each of your most searched keywords, the group that talks about it the most.")
        with st.container(border=True, key="mood_neu_links"):
            for row in overview["links"]:
                st.markdown(
                    f"**{md_safe(row['keyword'])}** → {md_safe(row['group'])}  \n"
                    f"{row['share']}% of this keyword's messages are in that group, "
                    f"which is {row['lift']}x more than its share of all messages."
                )

    with st.expander("How was this worked out?"):
        notes = [
            "PULSE turns each message into word weights and then groups messages that use similar words.",
            "No age, gender or personal details are guessed. Groups only reflect what people wrote.",
        ]
        if overview["quality"] is not None:
            notes.append(f"Group separation score: {overview['quality']} (closer to 1 means cleaner groups).")
        if overview["quality_low"]:
            notes.append("This score is low, so the groups overlap a lot. Treat them as a rough guide, not sharp boundaries.")
        notes.append("Language names come from automatic detection and can be wrong for mixed Hindi-English text.")
        st.markdown("\n".join(f"- {note}" for note in notes))


def render_trend_card(card):
    with st.container(border=True, key=f"trend_{card['tone']}_{card['topic_id']}"):
        st.markdown(f"#### {card['icon']} {md_safe(card['title'])}")
        st.caption(card["status_label"])
        col_before, col_now, col_change = st.columns(3)
        col_before.metric("Messages before", card["before"], help="Messages about this topic in the earlier time block.")
        col_now.metric("Messages now", card["now"], help="Messages about this topic in the latest time block.")
        col_change.metric("Change", card["change"], help="How much the message count changed. 'New' means there were none before.")
        st.markdown(f"**What is happening:** {md_safe(card['explanation'])}")
        st.markdown(f"**How people feel:** {md_safe(card['mood'])}")
        if card["repeated_warning"]:
            st.warning(
                f"About {card['repeated_share']}% of the messages in this topic are repeated or near-identical. "
                "That can mean copy-pasted promotion or spam rather than real discussion."
            )
        if card["searched_for"]:
            st.caption("Collected while searching for: " + ", ".join(md_safe(word) for word in card["searched_for"]))
        with st.expander("See example messages"):
            if card["words"]:
                st.caption("Words that define this topic: " + ", ".join(md_safe(word) for word in card["words"]))
            for example in card["examples"]:
                source = f"**{example['platform']}**  " if example["platform"] else ""
                st.markdown(f"{source}\n> {md_safe(example['text'])}")


def render_topic_trends(result):
    if result["status"] == "insufficient_data":
        st.warning(
            f"Not enough data yet. PULSE found {result['usable_events']} usable messages and needs at least {result['min_required']}. "
            "Open Live Search and collect more messages first."
        )
        return
    if result["status"] == "insufficient_history":
        st.warning("All collected messages are from a very short stretch of time, so PULSE can't tell what is growing yet. Collect data over a longer period.")
        return

    overview = trends_overview(result)
    window = overview["window_hours"]
    st.subheader("What changed")
    st.markdown(f"**{overview['headline']}**")
    st.caption(
        f"PULSE compared the latest {window:g} hours with the {window:g} hours before it, using {overview['messages']} messages."
    )

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card("Topics found", overview["total"], "Groups of messages about the same thing", "Each topic is a group of messages that use similar words.", "🔵", "blue")
    with kpi2:
        metric_card("New topics", overview["new"], "Just appeared", "Topics that were almost absent before and suddenly showed up.", "🆕", "amber")
    with kpi3:
        metric_card("Growing topics", overview["growing"], "Getting more messages", "Topics that already existed and are clearly growing.", "📈", "green")
    with kpi4:
        metric_card("Fading topics", overview["fading"], "Getting fewer messages", "Topics that lost a lot of attention.", "📉", "red")

    if not overview["sections"]:
        st.info("No topic had enough messages to be shown. Collect more data and try again.")
    for section in overview["sections"]:
        st.subheader(section["title"])
        st.caption(section["caption"])
        for card in section["cards"]:
            render_trend_card(card)

    with st.expander("How was this worked out?"):
        notes = [
            "PULSE groups messages that use similar words into topics, then counts how many messages each topic got in the latest time block and in the block before it.",
            f"Topics with fewer than {overview['min_volume']} messages in a block are left out ({overview['dropped']} left out this time).",
            "A topic is called new or growing only when the increase is bigger than normal ups and downs.",
        ]
        if not overview["persistence_evaluated"]:
            notes.append("Steady and long-running topics need more history than is collected so far, so that label is not used yet.")
        st.markdown("\n".join(f"- {note}" for note in notes))


def render_community_card(card):
    with st.container(border=True, key=f"mood_{card['tone']}_com{card['rank']}"):
        st.markdown(f"#### 🧩 {card['name']}: {md_safe(card['title'])}")
        st.caption(f"{card['authors']:,} {'person or account' if card['authors'] == 1 else 'people or accounts'} · {card['sources']:,} {'channel or video' if card['sources'] == 1 else 'channels or videos'} · {card['messages']:,} messages")
        if card["single_voice"]:
            st.info("This is mostly one account or channel posting many messages, so it is not a real crowd of people.")
        st.markdown("**How do they feel?**")
        if card["shares"]:
            st.markdown(mood_bar_html(card["shares"]), unsafe_allow_html=True)
        else:
            st.caption("Not enough analysed messages in this community yet.")
        if card["main_sources"]:
            st.markdown("**Where they mostly write:** " + "; ".join(f"{md_safe(item['label'])} ({item['messages']})" for item in card["main_sources"]))
        if card["platforms"]:
            st.caption("Platforms: " + ", ".join(f"{item['platform']} {item['share']}%" for item in card["platforms"]))
        with st.expander("Words and topics"):
            if card["words"]:
                st.caption("Words this community uses more than others: " + ", ".join(md_safe(word) for word in card["words"]))
            if card["topics"]:
                st.markdown("\n".join(f"- {md_safe(item['name'])} ({item['count']} messages)" for item in card["topics"]))
            else:
                st.caption("No topic had enough messages here.")


def render_node_list(title, rows, caption):
    st.markdown(f"**{title}**")
    st.caption(caption)
    if not rows:
        st.caption("Nothing to show yet.")
    for row in rows:
        st.markdown(f"{md_safe(row['label'])}")
        st.progress(min(1.0, max(0.0, row["level"])))
        st.caption(f"{row['connections']} connections · {row['messages']} messages · {row['community']} · {row['mood']}")


def render_network_intelligence(result, graph):
    if result["status"] != "ok":
        st.warning(
            f"Not enough network data yet. PULSE found {result['network_events']} messages with a known author and needs at least {result['min_required']}. "
            "Collect more YouTube or Telegram messages in Live Search first."
        )
        for note in result["limitations"]:
            st.caption(note)
        return

    overview = network_overview(result)
    st.subheader("Who is connected to whom")
    st.markdown(f"**{overview['headline']}**")
    st.caption("A community is a set of people and channels that mostly interact with each other.")

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card("People or accounts", f"{overview['people']:,}", "Who wrote the messages", "Distinct authors found in the collected messages.", "🟢", "green")
    with kpi2:
        metric_card("Channels and videos", f"{overview['sources']:,}", "Where they wrote", "Distinct channels or videos the messages came from.", "🔵", "blue")
    with kpi3:
        metric_card("Connections", f"{overview['connections']:,}", "Person to channel links", "Each connection means a person wrote in a channel or video.", "🟠", "amber")
    with kpi4:
        metric_card("Communities", len(overview["communities"]), f"With {overview['min_messages']}+ messages", "Communities too small to matter are left out.", "🟣", "purple")

    st.subheader("Communities")
    st.caption("Biggest first. The card colour shows the overall mood: green positive, grey neutral, red negative.")
    if overview["single_voice_count"]:
        st.caption(f"{overview['single_voice_count']} of these are really one account or channel posting a lot, not a crowd.")
    for card in overview["communities"]:
        render_community_card(card)

    st.subheader("Connectors between communities")
    st.caption("People or channels that are active in more than one community. They help ideas travel between groups.")
    if not overview["bridges"]:
        st.info("None found yet. These appear when the same person is active in channels that belong to different communities.")
    for row in overview["bridges"]:
        with st.container(border=True, key=f"mood_neu_bridge{abs(hash(row['label'])) % 100000}"):
            st.markdown(f"**🔗 {md_safe(row['label'])}**")
            st.caption(f"{row['kind']} · {row['platform']}")
            st.markdown(md_safe(row["text"]))

    st.subheader("Most connected")
    st.caption("Ranked by how well connected they are to other well-connected nodes. This shows position in the network, not real-world influence.")
    col_people, col_sources = st.columns(2)
    with col_people:
        render_node_list("People and accounts", overview["top_people"], "Who is most central.")
    with col_sources:
        render_node_list("Channels and videos", overview["top_sources"], "Which places are most central.")

    st.subheader("How topics spread")
    st.caption("For each topic, which communities talked about it and in what order.")
    if not overview["spreads"]:
        st.info("No topic had enough messages in a community yet. Collect more messages over a longer time and try again.")
    for item in overview["spreads"]:
        with st.container(border=True, key=f"trend_{item['tone']}_spread{abs(hash(item['name'])) % 100000}"):
            st.markdown(f"#### {item['icon']} {md_safe(item['name'])}")
            st.caption(item["label"])
            st.markdown(md_safe(item["reach"]))
            for step in item["steps"]:
                st.markdown(f"- {md_safe(step)}")

    st.subheader("Network map")
    st.caption("Each dot is a person (round) or a channel or video (square). Colours are communities, a thick border marks a connector, and bigger means more central. You can drag and zoom.")
    html_file = generate_network_html(graph)
    with open(html_file, "r", encoding="utf-8") as f:
        graph_html = f.read()
    st.components.v1.html(graph_html, height=620)

    if overview["notes"]:
        with st.expander("Things to keep in mind"):
            st.markdown("\n".join(f"- {md_safe(note)}" for note in overview["notes"]))
            st.markdown("- Communities are found automatically from who writes where, so they are a rough guide.")


PAGES = [
    "Command Center",
    "Live Search",
    "Sentiment",
    "Trends",
    "Audience",
    "Network",
    "PULSE Signals",
]

with st.sidebar:
    st.title("PULSE")
    st.caption("Live audience intelligence")
    page = st.radio("Go to", PAGES, key="nav_page", label_visibility="collapsed")
    st.divider()
    st.caption(f"Signed in as {st.session_state.get('user_email', '')}")
    if st.button("Log out", use_container_width=True):
        st.session_state.logged_in = False
        if hasattr(st, "user") and st.user.is_logged_in:
            st.logout()
        st.rerun()


SIGNAL_PRIORITY_COLORS = {"high": "#A32D2D", "medium": "#D98A2B", "low": "#5F5E5A"}
VECTOR_LABELS = {"trend": "Trend", "sentiment": "Sentiment", "audience": "Audience", "network": "Network"}


def render_signal_card(signal, key_prefix):
    card = signal_card(signal)
    with st.container(border=True, key=f"sig_{card['priority']}_{key_prefix}"):
        st.markdown(f"#### {card['icon']} {md_safe(card['title'])}")
        st.caption(f"{card['kind_label']} · {PRIORITY_ICONS.get(card['priority'], '')} {card['priority_name']}")
        st.markdown(md_safe(card["priority_meaning"]))
        col_score, col_conf, col_agree = st.columns(3)
        col_score.metric("Score", f"{card['score']:.0f} / 100", help="A combined score used to rank signals. It is a guide, not a probability.")
        col_conf.metric("Rough confidence", f"{card['confidence']}%", help="Based on how much data there is and how many checks agree. It is a rough guide, not a calibrated probability.")
        col_agree.metric("Other checks agree", f"{card['agree_count']} of 3", help="Mood, audience and spread are checked on top of growth. This is how many of them back the signal up.")
        if card["agree_names"]:
            st.caption("Backed up by: " + ", ".join(card["agree_names"]))
        if card["missing_names"]:
            st.caption("Not available yet: " + ", ".join(card["missing_names"]))

        st.markdown(f"**What is happening:** {md_safe(card['happening'])}")
        st.markdown(f"**How fast:** {md_safe(card['speed'])}")
        st.markdown(f"**Who is talking:** {md_safe(card['people'])}")
        st.markdown(f"**How people feel:** {md_safe(card['mood'])}")
        st.markdown(f"**How far it has spread:** {md_safe(card['spread'])}")
        if card["why"]:
            st.markdown(f"**Why it matters:** {md_safe(card['why'])}")

        if card["repeated_warning"]:
            st.warning(
                f"About {card['repeated_share']}% of the messages in this topic are repeated or near-identical. "
                "That can mean copy-pasted promotion or spam rather than real discussion."
            )
        context = []
        if card["searched_for"]:
            context.append("Collected while searching for: " + ", ".join(md_safe(word) for word in card["searched_for"]))
        if card["platforms"]:
            context.append("Platforms: " + ", ".join(f"{row['platform']} {row['share']}%" for row in card["platforms"]))
        if context:
            st.caption(" · ".join(context))

        explanation_key = f"explanation_{signal['signal_id']}"
        if st.button("Explain in simple words", key=f"explain_btn_{key_prefix}"):
            with st.spinner("Writing an explanation from the detected evidence..."):
                st.session_state[explanation_key] = explain_signal(signal, llm=generate_gemini_summary)
        if explanation_key in st.session_state:
            explanation = st.session_state[explanation_key]
            if explanation["source"] == "gemini":
                source_note = "Written by Gemini using only the detected evidence. Its numbers were checked against that evidence."
            else:
                reasons = {
                    "llm_unavailable": "Gemini was unavailable",
                    "ungrounded_numbers": "the Gemini text contained numbers not in the evidence",
                    "too_long": "the Gemini text was too long",
                    "empty": "Gemini returned nothing",
                    "no_llm": "no Gemini key is configured",
                }
                source_note = f"Built directly from the detected evidence ({reasons.get(explanation['reason'], explanation['reason'])})."
            st.info(explanation["text"])
            st.caption(source_note)
            with st.expander("Evidence given to the explainer"):
                st.code(explanation["facts"], language=None)

        with st.expander("Examples and score details"):
            if card["examples"]:
                st.markdown("**Example messages from this topic**")
                for example in card["examples"]:
                    source = f"**{example['platform']}**  " if example["platform"] else ""
                    st.markdown(f"{source}\n> {md_safe(example['text'])}")
            if card["words"]:
                st.caption("Words that define this topic: " + ", ".join(md_safe(word) for word in card["words"]))
            st.markdown("**How the score was worked out**")
            breakdown = []
            for name in ("trend", "sentiment", "audience", "network"):
                value = signal["components"].get(name)
                label = CHECK_LABELS[name]
                if value is None:
                    breakdown.append(f"- {label}: not available")
                else:
                    weight = signal["weights_used"].get(name, 0)
                    breakdown.append(f"- {label}: {round(value * 100)} out of 100, counts for {round(weight * 100)}% of the score")
            st.markdown("\n".join(breakdown))
            st.caption(
                "The weights are simple starting choices and have not been tested against real outcomes. "
                "If a check is missing, the others share its weight."
            )


CHECK_LABELS = {"trend": "Growth", "sentiment": "Mood change", "audience": "Audience focus", "network": "Spread across communities"}


def render_pulse_signals(result):
    if result["status"] == "insufficient_data":
        st.warning(
            f"Not enough data yet. PULSE found {result['usable_events']} usable messages and needs at least {result['min_required']}. "
            "Open Live Search and collect more messages first."
        )
        return
    if result["status"] == "insufficient_history":
        st.warning("All collected messages are from a very short stretch of time, so PULSE can't tell what is growing yet. Collect data over a longer period.")
        return

    overview = signals_overview(result)
    st.subheader("What needs attention")
    st.markdown(f"**{overview['headline']}**")
    st.caption(
        f"PULSE compared the latest {overview['window_hours']:g} hours with the {overview['window_hours']:g} hours before it, using {overview['messages']:,} messages."
    )

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card("Signals found", overview["total"], "Topics worth a look", "A signal is a new or growing topic that scored high enough.", "📡", "blue")
    with kpi2:
        metric_card("High priority", overview["high"], "Look at these first", "Several checks agree these matter.", "🔴", "red")
    with kpi3:
        metric_card("New topics", overview["new"], "Just appeared", "Topics that were almost absent before.", "🆕", "amber")
    with kpi4:
        metric_card("Growing topics", overview["growing"], "Getting more messages", "Topics that already existed and are growing.", "📈", "green")

    if not overview["cards"]:
        st.info("No new or growing topic scored high enough in this window. Collect more messages and try again.")
    else:
        st.subheader("Signals")
        st.caption("Strongest first. The card colour shows priority: red high, orange medium, grey low.")
        st.download_button(
            "Download signals as CSV",
            data=signals_csv(result["signals"]),
            file_name="pulse_signals.csv",
            mime="text/csv",
            key="download_signals_csv"
        )
    for position, signal in enumerate(result["signals"]):
        render_signal_card(signal, key_prefix=f"signal_{position}")

    with st.expander("What did PULSE check?"):
        names = {"ok": "done", "insufficient_data": "not enough data", "insufficient_history": "not enough history"}
        lines = [f"- {name}: {names.get(value, value)}" for name, value in overview["checks"].items()]
        lines.append("- Mood: read from the messages that were analysed")
        st.markdown("\n".join(lines))
        st.caption("A signal needs a new or growing topic with a score of at least 25 out of 100.")


def render_saved_signals(signals):
    if not signals:
        st.info("No saved signals yet.")
        return
    for position, signal in enumerate(signals):
        render_signal_card(signal, key_prefix=f"saved_{position}")
        st.caption(f"First saved {signal.get('created_at', 'unknown')} · last updated {signal.get('updated_at', 'unknown')}")


SENTIMENT_COLORS = ["#3B6D11", "#888780", "#A32D2D"]


@st.cache_data(ttl=300, show_spinner=False)
def load_intelligence():
    events = fetch_events(limit=20000)
    return events, run_pipeline(events)


def kpi_card(label, value, note="", accent="#3B6D11"):
    st.markdown(
        f"<div style='background:#fff;border-radius:14px;padding:16px 18px;border:0.5px solid #E5E2D9;"
        f"border-top:3px solid {accent};box-shadow:0 1px 3px rgba(44,44,42,0.05);min-height:104px;'>"
        f"<div style='font-size:11px;color:#888780;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;'>{html.escape(label)}</div>"
        f"<div style='font-size:30px;font-weight:800;color:#2C2C2A;margin-top:4px;line-height:1.1;'>{value}</div>"
        f"<div style='font-size:12px;color:#888780;margin-top:6px;'>{html.escape(note)}</div></div>",
        unsafe_allow_html=True
    )


def sentiment_frame(counts_by_label):
    return pd.DataFrame(
        {"Sentiment": ["Positive", "Neutral", "Negative"], "Messages": [counts_by_label["Positive"], counts_by_label["Neutral"], counts_by_label["Negative"]]}
    ).set_index("Sentiment")


def md_safe(text):
    return re.sub(r"([\\`*_{}\[\]()#+!|<>~$])", r"\\\1", str(text))


PRIORITY_ICONS = {"high": "🔴", "medium": "🟠", "low": "⚪"}
KIND_ICONS = {"emerging": "🆕", "rising": "📈"}


KPI_CARD_HEIGHT = 175


def metric_card(label, value, note, help_text, icon, tone):
    with st.container(border=True, height=KPI_CARD_HEIGHT, key=f"kpi_{tone}"):
        st.metric(f"{icon} {label}", value, help=help_text)
        st.caption(note)


def render_how_to_read():
    with st.expander("How to read this page"):
        st.markdown(
            "- **Topic**: a group of messages that talk about the same thing. PULSE finds these automatically and names each one with its most common words.\n"
            "- **New topic (Emerging)**: a topic that was almost absent before and has suddenly appeared.\n"
            "- **Growing topic (Rising)**: a topic that already existed and is getting clearly more messages.\n"
            "- **Priority**: how strongly different checks agree that the topic matters. Trend growth, sentiment change, audience concentration and spread across communities are all checked.\n"
            "- **Score (0-100)**: a simple combined number from those checks. It is a guide for ranking, not a probability.\n"
            "- **Latest window**: the most recent block of time PULSE compares against the block right before it."
        )


def render_signal_highlight(item, position):
    with st.container(border=True, key=f"sig_{item['priority']}_{position}"):
        st.markdown(f"#### {KIND_ICONS.get(item['kind'], '')} {md_safe(item['title'])}")
        st.caption(
            f"{item['kind_label']} · {PRIORITY_ICONS.get(item['priority'], '')} {item['priority'].title()} priority · score {item['score']:.0f} / 100"
        )
        if item["what"]:
            st.markdown(f"**What is happening:** {md_safe(item['what'])}")
        if item["why"]:
            st.markdown(f"**Why it matters:** {md_safe(item['why'])}")
        if item["repeated_warning"]:
            st.warning(
                f"About {item['repeated_share']}% of the messages in this topic are repeated or near-identical. "
                "That can mean copy-pasted promotion or spam rather than real discussion, so check the examples below."
            )
        context = []
        if item["searched_for"]:
            context.append("Collected while searching for: " + ", ".join(md_safe(word) for word in item["searched_for"]))
        if item["platforms"]:
            context.append("Platforms: " + ", ".join(f"{row['platform']} {row['share']}%" for row in item["platforms"]))
        if context:
            st.caption(" · ".join(context))
        if item["examples"]:
            st.markdown("**Example messages from this topic**")
            st.caption("These are the messages that match this topic's keywords most closely. They are here to show what the topic is about.")
            for example in item["examples"]:
                source = f"**{example['platform']}**  " if example["platform"] else ""
                st.markdown(f"{source}\n> {md_safe(example['text'])}")


def render_command_center():
    with st.container(key="hero"):
        st.header("Command Center")
        st.caption(
            "PULSE reads YouTube, Telegram and news messages and shows which conversations are growing right now, who is talking, and how they feel."
        )
        if st.button("Refresh intelligence", key="cc_refresh_btn"):
            load_intelligence.clear()
    render_how_to_read()

    try:
        events, pipeline = load_intelligence()
    except Exception as e:
        st.error(f"Couldn't load collected data: {e}")
        return

    summary = command_center_summary(events, pipeline)
    if summary["total_events"] == 0:
        st.info("No collected data yet. Open Live Search in the sidebar and collect some messages first.")
        return

    new_count = summary["emerging_signals"]
    growing_count = summary["rising_signals"]
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card(
            "Messages collected",
            f"{summary['total_events']:,}",
            f"{summary['analyzed_events']:,} checked for mood",
            "Every YouTube comment, Telegram message and news article stored so far.",
            "🟢",
            "green",
        )
    with kpi2:
        metric_card(
            "Topics found",
            summary["active_topics"],
            "groups of similar messages",
            "PULSE groups messages that talk about the same thing. Only groups with enough messages are counted.",
            "🔵",
            "blue",
        )
    with kpi3:
        metric_card(
            "Topics taking off",
            new_count + growing_count,
            f"{new_count} new · {growing_count} growing",
            "New topics just appeared. Growing topics already existed and are getting clearly more messages. Both are listed below.",
            "🟠",
            "amber",
        )
    with kpi4:
        metric_card(
            "Audience groups",
            summary["segments"],
            "grouped by what they write",
            "People are grouped by the topics, language and tone of what they write. Age, gender or identity are never guessed.",
            "🟣",
            "purple",
        )

    if summary["pipeline_status"] == "insufficient_history":
        st.warning("All messages come from a very short time span, so PULSE cannot tell what is growing yet. Collect data over a longer period.")
    elif summary["pipeline_status"] != "ok":
        st.warning("There are not enough messages yet to find topics and measure growth. Collect more data first.")

    window = summary["window_hours"]
    st.subheader("Topics to watch right now")
    st.caption("Topics that are new or growing fastest. Open PULSE Signals in the sidebar for full evidence and an explanation.")
    if not summary["highlights"]:
        st.info("Nothing is taking off right now. No topic has grown enough to be flagged in the latest window.")
    for position, item in enumerate(summary["highlights"]):
        render_signal_highlight(item, position)

    st.subheader("How fast is each topic changing?")
    if summary["velocity"] and window:
        st.caption(
            f"Number of messages about each topic in the latest {window:g} hours, compared with the {window:g} hours before that."
        )
        top_value = max([row["current"] for row in summary["velocity"]] + [row["previous"] for row in summary["velocity"]] + [1])
        table = pd.DataFrame(
            {
                "Topic": [row["title"] for row in summary["velocity"]],
                "Status": [row["status_label"] for row in summary["velocity"]],
                "Before": [row["previous"] for row in summary["velocity"]],
                "Now": [row["current"] for row in summary["velocity"]],
                "Change": [row["growth_text"] for row in summary["velocity"]],
            }
        )
        st.dataframe(
            table,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Topic": st.column_config.TextColumn("Topic", width="large"),
                "Status": st.column_config.TextColumn("Status", width="medium"),
                "Before": st.column_config.ProgressColumn(
                    f"Messages before ({window:g}h)", min_value=0, max_value=top_value, format="%d"
                ),
                "Now": st.column_config.ProgressColumn(
                    f"Messages now ({window:g}h)", min_value=0, max_value=top_value, format="%d"
                ),
                "Change": st.column_config.TextColumn("Change", width="small"),
            },
        )
    else:
        st.caption("Not enough history to measure how topics are changing yet.")

    st.subheader("How do people feel?")
    overview = sentiment_overview(events)
    if overview["analyzed_events"]:
        shares = overview["shares"]
        counts = overview["distribution"]
        sentiment_cols = st.columns(3)
        for column, label, icon, tone in zip(sentiment_cols, ("Positive", "Neutral", "Negative"), ("🟢", "⚪", "🔴"), ("pos", "neu", "neg")):
            with column:
                with st.container(border=True, key=f"mood_{tone}"):
                    st.metric(f"{icon} {label}", f"{round(shares[label] * 100)}%")
                    st.caption(f"{counts[label]:,} messages")
        st.caption(f"Based on {overview['analyzed_events']:,} of {overview['total_events']:,} messages that have been checked for sentiment.")
    else:
        st.caption("No messages have been checked for sentiment yet. Run analyze_pending.py to check stored messages.")

    render_system_status(summary)


def vector_problem(status):
    if status == "insufficient_history":
        return "the collected messages cover too short a time span"
    return "more collected messages are needed"


def render_system_status(summary):
    with st.expander("Is everything working? (System status)"):
        st.markdown("**1. Analysis checks**")
        st.caption("Each analysis needs enough collected messages to work. A warning here just means: collect more data.")
        vectors = summary["vectors"] or {}
        checks = (
            ("Topic detection", "trends", "finds what people are talking about"),
            ("Audience groups", "audience", "groups people by what they write"),
            ("Network", "network", "finds communities and how topics spread between them"),
        )
        for name, key, meaning in checks:
            status = vectors.get(key)
            if status == "ok":
                st.markdown(f"✅ **{name}**: ready ({meaning})")
            else:
                st.markdown(f"⚠️ **{name}**: not ready, {vector_problem(status)} ({meaning})")

        st.markdown("**2. Background worker (optional)**")
        st.caption(
            "A worker is a small program that keeps collecting and analysing new messages by itself. "
            "You can use this dashboard without it. Start it only if you want the data to update automatically."
        )
        try:
            workers = worker_summary(fetch_worker_status())
        except Exception:
            workers = None
        if workers is None:
            st.markdown("⚠️ Worker status could not be loaded. Check the database connection.")
        elif not workers:
            st.markdown("⚪ **No worker is running.** To start one, open a new terminal and run:")
            st.code("python workers/run_worker.py", language="bash")
        else:
            for row in workers:
                if row["alive"]:
                    waiting = "unknown" if row["consumer_lag"] is None else row["consumer_lag"]
                    st.markdown(
                        f"✅ **{row['worker_id']}** is running. Messages waiting in the queue: {waiting}. "
                        f"Messages stored so far: {row['stored']}."
                    )
                else:
                    st.markdown(f"⚠️ **{row['worker_id']}** has stopped reporting. Restart it with `python workers/run_worker.py`.")
            with st.expander("Technical details"):
                for row in workers:
                    age = "never" if row["heartbeat_age_seconds"] is None else f"{row['heartbeat_age_seconds']}s ago"
                    p95 = "n/a" if row["batch_seconds_p95"] is None else f"{row['batch_seconds_p95']}s"
                    st.caption(
                        f"{row['worker_id']}: last heartbeat {age} · analyzed {row['analyzed']} · malformed {row['malformed']} · "
                        f"analysis failures {row['analysis_failures']} · batch P95 {p95}"
                    )


def render_sentiment_page():
    with st.container(key="hero"):
        st.header("Sentiment")
        st.caption("How people feel about everything PULSE has collected so far, across YouTube, Telegram and news.")
    try:
        events, _ = load_intelligence()
    except Exception as e:
        st.error(f"Couldn't load collected data: {e}")
        return

    overview = sentiment_overview(events)
    if not overview["analyzed_events"]:
        st.info("No messages have been analyzed yet. Run python analyze_pending.py to check the stored messages.")
        return

    counts = overview["distribution"]
    total = overview["analyzed_events"]
    shares = overview["shares"]
    st.markdown(f"**{overall_headline(counts, total)}**")
    st.caption(f"Based on {total:,} of {overview['total_events']:,} collected messages that have been analyzed.")

    with st.expander("What do these terms mean?"):
        st.markdown(
            "- **Positive / Neutral / Negative**: every message is classified by PULSE's sentiment models.\n"
            "- **Sarcasm flagged**: messages that look sarcastic, where the literal words may mean the opposite. This is an approximate check and only works for English.\n"
            "- **Emotion**: the main feeling in a message, such as joy or anger. Also English only.\n"
            "- **Language**: detected automatically, so it can be wrong for very short or mixed-language text such as Hinglish."
        )

    st.subheader("How do people feel overall?")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        metric_card("Positive", f"{round(shares['Positive'] * 100)}%", f"{counts['Positive']:,} messages", "Messages that express a positive opinion or feeling.", "🟢", "green")
    with kpi2:
        metric_card("Neutral", f"{round(shares['Neutral'] * 100)}%", f"{counts['Neutral']:,} messages", "Messages that are factual, mixed or have no clear opinion.", "⚪", "grey")
    with kpi3:
        metric_card("Negative", f"{round(shares['Negative'] * 100)}%", f"{counts['Negative']:,} messages", "Messages that express a negative opinion or feeling.", "🔴", "red")
    with kpi4:
        sarcasm = overview["sarcasm"]
        if sarcasm["evaluated"]:
            metric_card("Sarcasm flagged", f"{round(sarcasm['share'] * 100)}%", f"of {sarcasm['evaluated']:,} English messages", "Share of English messages that look sarcastic. This is an approximate check.", "🟣", "purple")
        else:
            metric_card("Sarcasm flagged", "n/a", "no English messages checked", "Sarcasm is only checked for English messages.", "🟣", "purple")

    st.subheader("How has the mood changed over time?")
    timeline_rows = overview["timeline"]
    unit = overview["timeline_unit"]
    if len(timeline_rows) >= 2:
        days = None
        range_label = "all collected data"
        if unit == "day":
            chosen = st.radio("Time range", list(TIMELINE_RANGES), index=1, horizontal=True, key="sentiment_range")
            days = TIMELINE_RANGES[chosen]
            range_label = chosen.lower() + " of data" if days else "all collected data"
        rows, shown_unit = timeline_window(timeline_rows, unit, days)
        if len(rows) >= 2:
            frame = pd.DataFrame(rows)
            frame["bucket"] = pd.to_datetime(frame["bucket"])
            st.bar_chart(frame.set_index("bucket")[["Positive", "Neutral", "Negative"]], color=SENTIMENT_COLORS)
            st.caption(f"Number of messages per {shown_unit}, split by mood. Showing {range_label}.")
        else:
            st.caption("This time range has only one time bucket of messages, so a timeline can't be drawn. Try a longer range.")
    else:
        st.caption("All analyzed messages fall into a single time bucket, so a timeline can't be drawn yet.")

    st.subheader("How do the sources compare?")
    st.caption("Share of positive, neutral and negative messages on each platform.")
    platform_rows = [
        {"name": PLATFORM_LABELS.get(name, str(name).title()), "total": values["total"], "counts": {label: values[label] for label in ("Positive", "Neutral", "Negative")}}
        for name, values in overview["by_platform"].items()
    ]
    st.markdown(stacked_bars_html(platform_rows), unsafe_allow_html=True)

    st.subheader("Does the mood differ by language?")
    if overview["by_language"]:
        st.caption("Languages with at least a few analyzed messages. Language is detected automatically and can be wrong for short or mixed-language text, so treat small groups with care.")
        language_rows = [
            {"name": language_name(row["language"]), "total": row["total"], "counts": {label: row[label] for label in ("Positive", "Neutral", "Negative")}}
            for row in overview["by_language"]
        ]
        st.markdown(stacked_bars_html(language_rows), unsafe_allow_html=True)
    else:
        st.caption("No language has enough analyzed messages for a comparison yet.")

    st.subheader("What emotions do people show?")
    if overview["emotions"]:
        emotion_total = sum(row["count"] for row in overview["emotions"])
        st.caption("The main feeling detected in each English message. Messages in other languages are not included here.")
        st.markdown(
            ranked_bars_html([(row["emotion"], row["count"]) for row in overview["emotions"]], emotion_total),
            unsafe_allow_html=True,
        )
    else:
        st.caption("No emotion data stored yet. Run analyze_pending.py without skipping emotion to fill it in.")


if page == "Command Center":
    render_command_center()

if page == "Sentiment":
    render_sentiment_page()

def render_live_youtube_news():
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
        st.subheader("Trending on YouTube right now")
        st.caption("Click 'Analyze this' under any trending video to see how people feel about it.")

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

    compare_mode = st.checkbox("Compare two topics side by side")

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
        st.subheader("Side-by-side mood")
        st.markdown(
            compare_summary_html(
                result_a["keyword"], result_a["counts"], result_a["total"],
                result_b["keyword"], result_b["counts"], result_b["total"],
            ),
            unsafe_allow_html=True,
        )
        st.subheader("Details for each topic")
        comp_col1, comp_col2 = st.columns(2)
        with comp_col1:
            render_result(result_a, key_prefix="a", show_summary=False)
        with comp_col2:
            render_result(result_b, key_prefix="b", show_summary=False)

    elif not compare_mode and "last_result" not in st.session_state:
        st.info("Type a topic above, or click 'Analyze this' on a trending video, to see how people feel about it.")

def render_telegram_search():
    st.caption("PULSE looks up public Telegram channels related to your topic and checks the mood of their recent messages.")
    tg_col1, tg_col2 = st.columns([4, 1])
    with tg_col1:
        telegram_keyword = st.text_input("Search Telegram", placeholder="e.g. cricket, news, finance...", label_visibility="collapsed", key="telegram_keyword_input")
    with tg_col2:
        telegram_search_clicked = st.button("Analyze", use_container_width=True, type="primary", key="telegram_analyze_btn")

    if telegram_search_clicked and not telegram_keyword.strip():
        st.warning("Enter a keyword first, then click Analyze.")

    if telegram_search_clicked and telegram_keyword.strip():
        with st.spinner(f"Searching Telegram for '{telegram_keyword}'..."):
            telegram_result, telegram_error = analyze_telegram_keyword(telegram_keyword.strip())

        if telegram_error:
            st.error(f"Telegram search failed: {telegram_error}")
        elif telegram_result is None or telegram_result["total"] == 0:
            st.warning("No Telegram channels or messages found for this topic. Try a more common keyword.")
        else:
            st.session_state["telegram_last_result"] = telegram_result

    if "telegram_last_result" in st.session_state:
        tresult = st.session_state["telegram_last_result"]
        tcounts = tresult["counts"]
        ttotal = tresult["total"]

        st.markdown(f"<span class='keyword-pill'>{html.escape(telegram_keyword.strip())}</span>", unsafe_allow_html=True)
        st.caption(f"{ttotal} messages across {len(tresult['channels'])} channels")
        st.subheader("How do people feel?")
        st.markdown(single_summary_html(tcounts, ttotal), unsafe_allow_html=True)
        st.caption("Each message is classified as positive, neutral or negative by PULSE's sentiment models.")

        with st.expander("Show channels and sample messages"):
            for ch in tresult["channels"]:
                st.markdown(f"**{ch['channel_title']}** (@{ch['channel_username']}) — {ch['total_messages']} messages")
                for m in ch["messages"][:5]:
                    color = "#3B6D11" if m["sentiment"] == "Positive" else "#A32D2D" if m["sentiment"] == "Negative" else "#5F5E5A"
                    st.markdown(f"<span style='color:{color};font-weight:600;font-size:12px;'>{m['sentiment'].upper()}</span> {m['text'][:150]}", unsafe_allow_html=True)
                st.markdown("---")


if page == "Live Search":
    with st.container(key="hero"):
        st.header("Live Search")
        st.caption("Pick a topic and PULSE checks how people feel about it right now.")
        live_source = st.radio("Where should PULSE look?", ["YouTube & News", "Telegram"], horizontal=True, key="live_source")
    if live_source == "YouTube & News":
        render_live_youtube_news()
    else:
        render_telegram_search()

if page == "Trends":
    with st.container(key="hero"):
        st.header("Trends")
        st.caption("See which conversations are growing, steady or fading, and how people feel about each one.")
    with st.expander("How to read this page"):
        st.markdown(
            "- **Topic**: a group of messages that talk about the same thing. Its name is made from its most common words.\n"
            "- **New**: almost nobody talked about it before, now people do.\n"
            "- **Growing**: it already existed and is getting more messages.\n"
            "- **Steady**: about the same attention as before.\n"
            "- **Fading**: fewer messages than before.\n"
            "- **Messages before / now**: how many messages the topic got in the earlier and the latest time block."
        )

    trend_col1, trend_col2 = st.columns([4, 1])
    with trend_col1:
        trend_keyword = st.text_input(
            "Trend keyword",
            placeholder="Optional: look at one keyword only, or leave empty to use everything collected",
            label_visibility="collapsed",
            key="trend_keyword_input"
        )
    with trend_col2:
        trend_clicked = st.button("Find trends", use_container_width=True, type="primary", key="detect_trends_btn")

    if trend_clicked:
        with st.spinner("Grouping messages into topics and checking how each one changed..."):
            try:
                trend_events = fetch_events(keyword=trend_keyword.strip() or None, limit=20000)
                st.session_state["trend_results"] = detect_topic_trends(trend_events)
            except Exception as e:
                st.session_state.pop("trend_results", None)
                st.error(f"Couldn't find trends: {e}")

    if "trend_results" in st.session_state:
        render_topic_trends(st.session_state["trend_results"])
    else:
        st.info("Press Find trends to see which topics are changing.")


if page == "Audience":
    with st.container(key="hero"):
        st.header("Audience")
        st.caption("See who is taking part in the conversation: the different groups, what they talk about, and how they feel.")
    with st.expander("How to read this page"):
        st.markdown(
            "- **Group**: messages that use similar words, put together by PULSE. Each group is a kind of conversation.\n"
            "- **Share of messages**: how much of everything collected belongs to that group.\n"
            "- **How do they feel**: the split of positive, neutral and negative messages inside the group.\n"
            "- **Languages and Where they write**: the main languages and platforms in that group.\n"
            "- **Over time**: whether the group's share is growing, steady or shrinking."
        )

    seg_col1, seg_col2 = st.columns([4, 1])
    with seg_col1:
        segment_keyword = st.text_input(
            "Segment keyword",
            placeholder="Optional: look at one keyword only, or leave empty to use everything collected",
            label_visibility="collapsed",
            key="segment_keyword_input"
        )
    with seg_col2:
        segment_clicked = st.button("Find groups", use_container_width=True, type="primary", key="build_segments_btn")

    if segment_clicked:
        with st.spinner("Grouping messages into audience groups..."):
            try:
                segment_events = fetch_events(keyword=segment_keyword.strip() or None, limit=20000)
                st.session_state["audience_result"] = build_audience_segments(segment_events)
            except Exception as e:
                st.session_state.pop("audience_result", None)
                st.error(f"Couldn't find audience groups: {e}")

    if "audience_result" in st.session_state:
        render_audience_segments(st.session_state["audience_result"])
    else:
        st.info("Press Find groups to see who is talking.")


if page == "Network":
    with st.container(key="hero"):
        st.header("Network")
        st.caption("See how people and channels are connected, which communities they form, and how topics travel between them.")
    with st.expander("How to read this page"):
        st.markdown(
            "- **Person or account**: someone who wrote a comment or message.\n"
            "- **Channel or video**: the place where they wrote it.\n"
            "- **Community**: people and channels that mostly interact with each other.\n"
            "- **Connector**: someone active in more than one community, so ideas can pass through them.\n"
            "- **How topics spread**: which communities picked up a topic, and how many hours later."
        )

    network_col1, network_col2 = st.columns([4, 1])
    with network_col1:
        network_keyword = st.text_input(
            "Network keyword",
            placeholder="Optional: look at one keyword only, or leave empty to use everything collected",
            label_visibility="collapsed",
            key="network_keyword_input"
        )
    with network_col2:
        network_clicked = st.button("Map network", use_container_width=True, type="primary", key="build_network_btn")

    if network_clicked:
        with st.spinner("Connecting people and channels and checking how topics spread..."):
            try:
                network_events = fetch_events(keyword=network_keyword.strip() or None, limit=20000)
                network_trends = detect_topic_trends(network_events)
                network_topics = network_trends["topics"] if network_trends["status"] == "ok" else None
                network_result, network_graph = analyze_network(network_events, network_topics)
                st.session_state["network_result"] = network_result
                st.session_state["network_graph_obj"] = network_graph
            except Exception as e:
                st.session_state.pop("network_result", None)
                st.session_state.pop("network_graph_obj", None)
                st.error(f"Couldn't map the network: {e}")

    if "network_result" in st.session_state:
        render_network_intelligence(st.session_state["network_result"], st.session_state["network_graph_obj"])
    else:
        st.info("Press Map network to see who is connected.")


if page == "PULSE Signals":
    with st.container(key="hero"):
        st.header("PULSE Signals")
        st.caption("The most important findings in one place: new or growing conversations that several checks agree on.")
    with st.expander("How to read this page"):
        st.markdown(
            "- **Signal**: a new or growing topic that PULSE thinks is worth your attention.\n"
            "- **Priority**: High means several checks agree it matters. Medium means worth watching. Low means an early sign.\n"
            "- **Checks**: growth of messages, how people feel, which audience group is talking, and how far it spread between communities.\n"
            "- **Score and confidence**: rough guides for ranking. They are not probabilities.\n"
            "- **Explain in simple words**: writes a short explanation from the evidence PULSE already found."
        )

    signal_col1, signal_col2 = st.columns([4, 1])
    with signal_col1:
        signal_keyword = st.text_input(
            "Signal keyword",
            placeholder="Optional: look at one keyword only, or leave empty to use everything collected",
            label_visibility="collapsed",
            key="signal_keyword_input"
        )
    with signal_col2:
        signal_clicked = st.button("Find signals", use_container_width=True, type="primary", key="detect_signals_btn")

    if signal_clicked:
        with st.spinner("Checking growth, mood, audience and spread, then combining the results..."):
            try:
                signal_events = fetch_events(keyword=signal_keyword.strip() or None, limit=20000)
                st.session_state["signal_result"] = run_pipeline(signal_events)
                st.session_state.pop("signal_save_message", None)
            except Exception as e:
                st.session_state.pop("signal_result", None)
                st.error(f"Couldn't find signals: {e}")

    if "signal_result" in st.session_state:
        current_signals = st.session_state["signal_result"]["signals"]
        if current_signals:
            if st.button("Save these signals", key="save_signals_btn"):
                try:
                    summary = save_signals(current_signals)
                    st.session_state["signal_save_message"] = (
                        f"Saved {summary['inserted']} new and updated {summary['updated']} existing signals."
                    )
                except Exception as e:
                    st.session_state["signal_save_message"] = f"Couldn't save signals: {e}"
        if "signal_save_message" in st.session_state:
            st.info(st.session_state["signal_save_message"])
        render_pulse_signals(st.session_state["signal_result"])

    st.markdown("---")
    with st.expander("Saved signals history"):
        if st.button("Load saved signals", key="load_signals_btn"):
            try:
                st.session_state["saved_signals"] = fetch_signals(limit=20)
            except Exception as e:
                st.session_state.pop("saved_signals", None)
                st.error(f"Couldn't load saved signals: {e}")
        if "saved_signals" in st.session_state:
            render_saved_signals(st.session_state["saved_signals"])
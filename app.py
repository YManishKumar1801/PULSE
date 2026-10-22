import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from telethon.sync import TelegramClient
from pyvis.network import Network as PyvisNetwork
from datetime import datetime, timezone
import html
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
from intelligence.overview import command_center_summary, sentiment_overview, worker_summary
from intelligence.sentiment import analyze_events, analyze_text
from intelligence.signals import run_pipeline
from intelligence.trends import detect_topic_trends
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

st.set_page_config(page_title="PULSE - Live Audience Intelligence", layout="wide", initial_sidebar_state="expanded")

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
        if not (pos_reason.startswith("Could not generate") or neg_reason.startswith("Could not generate")):
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
            sarcastic = bool(c.get("sarcasm") and c["sarcasm"]["flag"])
            sarcasm_badge = "<span style='color:#B15FC9;font-weight:600;font-size:11px;margin-left:8px;'>SARCASM?</span>" if sarcastic else ""
            st.markdown(
                f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;'>"
                f"<span style='color:{color};font-weight:600;font-size:12px;'>{c['sentiment'].upper()}</span>{sarcasm_badge}<br>"
                f"<span style='color:#2C2C2A;font-size:13px;'>{c['text'][:150]}</span>"
                f"</div>",
                unsafe_allow_html=True
            )


def render_audience_segments(result):
    if result["status"] != "ok":
        st.warning(
            f"Not enough data to build segments. Found {result['usable_events']} usable messages, "
            f"at least {result['min_required']} are needed. Run more searches first."
        )
        return

    quality = result["silhouette"]
    quality_text = f" · cluster quality {quality} (silhouette)" if quality is not None else ""
    st.markdown(
        f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 4px 0;'>{result['usable_events']} messages grouped into "
        f"{len(result['segments'])} segments{quality_text} · {result['analyzed_events']} with sentiment</p>",
        unsafe_allow_html=True
    )
    st.caption("Segments group similar messages by topic and wording. They describe conversations, not individual people.")

    trend_colors = {"growing": "#3B6D11", "shrinking": "#A32D2D", "stable": "#5F5E5A"}

    for segment in result["segments"]:
        name = html.escape(segment["name"])
        size_pct = round(segment["size"] * 100)

        pills = "".join(
            f"<span style='display:inline-block;background:#EAF3DE;color:#3B6D11;padding:3px 10px;border-radius:14px;"
            f"font-size:12px;font-weight:600;margin:0 6px 6px 0;'>{html.escape(topic)}</span>"
            for topic in segment["top_topics"]
        )

        sentiment = segment["sentiment"]
        if sentiment:
            pos = round(sentiment["positive"] * 100)
            neu = round(sentiment["neutral"] * 100)
            neg = round(sentiment["negative"] * 100)
            sentiment_html = (
                f"<div style='display:flex;width:100%;height:10px;border-radius:6px;overflow:hidden;background:#F0EEE8;'>"
                f"<div style='width:{pos}%;background:#639922;'></div>"
                f"<div style='width:{neu}%;background:#B4B2A9;'></div>"
                f"<div style='width:{neg}%;background:#E24B4A;'></div></div>"
                f"<div style='display:flex;justify-content:space-between;margin-top:4px;'>"
                f"<span style='font-size:11px;color:#3B6D11;'>{pos}% positive</span>"
                f"<span style='font-size:11px;color:#5F5E5A;'>{neu}% neutral</span>"
                f"<span style='font-size:11px;color:#A32D2D;'>{neg}% negative</span></div>"
            )
        else:
            sentiment_html = "<p style='color:#888780;font-size:12px;margin:0;'>Sentiment not available for this segment yet.</p>"

        languages = ", ".join(f"{html.escape(item['name'])} {round(item['share'] * 100)}%" for item in segment["languages"])
        platforms = ", ".join(f"{html.escape(platform)} {round(share * 100)}%" for platform, share in segment["platforms"].items())
        engagement = segment["engagement"]
        engagement_text = (
            f"{engagement['avg_likes']} likes · {engagement['avg_replies']} replies · {engagement['avg_views']} views per message"
        )

        trend = segment["trend"]
        if trend["status"] == "ok":
            color = trend_colors.get(trend["label"], "#5F5E5A")
            trend_html = (
                f"<span style='color:{color};font-weight:600;'>{trend['label']}</span> "
                f"({trend['shift_points']:+.1f} pts share, earlier vs later period)"
            )
        else:
            trend_html = "<span style='color:#888780;'>not enough time history yet</span>"

        st.markdown(
            f"<div style='background:#fff;border-radius:14px;padding:18px 20px;border:0.5px solid #E5E2D9;"
            f"box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:12px;'>"
            f"<div style='display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px;'>"
            f"<span style='font-size:15px;font-weight:700;color:#2C2C2A;'>{name}</span>"
            f"<span style='font-size:12px;color:#5F5E5A;'>{size_pct}% of messages · {segment['count']} messages</span></div>"
            f"<div style='margin-bottom:10px;'>{pills}</div>"
            f"{sentiment_html}"
            f"<div style='margin-top:12px;font-size:12px;color:#5F5E5A;line-height:1.8;'>"
            f"<div><b style='color:#2C2C2A;'>Languages:</b> {languages}</div>"
            f"<div><b style='color:#2C2C2A;'>Sources:</b> {platforms}</div>"
            f"<div><b style='color:#2C2C2A;'>Engagement:</b> {engagement_text}</div>"
            f"<div><b style='color:#2C2C2A;'>Trend:</b> {trend_html}</div></div></div>",
            unsafe_allow_html=True
        )

        with st.expander(f"Sample messages from {segment['segment_id']}"):
            for text in segment["sample_texts"]:
                st.markdown(
                    f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;font-size:13px;color:#2C2C2A;'>{html.escape(text)}</div>",
                    unsafe_allow_html=True
                )

    keyword_rows = sorted(result["keyword_totals"].items(), key=lambda item: item[1], reverse=True)[:5]
    if keyword_rows:
        st.markdown("**Most associated segment per keyword**")
        for keyword, _ in keyword_rows:
            ranked = segments_for_keyword(result, keyword)
            if not ranked:
                continue
            top = ranked[0]
            st.markdown(
                f"<div style='font-size:13px;padding:6px 0;border-bottom:0.5px solid #F0EEE8;color:#2C2C2A;'>"
                f"<b>{html.escape(keyword)}</b> → {html.escape(top['name'])} "
                f"<span style='color:#5F5E5A;'>({round(top['share_of_keyword'] * 100)}% of its messages · "
                f"{top['lift']}x over-represented)</span></div>",
                unsafe_allow_html=True
            )


def render_topic_trends(result):
    if result["status"] == "insufficient_data":
        st.warning(
            f"Not enough data for topic trends. Found {result['usable_events']} usable messages, "
            f"at least {result['min_required']} are needed. Run more searches first."
        )
        return
    if result["status"] == "insufficient_history":
        st.warning("All collected messages fall within a very short time span, so growth can't be measured yet. Collect data over a longer period.")
        return

    window = result["window_hours"]
    st.markdown(
        f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 4px 0;'>Latest {window:g}h compared with the {window:g}h before it · "
        f"{result['usable_events']} messages · {len(result['topics'])} topics shown</p>",
        unsafe_allow_html=True
    )
    notes = [
        f"Topics with fewer than {result['min_volume']} mentions in a window are ignored ({result['dropped_low_volume']} dropped).",
        "Topics come from a TF-IDF + NMF topic model, which groups words that appear together."
    ]
    if not result["persistence_evaluated"]:
        notes.append("Persistent status needs at least three windows of history, so it is not assigned yet.")
    st.caption(" ".join(notes))

    if not result["topics"]:
        st.info("No topic reached the minimum volume. Collect more data and try again.")
        return

    status_colors = {
        "Emerging": "#D98A2B", "Rising": "#3B6D11", "Persistent": "#3B8AD9",
        "Stable": "#5F5E5A", "Declining": "#A32D2D"
    }

    for topic in result["topics"]:
        color = status_colors.get(topic["status"], "#5F5E5A")
        growth_text = "new" if topic["growth_pct"] is None else f"{topic['growth_pct']:+.0f}%"
        velocity_text = f"{topic['velocity']} ({topic['velocity_per_hour']:+.2f} messages/hour)"

        sentiment = topic["sentiment"]
        if sentiment["status"] == "ok":
            prev_s = sentiment["previous"]
            cur_s = sentiment["current"]
            sentiment_color = "#A32D2D" if sentiment["direction"] == "more negative" else "#3B6D11" if sentiment["direction"] == "more positive" else "#5F5E5A"
            sentiment_html = (
                f"<span style='color:{sentiment_color};font-weight:600;'>{sentiment['direction']}</span> "
                f"(negative {round(prev_s['negative_share'] * 100)}% → {round(cur_s['negative_share'] * 100)}%)"
            )
        else:
            sentiment_html = "<span style='color:#888780;'>not enough analyzed messages in both windows</span>"

        keywords = ", ".join(f"{html.escape(item['keyword'])} ({item['count']})" for item in topic["keywords"]) or "none"
        terms = "".join(
            f"<span style='display:inline-block;background:#F0EEE8;color:#2C2C2A;padding:3px 10px;border-radius:14px;"
            f"font-size:12px;margin:0 6px 6px 0;'>{html.escape(term)}</span>"
            for term in topic["terms"]
        )

        st.markdown(
            f"<div style='background:#fff;border-radius:14px;padding:18px 20px;border:0.5px solid #E5E2D9;"
            f"box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:12px;border-left:4px solid {color};'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;'>"
            f"<span style='font-size:15px;font-weight:700;color:#2C2C2A;'>{html.escape(topic['name'])}</span>"
            f"<span style='background:{color};color:#fff;font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px;'>"
            f"{topic['status'].upper()}</span></div>"
            f"<div style='margin-bottom:8px;'>{terms}</div>"
            f"<div style='display:flex;gap:28px;margin-bottom:8px;'>"
            f"<div><div style='font-size:11px;color:#888780;'>Mentions</div>"
            f"<div style='font-size:18px;font-weight:700;color:#2C2C2A;'>{topic['previous']} → {topic['current']}</div></div>"
            f"<div><div style='font-size:11px;color:#888780;'>Growth</div>"
            f"<div style='font-size:18px;font-weight:700;color:{color};'>{growth_text}</div></div></div>"
            f"<div style='font-size:12px;color:#5F5E5A;line-height:1.8;'>"
            f"<div><b style='color:#2C2C2A;'>Velocity:</b> {velocity_text}</div>"
            f"<div><b style='color:#2C2C2A;'>Share of conversation:</b> {round(topic['share_previous'] * 100)}% → {round(topic['share_current'] * 100)}%</div>"
            f"<div><b style='color:#2C2C2A;'>Sentiment:</b> {sentiment_html}</div>"
            f"<div><b style='color:#2C2C2A;'>Keywords:</b> {keywords}</div></div></div>",
            unsafe_allow_html=True
        )

        with st.expander(f"Sample messages from {topic['topic_id']}"):
            for text in topic["sample_texts"]:
                st.markdown(
                    f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;font-size:13px;color:#2C2C2A;'>{html.escape(text)}</div>",
                    unsafe_allow_html=True
                )


def render_network_intelligence(result, graph):
    for note in result["limitations"]:
        st.caption(note)

    if result["status"] != "ok":
        st.warning(
            f"Not enough network data. Found {result['network_events']} messages with a participant identity, "
            f"at least {result['min_required']} are needed. Run more YouTube or Telegram searches first."
        )
        return

    st.markdown(
        f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 6px 0;'>{result['authors']} participants · {result['sources']} sources · "
        f"{result['edges']} connections · {result['communities_significant']} communities "
        f"(with at least {result['min_community_messages']} messages)</p>",
        unsafe_allow_html=True
    )

    st.markdown("**Communities**")
    for community in result["communities"]:
        color = community_color(community["community_id"])
        sentiment = community["sentiment"]
        if sentiment:
            pos = round(sentiment["positive"] * 100)
            neu = round(sentiment["neutral"] * 100)
            neg = round(sentiment["negative"] * 100)
            sentiment_html = (
                f"<div style='display:flex;width:100%;height:10px;border-radius:6px;overflow:hidden;background:#F0EEE8;'>"
                f"<div style='width:{pos}%;background:#639922;'></div>"
                f"<div style='width:{neu}%;background:#B4B2A9;'></div>"
                f"<div style='width:{neg}%;background:#E24B4A;'></div></div>"
                f"<div style='font-size:11px;color:#5F5E5A;margin-top:4px;'>{pos}% positive · {neu}% neutral · {neg}% negative</div>"
            )
        else:
            sentiment_html = "<div style='font-size:12px;color:#888780;'>Sentiment not available yet.</div>"

        terms = ", ".join(html.escape(term) for term in community["top_terms"]) or "none"
        topics = ", ".join(f"{html.escape(item['name'])} ({item['count']})" for item in community["top_topics"]) or "no topic reached the minimum volume"
        sources = ", ".join(f"{html.escape(item['label'])} ({item['messages']})" for item in community["top_sources"])
        platforms = ", ".join(f"{html.escape(name)} {round(share * 100)}%" for name, share in community["platforms"].items())

        st.markdown(
            f"<div style='background:#fff;border-radius:14px;padding:16px 20px;border:0.5px solid #E5E2D9;"
            f"box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:12px;border-left:4px solid {color};'>"
            f"<div style='display:flex;justify-content:space-between;margin-bottom:10px;'>"
            f"<span style='font-size:14px;font-weight:700;color:#2C2C2A;'>{community['community_id']}</span>"
            f"<span style='font-size:12px;color:#5F5E5A;'>{community['authors']} participants · {community['sources']} sources · "
            f"{community['messages']} messages</span></div>"
            f"{sentiment_html}"
            f"<div style='margin-top:10px;font-size:12px;color:#5F5E5A;line-height:1.8;'>"
            f"<div><b style='color:#2C2C2A;'>Distinctive terms:</b> {terms}</div>"
            f"<div><b style='color:#2C2C2A;'>Main topics:</b> {topics}</div>"
            f"<div><b style='color:#2C2C2A;'>Main sources:</b> {sources}</div>"
            f"<div><b style='color:#2C2C2A;'>Platforms:</b> {platforms}</div></div></div>",
            unsafe_allow_html=True
        )

    st.markdown("**Bridge nodes**")
    st.caption("Participants or sources whose activity is split across different communities (at least 20% outside their main one).")
    if not result["bridge_nodes"]:
        st.info("No bridge nodes found. They appear when the same participant is active in sources that belong to different communities.")
    for node in result["bridge_nodes"]:
        connected = ", ".join(node["communities_connected"])
        st.markdown(
            f"<div style='font-size:13px;padding:8px 0;border-bottom:0.5px solid #F0EEE8;color:#2C2C2A;'>"
            f"<b>{html.escape(node['label'])}</b> <span style='color:#888780;'>({node['type']}, {node['platform']})</span> connects {connected} "
            f"<span style='color:#5F5E5A;'>· {round(node['bridge_share'] * 100)}% of activity outside the main community · "
            f"betweenness {node['betweenness']}</span></div>",
            unsafe_allow_html=True
        )

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)
    st.markdown("**Most structurally important nodes**")
    st.caption("PageRank rewards nodes connected to other well-connected nodes. It is a structural measure, not proof of real-world influence.")

    def sentiment_word(score):
        if score is None:
            return "n/a"
        if score >= 0.3:
            return "mostly positive"
        if score <= -0.3:
            return "mostly negative"
        return "mixed or neutral"

    nc1, nc2 = st.columns(2)
    for column, heading, nodes, bar_color in (
        (nc1, "Participants", result["top_authors"], "#639922"),
        (nc2, "Channels and videos", result["top_sources"], "#E24B4A"),
    ):
        with column:
            st.markdown(f"**{heading}**")
            max_rank = max([node["pagerank"] for node in nodes], default=0) or 1
            for node in nodes:
                bar_pct = round(node["pagerank"] / max_rank * 100)
                st.markdown(
                    f"<div style='margin-bottom:10px;'>"
                    f"<div style='display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px;'>"
                    f"<span style='color:#2C2C2A;'>{html.escape(node['label'])}</span>"
                    f"<span style='color:#5F5E5A;'>{node['connections']} connections · {node['messages']} messages</span></div>"
                    f"<div style='background:#F0EEE8;border-radius:5px;height:8px;overflow:hidden;'>"
                    f"<div style='width:{bar_pct}%;background:{bar_color};height:100%;'></div></div>"
                    f"<div style='font-size:11px;color:#888780;margin-top:2px;'>{node['community_id'] or 'minor community'} · "
                    f"{sentiment_word(node['sentiment_score'])}</div></div>",
                    unsafe_allow_html=True
                )

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)
    st.markdown("**Topic propagation across communities**")
    if not result["propagation"]:
        st.info("No topic reached the minimum mentions in a community yet. Topic propagation needs enough messages and time history (see Topic Trends).")
    spread_colors = {"high": "#D98A2B", "medium": "#3B8AD9", "contained": "#5F5E5A"}
    for item in result["propagation"]:
        steps = []
        for position, step in enumerate(item["timeline"]):
            timing = "first seen" if position == 0 else (f"+{step['lag_hours']}h" if step["lag_hours"] is not None else "")
            steps.append(
                f"<span style='color:{community_color(step['community_id'])};font-weight:600;'>{step['community_id']}</span> "
                f"({step['mentions']} mentions, {timing})"
            )
        badge_color = spread_colors.get(item["spread"], "#5F5E5A")
        st.markdown(
            f"<div style='background:#fff;border-radius:14px;padding:14px 18px;border:0.5px solid #E5E2D9;margin-bottom:10px;'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;'>"
            f"<span style='font-size:14px;font-weight:700;color:#2C2C2A;'>{html.escape(item['name'])}</span>"
            f"<span style='background:{badge_color};color:#fff;font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px;'>"
            f"{item['spread'].upper()} SPREAD</span></div>"
            f"<div style='font-size:12px;color:#5F5E5A;margin-bottom:6px;'>Reached {item['communities_reached']} of "
            f"{item['total_communities']} communities · {item['total_mentions']} mentions</div>"
            f"<div style='font-size:12px;color:#2C2C2A;line-height:1.8;'>{' → '.join(steps)}</div></div>",
            unsafe_allow_html=True
        )

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)
    st.markdown("**Interactive Network Graph** — colors are communities, squares are channels or videos, thick border is a bridge node, size is PageRank")
    html_file = generate_network_html(graph)
    with open(html_file, "r", encoding="utf-8") as f:
        graph_html = f.read()
    st.components.v1.html(graph_html, height=620)


PAGES = [
    "Command Center",
    "Live Search",
    "Telegram Search",
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
    color = SIGNAL_PRIORITY_COLORS.get(signal["priority"], "#5F5E5A")
    terms = "".join(
        f"<span style='display:inline-block;background:#F0EEE8;color:#2C2C2A;padding:3px 10px;border-radius:14px;"
        f"font-size:12px;margin:0 6px 6px 0;'>{html.escape(term)}</span>"
        for term in signal["terms"]
    )
    rows = [
        ("WHAT", signal["what"]),
        ("WHO", signal["who"]),
        ("HOW THEY FEEL", signal["how_they_feel"]),
        ("HOW IT SPREADS", signal["how_it_spreads"]),
        ("HOW FAST", signal["how_fast"]),
        ("WHY IT MATTERS", signal["why_it_matters"]),
    ]
    rows_html = "".join(
        f"<div style='margin-bottom:8px;'><div style='font-size:10px;font-weight:700;color:#888780;letter-spacing:0.06em;'>{label}</div>"
        f"<div style='font-size:13px;color:#2C2C2A;line-height:1.5;'>{html.escape(str(value))}</div></div>"
        for label, value in rows
    )
    st.markdown(
        f"<div style='background:#fff;border-radius:14px;padding:18px 20px;border:0.5px solid #E5E2D9;"
        f"box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:12px;border-left:4px solid {color};'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;'>"
        f"<span style='font-size:15px;font-weight:700;color:#2C2C2A;'>{html.escape(signal['topic'])}</span>"
        f"<span><span style='background:#F0EEE8;color:#2C2C2A;font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px;margin-right:6px;'>"
        f"{signal['type'].upper()}</span>"
        f"<span style='background:{color};color:#fff;font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px;'>"
        f"{signal['priority'].upper()}</span></span></div>"
        f"<div style='margin-bottom:8px;'>{terms}</div>"
        f"<div style='display:flex;gap:28px;margin-bottom:12px;'>"
        f"<div><div style='font-size:11px;color:#888780;'>Signal score</div>"
        f"<div style='font-size:20px;font-weight:700;color:{color};'>{signal['score']:.0f}<span style='font-size:12px;color:#888780;'> / 100</span></div></div>"
        f"<div><div style='font-size:11px;color:#888780;'>Confidence proxy</div>"
        f"<div style='font-size:20px;font-weight:700;color:#2C2C2A;'>{round(signal['confidence_proxy'] * 100)}%</div></div></div>"
        f"{rows_html}</div>",
        unsafe_allow_html=True
    )

    explanation_key = f"explanation_{signal['signal_id']}"
    if st.button("Explain this signal", key=f"explain_btn_{key_prefix}"):
        with st.spinner("Writing an explanation from the detected evidence..."):
            st.session_state[explanation_key] = explain_signal(signal, llm=generate_gemini_summary)
    if explanation_key in st.session_state:
        explanation = st.session_state[explanation_key]
        if explanation["source"] == "gemini":
            source_note = "Written by Gemini from the detected evidence only; numbers were checked against the evidence."
        else:
            reasons = {
                "llm_unavailable": "Gemini was unavailable",
                "ungrounded_numbers": "the Gemini text contained numbers not in the evidence",
                "too_long": "the Gemini text was too long",
                "empty": "Gemini returned nothing",
                "no_llm": "no Gemini key is configured",
            }
            source_note = f"Built directly from the detected evidence ({reasons.get(explanation['reason'], explanation['reason'])})."
        st.markdown(
            f"<div style='background:#F7F5EF;border-radius:10px;padding:12px 14px;font-size:13px;color:#2C2C2A;line-height:1.6;margin-bottom:6px;'>"
            f"{html.escape(explanation['text'])}</div>",
            unsafe_allow_html=True
        )
        st.caption(source_note)
        with st.expander("Evidence given to the explainer"):
            st.code(explanation["facts"], language=None)

    with st.expander("Score breakdown and evidence"):
        breakdown = []
        for name in ("trend", "sentiment", "audience", "network"):
            value = signal["components"].get(name)
            if value is None:
                breakdown.append(f"- **{VECTOR_LABELS[name]}**: not available")
            else:
                weight = signal["weights_used"].get(name, 0)
                mark = " (corroborating)" if name in signal["corroborating_vectors"] else ""
                breakdown.append(f"- **{VECTOR_LABELS[name]}**: {value:.2f} × weight {weight:.2f}{mark}")
        st.markdown("\n".join(breakdown))
        st.caption(
            "The score is a weighted heuristic over the available vectors, renormalized when a vector is missing. "
            "The weights are documented baseline choices and have not been validated. The confidence proxy reflects data volume, "
            "vector availability and corroboration; it is not a calibrated probability."
        )
        for text in signal.get("sample_texts", []):
            st.markdown(
                f"<div style='padding:6px 0;border-bottom:0.5px solid #F0EEE8;font-size:13px;color:#2C2C2A;'>{html.escape(text)}</div>",
                unsafe_allow_html=True
            )


def render_pulse_signals(result):
    if result["status"] == "insufficient_data":
        st.warning(
            f"Not enough data to detect signals. Found {result['usable_events']} usable messages, "
            f"at least {result['min_required']} are needed. Run more searches first."
        )
        return
    if result["status"] == "insufficient_history":
        st.warning("All collected messages fall within a very short time span, so growth can't be measured yet. Collect data over a longer period.")
        return

    vectors = result["vectors"]
    st.markdown(
        f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 4px 0;'>{result['total_events']} messages · "
        f"latest {result['window_hours']:g}h window · {len(result['signals'])} signals</p>",
        unsafe_allow_html=True
    )
    st.caption(
        f"Vector status: trends {vectors['trends']}, audience {vectors['audience']}, network {vectors['network']}. "
        "Sentiment is read from analyzed messages. A signal needs an Emerging or Rising topic with a score of at least 25."
    )
    if not result["signals"]:
        st.info("No Emerging or Rising topic reached the signal threshold in this window.")
        return
    for position, signal in enumerate(result["signals"]):
        render_signal_card(signal, key_prefix=f"signal_{position}")


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


def render_command_center():
    st.markdown(
        "<div style='margin-bottom:6px;'><div style='font-size:26px;font-weight:800;letter-spacing:2px;color:#2C2C2A;'>PULSE</div>"
        "<div style='font-size:12px;font-weight:600;letter-spacing:3px;color:#888780;'>LIVE AUDIENCE INTELLIGENCE</div></div>",
        unsafe_allow_html=True
    )
    if st.button("Refresh intelligence", key="cc_refresh_btn"):
        load_intelligence.clear()

    try:
        events, pipeline = load_intelligence()
    except Exception as e:
        st.error(f"Couldn't load collected data: {e}")
        return

    summary = command_center_summary(events, pipeline)
    if summary["total_events"] == 0:
        st.info("No collected data yet. Use the search pages to collect messages first.")
        return

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        kpi_card("Total Events", f"{summary['total_events']:,}", f"{summary['analyzed_events']:,} analyzed for sentiment", "#3B6D11")
    with kpi2:
        kpi_card("Active Topics", summary["active_topics"], "topics above the minimum volume", "#185FA5")
    with kpi3:
        kpi_card("Audience Segments", summary["segments"], "observable, not demographic", "#7F5AA2")
    with kpi4:
        kpi_card("Emerging Signals", summary["emerging_signals"], f"{summary['rising_signals']} more rising", "#BA7517")

    if summary["pipeline_status"] != "ok":
        st.warning("Topic trends need more data over a longer period before signals can be detected.")

    st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)
    st.markdown("#### Top Signals")
    if not summary["top_signals"]:
        st.caption("No Emerging or Rising topic has reached the signal threshold yet.")
    for signal in summary["top_signals"]:
        color = SIGNAL_PRIORITY_COLORS.get(signal["priority"], "#5F5E5A")
        st.markdown(
            f"<div style='background:#fff;border-radius:12px;padding:14px 18px;border:0.5px solid #E5E2D9;"
            f"border-left:4px solid {color};margin-bottom:8px;'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;'>"
            f"<span style='font-size:14px;font-weight:700;color:#2C2C2A;'>{html.escape(signal['topic'])}</span>"
            f"<span style='font-size:12px;color:#5F5E5A;'>{signal['type'].upper()} · "
            f"<b style='color:{color};'>{signal['priority'].upper()}</b> · score {signal['score']:.0f}</span></div>"
            f"<div style='font-size:12px;color:#5F5E5A;margin-top:4px;'>{html.escape(signal['why_it_matters'])}</div></div>",
            unsafe_allow_html=True
        )
    if summary["top_signals"]:
        st.caption("Open the PULSE Signals page for the full evidence and explanation.")

    left, right = st.columns(2)
    with left:
        st.markdown("#### Trend Velocity")
        if summary["velocity"]:
            velocity_frame = pd.DataFrame(summary["velocity"]).set_index("topic")[["velocity_per_hour"]]
            velocity_frame.columns = ["Messages per hour change"]
            st.bar_chart(velocity_frame, horizontal=True)
            st.caption("Change in messages per hour, latest window versus the one before it.")
        else:
            st.caption("Not enough history to measure velocity yet.")
    with right:
        st.markdown("#### Sentiment Overview")
        overview = sentiment_overview(events)
        if overview["analyzed_events"]:
            st.bar_chart(sentiment_frame(overview["distribution"]), horizontal=True, color=["#5F5E5A"])
            st.caption(f"{overview['analyzed_events']:,} of {overview['total_events']:,} messages have been analyzed.")
        else:
            st.caption("No analyzed messages yet. Run analyze_pending.py to analyze stored events.")

    vectors = summary["vectors"]
    if vectors:
        st.caption(f"Vector status: trends {vectors.get('trends')}, audience {vectors.get('audience')}, network {vectors.get('network')}.")

    st.markdown("#### Processing Workers")
    try:
        workers = worker_summary(fetch_worker_status())
    except Exception:
        workers = None
    if workers is None:
        st.caption("Worker status could not be loaded.")
    elif not workers:
        st.caption("No processing worker has reported yet. Start one with: python workers/run_worker.py")
    else:
        for row in workers:
            state = "running" if row["alive"] else "not reporting"
            age = "never" if row["heartbeat_age_seconds"] is None else f"{row['heartbeat_age_seconds']}s ago"
            lag = "unknown" if row["consumer_lag"] is None else row["consumer_lag"]
            p95 = "n/a" if row["batch_seconds_p95"] is None else f"{row['batch_seconds_p95']}s"
            st.caption(
                f"{row['worker_id']}: {state} (last heartbeat {age}) · stored {row['stored']} · analyzed {row['analyzed']} · "
                f"malformed {row['malformed']} · analysis failures {row['analysis_failures']} · lag {lag} · batch P95 {p95}"
            )


def render_sentiment_page():
    st.markdown("#### Sentiment Analytics")
    st.caption("Built from stored, analyzed messages across every platform. Emotion and sarcasm are only evaluated for English text.")
    try:
        events, _ = load_intelligence()
    except Exception as e:
        st.error(f"Couldn't load collected data: {e}")
        return

    overview = sentiment_overview(events)
    if not overview["analyzed_events"]:
        st.info("No analyzed messages yet. Run analyze_pending.py to analyze stored events.")
        return

    metric1, metric2, metric3, metric4 = st.columns(4)
    shares = overview["shares"]
    with metric1:
        kpi_card("Positive", f"{round(shares['Positive'] * 100)}%", f"{overview['distribution']['Positive']:,} messages", "#3B6D11")
    with metric2:
        kpi_card("Neutral", f"{round(shares['Neutral'] * 100)}%", f"{overview['distribution']['Neutral']:,} messages", "#B4B2A9")
    with metric3:
        kpi_card("Negative", f"{round(shares['Negative'] * 100)}%", f"{overview['distribution']['Negative']:,} messages", "#A32D2D")
    with metric4:
        sarcasm = overview["sarcasm"]
        if sarcasm["evaluated"]:
            kpi_card("Sarcasm Flagged", f"{round(sarcasm['share'] * 100)}%", f"of {sarcasm['evaluated']:,} English messages checked")
        else:
            kpi_card("Sarcasm Flagged", "n/a", "no English messages checked")

    st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)
    st.markdown("#### Sentiment Over Time")
    timeline = pd.DataFrame(overview["timeline"])
    if len(timeline) >= 2:
        timeline["bucket"] = pd.to_datetime(timeline["bucket"])
        st.bar_chart(timeline.set_index("bucket")[["Positive", "Neutral", "Negative"]], color=SENTIMENT_COLORS)
        st.caption(f"Messages per {overview['timeline_unit']}, split by sentiment.")
    else:
        st.caption("All analyzed messages fall into a single time bucket, so a timeline can't be drawn yet.")

    left, right = st.columns(2)
    with left:
        st.markdown("#### Platform Comparison")
        platform_rows = {
            name: {label: round(values["shares"][label] * 100, 1) for label in ("Positive", "Neutral", "Negative")}
            for name, values in overview["by_platform"].items()
        }
        st.bar_chart(pd.DataFrame(platform_rows).T, horizontal=True, color=SENTIMENT_COLORS)
        st.caption("Share of each platform's analyzed messages, in percent. " + ", ".join(
            f"{name}: {values['total']:,}" for name, values in overview["by_platform"].items()
        ))
    with right:
        st.markdown("#### Language Comparison")
        if overview["by_language"]:
            language_rows = {
                row["language"]: {label: round(row["shares"][label] * 100, 1) for label in ("Positive", "Neutral", "Negative")}
                for row in overview["by_language"]
            }
            st.bar_chart(pd.DataFrame(language_rows).T, horizontal=True, color=SENTIMENT_COLORS)
            st.caption("Languages with at least 5 analyzed messages, in percent. " + ", ".join(
                f"{row['language']}: {row['total']:,}" for row in overview["by_language"]
            ))
        else:
            st.caption("No language has enough analyzed messages for a comparison yet.")

    st.markdown("#### Emotions")
    if overview["emotions"]:
        emotion_frame = pd.DataFrame(overview["emotions"]).set_index("emotion")
        st.bar_chart(emotion_frame, horizontal=True, color=["#5F5E5A"])
        st.caption("Emotion is classified only for English messages.")
    else:
        st.caption("No emotion data stored. Run analyze_pending.py without skipping emotion to fill it in.")


if page == "Command Center":
    render_command_center()

if page == "Sentiment":
    render_sentiment_page()

if page == "Live Search":
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

if page == "Telegram Search":
    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    st.markdown("#### Search Telegram")
    tg_col1, tg_col2 = st.columns([4, 1])
    with tg_col1:
        telegram_keyword = st.text_input("Search Telegram", placeholder="e.g. cricket, news, finance...", label_visibility="collapsed", key="telegram_keyword_input")
    with tg_col2:
        telegram_search_clicked = st.button("Analyze", use_container_width=True, type="primary", key="telegram_analyze_btn")

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

        tpos = round(tcounts["Positive"] / ttotal * 100) if ttotal else 0
        tneu = round(tcounts["Neutral"] / ttotal * 100) if ttotal else 0
        tneg = round(tcounts["Negative"] / ttotal * 100) if ttotal else 0

        t_radius = 58
        t_circumference = 2 * 3.14159265 * t_radius

        def t_donut_segment(percent, offset_percent):
            length = t_circumference * percent / 100
            dash = f"{length:.2f} {t_circumference - length:.2f}"
            dashoffset = -(t_circumference * offset_percent / 100)
            return dash, dashoffset

        t_pos_dash, t_pos_offset = t_donut_segment(tpos, 0)
        t_neu_dash, t_neu_offset = t_donut_segment(tneu, tpos)
        t_neg_dash, t_neg_offset = t_donut_segment(tneg, tpos + tneu)

        t_dominant_pct = max(tpos, tneu, tneg)
        t_dominant_label = "positive" if tpos == t_dominant_pct else "negative" if tneg == t_dominant_pct else "neutral"
        t_dominant_color = "#3B6D11" if t_dominant_label == "positive" else "#A32D2D" if t_dominant_label == "negative" else "#5F5E5A"

        st.markdown(f"<p style='color:#5F5E5A;font-size:13px;margin:0 0 12px 0;'>{ttotal} messages across {len(tresult['channels'])} channels</p>", unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background:#fff;border-radius:14px;padding:22px;border:0.5px solid #E5E2D9;box-shadow:0 1px 3px rgba(44,44,42,0.05);margin-bottom:16px;display:grid;grid-template-columns:180px 1fr;gap:24px;align-items:center;">
            <div style="display:flex;justify-content:center;">
                <svg width="140" height="140" viewBox="0 0 140 140">
                    <circle cx="70" cy="70" r="{t_radius}" fill="none" stroke="#F0EEE8" stroke-width="16"/>
                    <circle cx="70" cy="70" r="{t_radius}" fill="none" stroke="#639922" stroke-width="16" stroke-dasharray="{t_pos_dash}" stroke-dashoffset="{t_pos_offset}" transform="rotate(-90 70 70)"/>
                    <circle cx="70" cy="70" r="{t_radius}" fill="none" stroke="#B4B2A9" stroke-width="16" stroke-dasharray="{t_neu_dash}" stroke-dashoffset="{t_neu_offset}" transform="rotate(-90 70 70)"/>
                    <circle cx="70" cy="70" r="{t_radius}" fill="none" stroke="#E24B4A" stroke-width="16" stroke-dasharray="{t_neg_dash}" stroke-dashoffset="{t_neg_offset}" transform="rotate(-90 70 70)"/>
                    <text x="70" y="65" text-anchor="middle" font-size="26" font-weight="700" fill="{t_dominant_color}">{t_dominant_pct}%</text>
                    <text x="70" y="85" text-anchor="middle" font-size="11" fill="#5F5E5A">{t_dominant_label}</text>
                </svg>
            </div>
            <div style="display:flex;flex-direction:column;justify-content:center;gap:10px;">
                <div style="display:flex;align-items:center;gap:10px;">
                    <span style="width:10px;height:10px;border-radius:50%;background:#639922;"></span>
                    <span style="color:#2C2C2A;font-size:14px;flex:1;">Positive</span>
                    <span style="font-weight:700;color:#3B6D11;">{tpos}%</span>
                </div>
                <div style="display:flex;align-items:center;gap:10px;">
                    <span style="width:10px;height:10px;border-radius:50%;background:#B4B2A9;"></span>
                    <span style="color:#2C2C2A;font-size:14px;flex:1;">Neutral</span>
                    <span style="font-weight:700;color:#5F5E5A;">{tneu}%</span>
                </div>
                <div style="display:flex;align-items:center;gap:10px;">
                    <span style="width:10px;height:10px;border-radius:50%;background:#E24B4A;"></span>
                    <span style="color:#2C2C2A;font-size:14px;flex:1;">Negative</span>
                    <span style="font-weight:700;color:#A32D2D;">{tneg}%</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        with st.expander("Show channels and sample messages"):
            for ch in tresult["channels"]:
                st.markdown(f"**{ch['channel_title']}** (@{ch['channel_username']}) — {ch['total_messages']} messages")
                for m in ch["messages"][:5]:
                    color = "#3B6D11" if m["sentiment"] == "Positive" else "#A32D2D" if m["sentiment"] == "Negative" else "#5F5E5A"
                    st.markdown(f"<span style='color:{color};font-weight:600;font-size:12px;'>{m['sentiment'].upper()}</span> {m['text'][:150]}", unsafe_allow_html=True)
                st.markdown("---")


if page == "Trends":
    st.markdown("#### Topic Trends")
    st.caption("Groups collected messages into topics and tracks how fast each topic is growing or fading, including how its sentiment changes")

    trend_col1, trend_col2 = st.columns([4, 1])
    with trend_col1:
        trend_keyword = st.text_input(
            "Trend keyword",
            placeholder="Optional: limit to one keyword, or leave empty to use all collected data",
            label_visibility="collapsed",
            key="trend_keyword_input"
        )
    with trend_col2:
        trend_clicked = st.button("Detect Trends", use_container_width=True, type="primary", key="detect_trends_btn")

    if trend_clicked:
        with st.spinner("Detecting topics and measuring their growth..."):
            try:
                trend_events = fetch_events(keyword=trend_keyword.strip() or None, limit=20000)
                st.session_state["trend_results"] = detect_topic_trends(trend_events)
            except Exception as e:
                st.session_state.pop("trend_results", None)
                st.error(f"Couldn't detect topic trends: {e}")

    if "trend_results" in st.session_state:
        render_topic_trends(st.session_state["trend_results"])


if page == "Audience":
    st.markdown("#### Audience Intelligence & Segmentation")
    st.caption("Observable audience segments built from what people write: topics, language, engagement and sentiment. No age or gender is inferred.")

    seg_col1, seg_col2 = st.columns([4, 1])
    with seg_col1:
        segment_keyword = st.text_input(
            "Segment keyword",
            placeholder="Optional: limit to one keyword, or leave empty to use all collected data",
            label_visibility="collapsed",
            key="segment_keyword_input"
        )
    with seg_col2:
        segment_clicked = st.button("Build Segments", use_container_width=True, type="primary", key="build_segments_btn")

    if segment_clicked:
        with st.spinner("Grouping collected messages into audience segments..."):
            try:
                segment_events = fetch_events(keyword=segment_keyword.strip() or None, limit=20000)
                st.session_state["audience_result"] = build_audience_segments(segment_events)
            except Exception as e:
                st.session_state.pop("audience_result", None)
                st.error(f"Couldn't build audience segments: {e}")

    if "audience_result" in st.session_state:
        render_audience_segments(st.session_state["audience_result"])


if page == "Network":
    st.markdown("#### Link & Network Analysis")
    st.caption("Maps participants and the channels or videos they post in, then finds communities, bridge nodes and how topics spread between communities")

    network_col1, network_col2 = st.columns([4, 1])
    with network_col1:
        network_keyword = st.text_input(
            "Network keyword",
            placeholder="Optional: limit to one keyword, or leave empty to use all collected data",
            label_visibility="collapsed",
            key="network_keyword_input"
        )
    with network_col2:
        network_clicked = st.button("Build Network", use_container_width=True, type="primary", key="build_network_btn")

    if network_clicked:
        with st.spinner("Building the network and measuring how topics spread..."):
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
                st.error(f"Couldn't build the network: {e}")

    if "network_result" in st.session_state:
        render_network_intelligence(st.session_state["network_result"], st.session_state["network_graph_obj"])


if page == "PULSE Signals":
    st.markdown("#### PULSE Signals")
    st.caption("Fuses trend, sentiment, audience and network evidence into explainable signals about conversations that are emerging or rising")

    signal_col1, signal_col2 = st.columns([4, 1])
    with signal_col1:
        signal_keyword = st.text_input(
            "Signal keyword",
            placeholder="Optional: limit to one keyword, or leave empty to use all collected data",
            label_visibility="collapsed",
            key="signal_keyword_input"
        )
    with signal_col2:
        signal_clicked = st.button("Detect Signals", use_container_width=True, type="primary", key="detect_signals_btn")

    if signal_clicked:
        with st.spinner("Running trend, audience and network analysis and fusing the evidence..."):
            try:
                signal_events = fetch_events(keyword=signal_keyword.strip() or None, limit=20000)
                st.session_state["signal_result"] = run_pipeline(signal_events)
                st.session_state.pop("signal_save_message", None)
            except Exception as e:
                st.session_state.pop("signal_result", None)
                st.error(f"Couldn't detect signals: {e}")

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
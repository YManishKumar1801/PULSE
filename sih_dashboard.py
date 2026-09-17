import streamlit as st
from telethon.sync import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from pymongo import MongoClient
from datetime import datetime, timezone
import re
import html
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
MONGO_URI = os.getenv("MONGO_URI")

st.set_page_config(page_title="PULSE - Social Media Analytics", layout="wide")


@st.cache_resource
def get_telegram_client():
    client = TelegramClient("pulse_session", TELEGRAM_API_ID, TELEGRAM_API_HASH)
    client.start()
    return client


@st.cache_resource
def get_vader():
    return SentimentIntensityAnalyzer()


@st.cache_resource
def get_bert():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")


@st.cache_resource
def get_multilingual_sentiment():
    return pipeline("sentiment-analysis", model="cardiffnlp/twitter-xlm-roberta-base-sentiment")


@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client["pulse_db"]["telegram_sentiment_results"]


with st.spinner("Loading models and connecting to Telegram..."):
    telegram_client = get_telegram_client()
    vader_analyzer = get_vader()
    bert_classifier = get_bert()
    xlm_classifier = get_multilingual_sentiment()
    telegram_collection = get_mongo_collection()


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


def search_telegram_channels(keyword, max_results=5):
    try:
        result = telegram_client(SearchRequest(q=keyword, limit=max_results))
        channels = []
        for chat in result.chats:
            username = getattr(chat, "username", None)
            if username:
                channels.append({"id": chat.id, "title": getattr(chat, "title", ""), "username": username})
        return channels
    except Exception as e:
        st.error(f"Telegram search error: {e}")
        return []


def fetch_channel_messages(username, limit=50):
    messages = []
    try:
        entity = telegram_client.get_entity(username)
        for message in telegram_client.iter_messages(entity, limit=limit):
            if message.text:
                messages.append({
                    "text": message.text,
                    "date": message.date,
                    "sender_id": message.sender_id,
                    "views": getattr(message, "views", None)
                })
    except Exception:
        pass
    return messages


def analyze_telegram_keyword(keyword):
    channels = search_telegram_channels(keyword)
    if not channels:
        return None

    all_results = []
    counts = {"Positive": 0, "Negative": 0, "Neutral": 0}

    for ch in channels:
        messages = fetch_channel_messages(ch["username"])
        message_docs = []

        for msg in messages:
            cleaned = clean_text(msg["text"])
            v_label, v_score = get_vader_sentiment(cleaned)
            b_label, b_score = get_bert_sentiment(cleaned)
            x_label, x_score = get_xlm_sentiment(cleaned)
            final = ensemble_sentiment(v_label, b_label, x_label)

            counts[final] += 1
            message_docs.append({
                "text": cleaned,
                "sentiment": final,
                "date": str(msg["date"]),
                "sender_id": msg["sender_id"]
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

        telegram_collection.update_one(
            {"channel_username": ch["username"], "keyword": keyword},
            {"$set": document},
            upsert=True
        )

        all_results.append(document)

    total = sum(len(r["messages"]) for r in all_results)
    return {"channels": all_results, "counts": counts, "total": total}


st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
header[data-testid="stHeader"] { display: none !important; }
div[data-testid="stToolbar"] { display: none !important; }
div[data-testid="stHeaderActionElements"] { display: none !important; }
.stApp h1 a, .stApp h2 a, .stApp h3 a, .stApp h4 a { display: none !important; }
.stApp { background-color: #FAF9F6; }
.block-container { padding-top: 2rem; max-width: 1100px; }

h1, h2, h3, h4, h5, h6 { color: #2C2C2A !important; }
p, span, label, .stMarkdown, .stCaption { color: #2C2C2A !important; }

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
}
div.stButton > button[kind="primary"]:hover { background-color: #305a0d; }

.metric-card {
    background: #fff; border-radius: 12px; padding: 20px 18px;
    border: 0.5px solid #E5E2D9; text-align: center;
    box-shadow: 0 1px 3px rgba(44,44,42,0.04);
}
.metric-label { color: #5F5E5A !important; font-size: 12px; font-weight: 500; margin-bottom: 8px; }
.metric-value { font-size: 30px; font-weight: 700; margin: 0; }

div[data-testid="stExpander"] {
    background-color: #fff;
    border: 0.5px solid #E5E2D9;
    border-radius: 12px;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
    <span style="width:7px;height:7px;border-radius:50%;background:#639922;"></span>
    <span style="width:7px;height:7px;border-radius:50%;background:#B4B2A9;"></span>
    <span style="width:7px;height:7px;border-radius:50%;background:#E24B4A;"></span>
    <span style="font-weight:600;font-size:18px;margin-left:6px;">PULSE</span>
</div>
""", unsafe_allow_html=True)
st.caption("Social Media Analytics Framework — Sentiment, Demographics, Trends & Network Intelligence")

col1, col2 = st.columns([4, 1])
with col1:
    keyword = st.text_input("Search Telegram", placeholder="e.g. cricket, news, finance...", label_visibility="collapsed")
with col2:
    search_clicked = st.button("Analyze", use_container_width=True, type="primary")

if search_clicked and keyword.strip():
    with st.spinner(f"Searching Telegram for '{keyword}'..."):
        result = analyze_telegram_keyword(keyword.strip())

    if result is None or result["total"] == 0:
        st.warning("No Telegram channels or messages found for this topic. Try a more common keyword.")
    else:
        st.session_state["last_result"] = result

if "last_result" in st.session_state:
    result = st.session_state["last_result"]
    counts = result["counts"]
    total = result["total"]

    st.markdown(f"### Results — {total} messages across {len(result['channels'])} channels")

    c1, c2, c3 = st.columns(3)
    pos_pct = round(counts["Positive"] / total * 100) if total else 0
    neu_pct = round(counts["Neutral"] / total * 100) if total else 0
    neg_pct = round(counts["Negative"] / total * 100) if total else 0

    with c1:
        st.markdown(f'<div class="metric-card"><p class="metric-label">POSITIVE</p><p class="metric-value" style="color:#3B6D11;">{pos_pct}%</p></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><p class="metric-label">NEUTRAL</p><p class="metric-value" style="color:#5F5E5A;">{neu_pct}%</p></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><p class="metric-label">NEGATIVE</p><p class="metric-value" style="color:#A32D2D;">{neg_pct}%</p></div>', unsafe_allow_html=True)

    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

    with st.expander("Show channels and sample messages"):
        for ch in result["channels"]:
            st.markdown(f"**{ch['channel_title']}** (@{ch['channel_username']}) — {ch['total_messages']} messages")
            for m in ch["messages"][:5]:
                color = "#3B6D11" if m["sentiment"] == "Positive" else "#A32D2D" if m["sentiment"] == "Negative" else "#5F5E5A"
                st.markdown(f"<span style='color:{color};font-weight:600;font-size:12px;'>{m['sentiment'].upper()}</span> {m['text'][:150]}", unsafe_allow_html=True)
            st.markdown("---")
else:
    st.info("Enter a keyword above and click Analyze to search Telegram channels.")
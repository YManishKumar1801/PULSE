# PULSE

**Platform for Understanding Live Sentiment using big data Extraction**

PULSE is a real-time public sentiment analysis dashboard. Give it any topic — a product, a public figure, a news event — and it pulls live opinion from YouTube and news sources, runs it through a streaming NLP pipeline, and shows you what people actually think, and why.

Built as an educational project to explore real-time data pipelines, streaming architectures, and applied NLP.

## What it does

- **Live keyword search** — type any topic and get real-time sentiment from YouTube comments and news articles, not a stale dataset
- **A real streaming pipeline** — data flows through Apache Kafka (Producer → Topic → Consumer) before it's ever analyzed, not a shortcut straight from API to dashboard
- **Ensemble sentiment analysis** — VADER (lexicon-based) and DistilBERT (transformer-based) vote together, so single-model blind spots matter less
- **Reason summarization in multiple languages** — an mT5-based model explains *why* sentiment leans positive or negative, and understands Hindi and Hinglish as well as English
- **Emotion breakdown** — optional joy / anger / fear / sadness / surprise detection on top of plain sentiment
- **Sarcasm flagging** — flags comments that are likely being sarcastic, so "great, another price hike 🙄" doesn't get counted as genuine praise
- **Platform comparison** — see how sentiment differs between YouTube and news coverage on the same topic
- **Keyword comparison mode** — put two topics side by side and compare their sentiment directly
- **Trending topic suggestions** — one click to analyze whatever's actually trending on YouTube right now
- **Authentication** — email/password (Firebase) and native Google sign-in
- **Persistent storage** — every analysis is saved to MongoDB Atlas for later reference

## How it's built

| Layer | Technology |
|---|---|
| Data sources | YouTube Data API v3, NewsAPI.org |
| Streaming | Apache Kafka + Zookeeper (Docker) |
| Sentiment | VADER, DistilBERT (HuggingFace Transformers) |
| Summarization | mT5_multilingual_XLSum |
| Emotion detection | j-hartmann/emotion-english-distilroberta-base |
| Sarcasm detection | helinivan/english-sarcasm-detector |
| Database | MongoDB Atlas |
| Frontend | Streamlit |
| Auth | Firebase Authentication, Google OIDC |

## Architecture

```
YouTube API + NewsAPI  →  Kafka Producer  →  Kafka Topic  →  Kafka Consumer
                                                                    ↓
                                                    Text Cleaning + Sentiment Analysis
                                                                    ↓
                                                    MongoDB Atlas  +  Streamlit Dashboard
```

Nothing gets analyzed until it's passed through Kafka first — the goal was to build something that actually behaves like a streaming system, not just call it one.

## Setup

### 1. Clone and set up the environment

```
git clone <your-repo-url>
cd pulse
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Start Kafka

Requires Docker Desktop running.

```
docker-compose up -d
```

### 3. Add your credentials

Create a `.env` file in the project root:

```
YOUTUBE_API_KEY=your_youtube_api_key
MONGO_URI=your_mongodb_connection_string
FIREBASE_API_KEY=your_firebase_web_api_key
NEWS_API_KEY=your_newsapi_key
```

Create `.streamlit/secrets.toml` for Google sign-in:

```toml
[auth]
redirect_uri = "http://localhost:8501/oauth2callback"
cookie_secret = "a-random-string"

[auth.google]
client_id = "your-google-client-id"
client_secret = "your-google-client-secret"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
```

### 4. Run the dashboard

```
python -m streamlit run app.py
```

## Project Files

| File | Purpose |
|---|---|
| `app.py` | Main Streamlit dashboard — Kafka producer/consumer, sentiment pipeline, and UI all live here |
| `docker-compose.yml` | Kafka + Zookeeper container setup |
| `requirements.txt` | Python dependencies |
| `.streamlit/config.toml` | Dashboard theme configuration |
| `.env` / `.streamlit/secrets.toml` | API credentials — never committed to version control |

## Known Limitations

- The sentiment, emotion, and sarcasm models are primarily English-trained, so accuracy dips on Hindi/Hinglish text specifically for those models. The reason summarization model is genuinely multilingual and handles this better.
- Free-tier API quotas apply: YouTube Data API (10,000 units/day), NewsAPI (100 requests/day).
- Reddit was evaluated as a data source but excluded after Reddit's 2026 API policy changes made live access impractical for a project at this scale. YouTube and NewsAPI serve as the live sources instead.

## Why this project exists

This was built to get hands-on with the full stack of a real-time data product — not just training a model on a static CSV, but wiring together live APIs, a message broker, an NLP pipeline, a database, and a usable interface, and dealing with everything that breaks along the way when data is messy and arrives whenever it wants to.
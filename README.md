# PULSE

**Platform for Understanding Live Sentiment using big data Extraction**

PULSE is an audience-intelligence and emerging-signal detection platform. It collects public conversation about a topic from YouTube, news and Telegram, normalizes everything into one event format, and combines four analysis vectors into explainable **PULSE Signals**:

1. **Sentiment**: how people feel
2. **Audience Intelligence**: which observable audience segments are talking
3. **Trend Tracking**: which topics are emerging, rising, persistent or declining, and how fast
4. **Network Analysis**: which communities exist and how a topic spreads between them

Each signal answers six questions: what is happening, who is involved, how they feel, how it spreads, how fast, and why it matters. Every number on a signal comes from a defined calculation on stored data.

This is a hackathon prototype built to explore streaming data, applied NLP and graph analysis. It is not a production system. See [Limitations](#limitations) before quoting any result.

## What is implemented

| Area | What it does today |
|---|---|
| Collection | YouTube Data API (videos and comments), NewsAPI (articles), Telegram via Telethon (channel messages) |
| Streaming | YouTube and news items pass through a single-node Kafka topic (`pulse-youtube`) before analysis. A separate continuously running worker can consume normalized events from `pulse.events` (see [Processing worker](#processing-worker)) |
| Unified schema | Every source is normalized into one event format with a deterministic `event_id`, so re-collecting the same message does not create duplicates |
| Storage | MongoDB Atlas `pulse_db.events` (events and their analysis) and `pulse_db.signals`, with upserts and unique indexes |
| Sentiment | VADER + DistilBERT + XLM-RoBERTa majority vote, a confidence proxy, language detection, emotion and sarcasm (English only) |
| Audience | TF-IDF + KMeans segments (cluster count picked by silhouette score), interest hints, per-segment sentiment, trend and language mix |
| Trends | TF-IDF + NMF topics, time-window growth, velocity, sentiment shift, and a significance check before a topic counts as Emerging or Rising |
| Network | Author-to-source graph, Louvain communities, degree, PageRank, betweenness, bridge nodes, topic propagation across communities |
| Fusion | Weighted fusion of the four vectors into a score, with transparent weights and a corroboration rule |
| Signals | Emerging and Rising signals with priority, score breakdown and a confidence proxy |
| Explanations | Optional Gemini explanation written only from the signal's detected evidence; numbers in the reply are checked against the evidence and the reply is rejected if they do not match |
| Dashboard | Streamlit pages written in plain language with a consistent layout: Command Center, Live Search (YouTube & News and Telegram in one page), Sentiment, Trends, Audience, Network, PULSE Signals. Each page has a short "How to read this page" guide, equal-height summary cards and the technical method in an expander. Signals can be downloaded as CSV |
| Auth | Firebase email/password and Streamlit Google sign-in |
| Resilience | Retries for transient MongoDB errors, classified Gemini failures with model fallback, per-service startup guards, clear warnings for API key, quota and timeout problems |

## How a signal is built

```
YouTube / News / Telegram
          |
     normalizer  ->  unified events
          |
        Kafka (YouTube and news path)
          |
      MongoDB events  <-  sentiment analysis (analysis field)
          |
   +------+--------+----------------+
   |               |                |
 Trends        Audience         Network
 (topics)     (segments)     (communities)
   |               |                |
   +------+--------+----------------+
          |
   Signal Fusion (per topic)
          |
     PULSE Signals  ->  optional Gemini explanation  ->  MongoDB signals + dashboard
```

### Fusion rules

Each topic gets a score from 0 to 100:

| Vector | Weight | What feeds it |
|---|---|---|
| Trend | 0.35 | Growth of the topic between the latest window and the one before |
| Sentiment | 0.25 | Size of the sentiment shift between windows (or overall polarity when only one window has data) |
| Network | 0.25 | Share of detected communities the topic has reached |
| Audience | 0.15 | How concentrated the topic is in one audience segment |

If a vector is unavailable for a topic, the remaining weights are renormalized and the signal lists that vector as missing.

A signal is created only for topics whose trend status is Emerging or Rising and whose score is at least 25. Priority is low by default, medium at score 40 or more with at least one corroborating vector, and high at score 60 or more with at least two. A vector corroborates when its component is at least 0.3.

The weights and thresholds are baseline heuristics chosen for transparency. **They have not been validated against labelled real-world signals.**

The confidence value on a signal is a **proxy** (0.4 data volume, 0.3 vector availability, 0.3 corroboration). It is not a calibrated probability.

### Trend significance

A topic is only called Emerging or Rising if its growth is larger than count noise: `(current - previous) / sqrt(current + previous)` must be at least 2. This was added after the synthetic benchmark showed small counts being flagged as growth.

## Tech stack

| Layer | Technology |
|---|---|
| Data sources | YouTube Data API v3, NewsAPI.org, Telegram (Telethon) |
| Streaming | Apache Kafka + Zookeeper (Docker, single node) |
| Sentiment | VADER, `distilbert-base-uncased-finetuned-sst-2-english`, `cardiffnlp/twitter-xlm-roberta-base-sentiment` |
| Emotion | `j-hartmann/emotion-english-distilroberta-base` (English only) |
| Sarcasm | `helinivan/english-sarcasm-detector` (English only) |
| Audience, trends | scikit-learn (TF-IDF, KMeans, NMF) |
| Network | NetworkX (Louvain, PageRank, betweenness), PyVis |
| Explanations | Gemini via the Generative Language REST API (optional) |
| Database | MongoDB Atlas |
| Frontend | Streamlit |
| Auth | Firebase Authentication, Google OIDC |

## Setup

### 1. Environment

```
git clone https://github.com/YManishKumar1801/PULSE.git
cd PULSE
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Streamlit 1.39 or newer is required (the pages use keyed containers). Keep your virtual environment folder (`venv`, `venv311`, ...) out of git; `.gitignore` covers `venv*/`.

```
```

### 2. Kafka

Docker Desktop must be running.

```
docker-compose up -d
```

### 3. Credentials

Copy `.env.example` to `.env` and fill in the values:

| Variable | Used for |
|---|---|
| `YOUTUBE_API_KEY` | YouTube search and comments |
| `NEWS_API_KEY` | News search |
| `GEMINI_API_KEY` | Optional signal and sentiment explanations |
| `MONGO_URI` | MongoDB Atlas connection string |
| `FIREBASE_API_KEY` | Email/password sign-in |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Telegram search |

For Google sign-in create `.streamlit/secrets.toml`:

```toml
[auth]
redirect_uri = "http://localhost:8501/oauth2callback"
cookie_secret = "a-random-string"

[auth.google]
client_id = "your-google-client-id"
client_secret = "your-google-client-secret"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
```

If a variable is missing, the dashboard starts anyway and lists the unavailable feature in an expander at the top.

### 4. Telegram session (one time)

Telethon asks for your phone number and a login code the first time. Do this once in the terminal, not inside Streamlit:

```
python -c "import os; from dotenv import load_dotenv; load_dotenv(); from telethon.sync import TelegramClient; TelegramClient('pulse_session', int(os.getenv('TELEGRAM_API_ID')), os.getenv('TELEGRAM_API_HASH')).start()"
```

This creates `pulse_session.session`. It is git-ignored. Never commit it.

### 5. Run

```
python -m streamlit run app.py
```

### 6. Analyze stored events

Sentiment analysis is stored on each event. Events collected earlier, or by another path, can be analyzed in batches:

```
python analyze_pending.py
```

## Processing worker

The dashboard collects data when you press search. A separate worker adds a continuously running processing path that does not depend on the dashboard:

```
Producer  ->  Kafka topic pulse.events  ->  worker  ->  sentiment analysis  ->  MongoDB  ->  dashboard
                                              |
                                      malformed messages -> pulse.dead
```

Start the worker in its own terminal (Kafka and your `.env` must be ready):

```
python workers/run_worker.py
```

Publish events to it from another terminal:

```
python workers/publish_events.py --synthetic 200
python workers/publish_events.py --from-json events.json
python workers/publish_events.py --from-mongo --keyword ipl --limit 300
```

What the worker does:

- reads batches with manual offset commits, so an offset is committed only after the batch is stored (at-least-once delivery; writes are idempotent because `event_id` is deterministic)
- validates each message against the event schema and sends malformed ones to `pulse.dead` with the reason
- skips duplicates inside a batch and events that already have an analysis
- saves events first, then analysis; if the analysis step fails, the events stay stored and `python analyze_pending.py` can analyze them later
- on a MongoDB outage it rewinds to the start of the batch, waits and retries without committing
- writes a heartbeat to MongoDB (`worker_status`) with counts, consumer lag and batch latency; the Command Center shows it

`--synthetic` publishes generated test messages. They are useful for load and failure testing, not for analysis; they use the keyword `synthetic`.

### Standalone collector

The collectors also run without the dashboard. `workers/collector.py` fetches YouTube comments, news articles and Telegram messages for one or more keywords, normalizes them into events and publishes them to `pulse.events`:

```
python workers/collector.py "ipl" "budget" --sources youtube news telegram
python workers/collector.py "ipl" --sources news --interval 900
python workers/collector.py "ipl" --dry-run
```

- `--interval 900` repeats the collection every 15 minutes until you press Ctrl+C
- `--dry-run` collects and prints counts without publishing
- a source whose key or session is missing is skipped with a message, and the others still run
- the Telegram session must already be logged in (see the Telegram session step); the collector never waits for a login prompt

Together with the worker this gives the path `collector -> Kafka -> worker -> sentiment -> MongoDB -> dashboard`. The dashboard's own search buttons use the same collection code (`ingestion/sources.py`).

## Using the dashboard

1. Collect data on **Live Search**. Choose **YouTube & News** or **Telegram** at the top of the page.
2. Run `python analyze_pending.py` if the Sentiment page shows unanalyzed events.
3. Open **Command Center** for totals, top signals, trend velocity and the system status.
4. **Sentiment**: how people feel overall, over time, by platform and by language.
5. **Trends**: press **Find trends** to see which topics are new, growing, steady or fading.
6. **Audience**: press **Find groups** to see the groups of conversation, their mood, languages and platforms.
7. **Network**: press **Map network** to see communities, connectors between them and how topics spread.
8. **PULSE Signals**: press **Find signals**, then **Explain in simple words** for a written explanation. **Save these signals** stores them in MongoDB and **Download signals as CSV** exports them.

Every page has a "How to read this page" box and a "How was this worked out?" box. Words like TF-IDF, PageRank or velocity are kept in those boxes, not in the main view.

Signals need enough data spread over time. With too few messages, or messages from a very short span, the pages say so instead of producing weak results.

## Tests

```
python tests/test_audience.py
python tests/test_trends.py
python tests/test_network.py
python tests/test_fusion.py
python tests/test_explain.py
python tests/test_overview.py
python tests/test_evaluation.py
python tests/test_resilience.py
python tests/test_nlp_robustness.py
python tests/test_security.py
python tests/test_worker.py
python tests/test_ingestion_sources.py
python tests/test_ui_sentiment.py
```

Most of them also run with `python -m pytest tests -q`. Two helper functions in `test_audience.py` and `test_network.py` take arguments and are meant to be run through their own script, not collected by pytest.

The tests use synthetic data and stubbed models and do not need network access, API keys or model downloads. `python check_security.py` scans the project for hardcoded secrets and tracked sensitive files.

## Evaluation

All numbers below are produced by scripts in `evaluation/`. Nothing here is estimated.

### Sentiment (needs your labels)

```
python evaluation/export_labeling_sample.py --size 300
python evaluation/evaluate_sentiment.py
```

The first command writes a labelling sheet without model predictions. Label it following `evaluation/LABELING_GUIDE.md`, then the second command reports accuracy, per-class precision and recall, macro F1, the confusion matrix, per-model scores, and breakdowns by detected language, annotated language and platform. The report is saved to `evaluation/results/sentiment_report.json`.

Results on the project's own labelled sample: **not yet recorded in this README.** Add them here, with the sample size, after running the evaluation.

### Trend detection (synthetic)

```
python evaluation/trend_benchmark.py
```

This plants one surging topic into simulated background chatter (20 scenarios: 10 with a planted topic, 10 without) and checks whether the detector flags it. In the run recorded for this README (10 planted and 10 control scenarios, 6 hour checkpoint resolution):

| Measure | Result |
|---|---|
| Planted topic detected | 10 of 10 scenarios |
| Median detection latency | 12 hours after the surge began |
| False flags per control checkpoint | 0.186 |
| Share of flags that were on the planted topic | 0.634 |

This only shows that the detector reacts to a known surge in simulated data. It is not accuracy on real conversations. Before the significance check was added, the same benchmark produced 1.2 false flags per control checkpoint on a 6-scenario run.

### System performance

```
python evaluation/benchmark_system.py
```

Measures sentiment inference throughput and chunk latency (average and P95), pipeline run time and MongoDB write rate, and records the machine it ran on. Results depend on your hardware and network; quote them together with the environment block in `evaluation/results/system_benchmark.json`.

Kafka lag and worker throughput are reported by the processing worker itself (`consumer_lag`, `events_per_busy_second`, `batch_seconds_p95`) in its final printout and in the `worker_status` collection. To measure them, publish a known number of events with `--synthetic`, run the worker until the lag is 0, and record the numbers together with your machine. None have been recorded in this README yet.

## Project structure

```
app.py                   Streamlit dashboard
ui_sentiment.py          Donut, bar and summary HTML helpers used by the pages
config/env.py            Environment variable helpers
ingestion/sources.py     YouTube, news and Telegram collection functions
ingestion/normalizer.py  Source-to-event normalization
models/schemas.py        Unified event schema and validation
intelligence/
  sentiment.py           Sentiment, emotion, sarcasm, language
  audience.py            Audience segmentation
  trends.py              Topic trends
  network.py             Network and propagation analysis
  fusion.py              Signal fusion
  signals.py             PULSE Signal generation and pipeline
  explain.py             Evidence-grounded explanations
  gemini_client.py       Gemini client with retry and fallback
  overview.py            Plain-language page data for Command Center, Sentiment, Trends, Audience, Network and Signals
storage/mongodb.py       MongoDB access with retry
evaluation/              Labelling, metrics, benchmarks
tests/                   Test suite
workers/
  processor.py           Kafka batch worker (validation, dead letters, retry, lag)
  run_worker.py          Worker entry point
  collector.py           Standalone collector that publishes events to Kafka
  publish_events.py      Event publisher (JSON, Mongo replay, synthetic)
analyze_pending.py       Batch sentiment analysis of stored events
check_security.py        Secret and tracked-file scanner
docker-compose.yml       Kafka and Zookeeper
```

### Legacy scripts

These files come from the first version of the project. The dashboard no longer imports any of them, and they use older, inconsistent logic (two-model sentiment, per-script storage). They are kept for reference and should not be used for results:

`sentiment_pipeline.py`, `store_to_mongo.py`, `youtube_fetch.py`, `telegram_fetch.py`, `telegram_sentiment.py`, `demographics.py`, `trend_detection.py`, `network_analysis.py`, `kafka_consumer_test.py`, `spark_test.py`, `generate_telegram_session.py`

## Limitations

- **Sentiment accuracy is not yet reported.** The ensemble has not been scored on a labelled sample in this README. Emotion and sarcasm models are English-only, and the sentiment models are weaker on Hindi and Hinglish. Language detection cannot identify Hinglish written in Latin script.
- **Confidence values are proxies**, not calibrated probabilities. This applies to both the sentiment `confidence_proxy` and the signal confidence.
- **Page text describes what PULSE found, not what is true.** Plain wording such as "people are getting more negative" comes from model labels on the collected messages, so it inherits the sentiment limitations above.
- **Fusion weights are not validated.** They are documented baseline choices.
- **Audience and trend vectors are both text-based.** Segments and topics are built from similar word statistics, so audience concentration is only weak independent evidence for a trend.
- **Audience segments are observable groups**, built from what people write. PULSE does not infer age, gender or other personal attributes.
- **Network analysis covers YouTube and Telegram only.** News has no author or community structure. Telegram broadcast channels often have the channel itself as the author, and YouTube display names are not unique IDs, so graphs can be sparse and communities uncertain. The page lists these caveats when they apply.
- **Influence is described by graph measures** (degree, PageRank, betweenness). These show structural position, not proven influence.
- **Kafka is a local single node.** The dashboard's own collection path uses it in a request and response pattern. The processing worker and the standalone collector are continuous, but the dashboard's search buttons still collect on demand. No lag or throughput numbers are recorded here, and there is no scalability claim. The worker and collector have been tested with fake clients; run them against your broker and real APIs before relying on them.
- **Gemini explanations are generated text.** Numbers are checked against the evidence, but wording is not verified and is not proof of cause. Without a Gemini key or when it fails, a plain summary built from the evidence is shown.
- **Synthetic benchmarks do not measure real-world accuracy.**
- **API quotas apply**: YouTube Data API (10,000 units per day) and NewsAPI free tier limits.
- Reddit was evaluated and excluded after 2026 API policy changes.

## Planned, not implemented

- Splitting `app.py` into a `ui/` package
- Validating fusion weights against labelled signals (for example, checking whether topics flagged as Emerging kept growing in the next window)
- A better approach for Hinglish text, evaluated on the labelled sample before it is claimed
- Alerts when a high-priority signal appears

## Security

- API keys, the Mongo URI and Firebase settings come from environment variables or `.streamlit/secrets.toml`.
- `.env`, `.streamlit/secrets.toml` and `*.session` are git-ignored. Do not commit them.
- A key that was committed once stays in git history. Rotate it even after deleting the file.
- Run `python check_security.py` before pushing.

## Why this project exists

PULSE was built to get hands-on with the full stack of a data product: live APIs, a message broker, an NLP pipeline, graph analysis, a database and a usable interface, and to deal with everything that breaks when data is messy and arrives whenever it wants to.

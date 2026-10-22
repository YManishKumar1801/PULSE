import re
import html
from datetime import datetime, timezone

from langdetect import DetectorFactory, LangDetectException, detect

DetectorFactory.seed = 0

ENGINE_VERSION = "1.0"

BERT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"
XLM_MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
EMOTION_MODEL = "j-hartmann/emotion-english-distilroberta-base"
SARCASM_MODEL = "helinivan/english-sarcasm-detector"

BERT_NEUTRAL_THRESHOLD = 0.75
SARCASM_THRESHOLD = 0.6
MAX_LENGTH = 512
BATCH_SIZE = 16
MIN_EMOTION_LENGTH = 15
MIN_SARCASM_LENGTH = 10
MIN_LANGUAGE_LENGTH = 10

LANGUAGE_NAMES = {
    "en": "English", "hi": "Hindi", "mr": "Marathi", "ta": "Tamil",
    "te": "Telugu", "bn": "Bengali", "gu": "Gujarati", "kn": "Kannada",
    "ml": "Malayalam", "pa": "Punjabi", "ur": "Urdu", "ne": "Nepali"
}

_resources = {}


def _get_vader():
    if "vader" not in _resources:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _resources["vader"] = SentimentIntensityAnalyzer()
    return _resources["vader"]


def _get_pipeline(key, task, model):
    if key not in _resources:
        from transformers import pipeline
        _resources[key] = pipeline(task, model=model)
    return _resources[key]


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


def detect_language(text):
    if not text or len(text.strip()) < MIN_LANGUAGE_LENGTH:
        return "unknown", "unknown"
    try:
        code = detect(text)
    except LangDetectException:
        return "unknown", "unknown"
    return code, LANGUAGE_NAMES.get(code, code)


def vader_sentiment(text):
    scores = _get_vader().polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "Positive"
    elif compound <= -0.05:
        label = "Negative"
    else:
        label = "Neutral"
    return label, round(compound, 3)


def _run_pipeline(pipe, texts):
    if not texts:
        return []
    try:
        return pipe(texts, batch_size=BATCH_SIZE, truncation=True, max_length=MAX_LENGTH)
    except Exception:
        results = []
        for text in texts:
            try:
                results.append(pipe(text, truncation=True, max_length=MAX_LENGTH)[0])
            except Exception:
                results.append(None)
        return results


def _bert_label(result):
    if result is None:
        return "Neutral", 0.0
    label = "Positive" if result["label"].upper() == "POSITIVE" else "Negative"
    score = float(result["score"])
    if score < BERT_NEUTRAL_THRESHOLD:
        label = "Neutral"
    return label, round(score, 3)


def _xlm_label(result):
    if result is None:
        return "Neutral", 0.0
    label_map = {"positive": "Positive", "neutral": "Neutral", "negative": "Negative"}
    label = label_map.get(result["label"].lower(), "Neutral")
    return label, round(float(result["score"]), 3)


def ensemble(vader_label, bert_label, xlm_label):
    votes = [vader_label, bert_label, xlm_label]
    counts = {}
    for vote in votes:
        counts[vote] = counts.get(vote, 0) + 1
    best = max(counts, key=counts.get)
    if counts[best] >= 2:
        return best, counts[best]
    return xlm_label, 1


def confidence_proxy(final_label, vader, bert, xlm):
    matching = []
    if vader[0] == final_label:
        matching.append(abs(vader[1]))
    if bert[0] == final_label:
        matching.append(bert[1])
    if xlm[0] == final_label:
        matching.append(xlm[1])
    return round(max(matching), 3) if matching else 0.5


def _empty_analysis(cleaned, code, name, timestamp):
    return {
        "language": {"code": code, "name": name},
        "cleaned_text": cleaned,
        "sentiment": {
            "label": "Neutral",
            "confidence_proxy": 0.0,
            "agreement": 0,
            "models": {}
        },
        "emotion": None,
        "sarcasm": None,
        "engine_version": ENGINE_VERSION,
        "analyzed_at": timestamp
    }


def analyze_texts(texts, with_emotion=True, with_sarcasm=True):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    cleaned_list = [clean_text(t) for t in texts]
    languages = [detect_language(c) for c in cleaned_list]
    analyses = [_empty_analysis(c, l[0], l[1], timestamp) for c, l in zip(cleaned_list, languages)]

    active = [i for i, c in enumerate(cleaned_list) if c]
    if not active:
        return analyses

    active_texts = [cleaned_list[i] for i in active]

    bert_results = _run_pipeline(_get_pipeline("bert", "sentiment-analysis", BERT_MODEL), active_texts)
    xlm_results = _run_pipeline(_get_pipeline("xlm", "sentiment-analysis", XLM_MODEL), active_texts)

    for position, index in enumerate(active):
        text = cleaned_list[index]
        vader = vader_sentiment(text)
        bert = _bert_label(bert_results[position])
        xlm = _xlm_label(xlm_results[position])
        final, agreement = ensemble(vader[0], bert[0], xlm[0])
        analyses[index]["sentiment"] = {
            "label": final,
            "confidence_proxy": confidence_proxy(final, vader, bert, xlm),
            "agreement": agreement,
            "models": {
                "vader": {"label": vader[0], "score": vader[1]},
                "distilbert": {"label": bert[0], "score": bert[1]},
                "xlm_roberta": {"label": xlm[0], "score": xlm[1]}
            }
        }

    english = [i for i in active if languages[i][0] == "en"]

    if with_emotion:
        targets = [i for i in english if len(cleaned_list[i]) >= MIN_EMOTION_LENGTH]
        results = _run_pipeline(
            _get_pipeline("emotion", "text-classification", EMOTION_MODEL),
            [cleaned_list[i] for i in targets]
        )
        for i, result in zip(targets, results):
            if result is not None:
                analyses[i]["emotion"] = {
                    "label": result["label"],
                    "score": round(float(result["score"]), 3)
                }

    if with_sarcasm:
        targets = [i for i in english if len(cleaned_list[i]) >= MIN_SARCASM_LENGTH]
        results = _run_pipeline(
            _get_pipeline("sarcasm", "text-classification", SARCASM_MODEL),
            [cleaned_list[i] for i in targets]
        )
        for i, result in zip(targets, results):
            if result is not None:
                label = str(result["label"]).lower()
                score = float(result["score"])
                analyses[i]["sarcasm"] = {
                    "flag": label in ("sarcastic", "label_1", "1") and score >= SARCASM_THRESHOLD,
                    "label": label,
                    "score": round(score, 3)
                }

    return analyses


def analyze_text(text, with_emotion=True, with_sarcasm=True):
    return analyze_texts([text], with_emotion, with_sarcasm)[0]


def analyze_events(events, with_emotion=True, with_sarcasm=True):
    analyses = analyze_texts([e.get("text", "") for e in events], with_emotion, with_sarcasm)
    return [(event["event_id"], analysis) for event, analysis in zip(events, analyses)]
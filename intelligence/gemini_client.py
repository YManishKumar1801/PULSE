import time

import requests

GEMINI_MODELS = ["gemini-3.5-flash", "gemini-flash-latest", "gemini-2.5-flash", "gemini-3.1-flash-lite"]
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
ATTEMPTS_PER_MODEL = 2
RETRY_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT_SECONDS = 20
MAX_TOTAL_SECONDS = 45
ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

ERROR_MESSAGES = {
    "no_key": "No Gemini API key is configured.",
    "invalid_key": "The Gemini API key was rejected. Check GEMINI_API_KEY.",
    "quota": "The Gemini quota or rate limit was reached. Try again later.",
    "timeout": "Gemini did not answer in time.",
    "unavailable": "Gemini is temporarily unavailable.",
    "empty": "Gemini returned no usable text.",
    "network": "Could not reach Gemini.",
    "deadline": "Gemini kept failing and the time limit was reached.",
}


def classify_status(status, body_text=""):
    lowered = (body_text or "").lower()
    if status in (401, 403):
        return "invalid_key"
    if status == 400 and "api key" in lowered:
        return "invalid_key"
    if status == 429:
        return "quota"
    if status in RETRYABLE_STATUSES:
        return "unavailable"
    if status == 404:
        return "model_not_found"
    return "unavailable"


def _extract_text(data):
    candidates = data.get("candidates") or []
    if not candidates:
        return ""
    parts = (candidates[0].get("content") or {}).get("parts") or []
    return " ".join(part.get("text", "") for part in parts if part.get("text")).strip()


def generate_text(
    prompt,
    api_key,
    models=None,
    http=None,
    sleep=time.sleep,
    clock=time.monotonic,
    attempts_per_model=ATTEMPTS_PER_MODEL,
    retry_delay=RETRY_DELAY_SECONDS,
    timeout=REQUEST_TIMEOUT_SECONDS,
    max_total_seconds=MAX_TOTAL_SECONDS,
):
    if not api_key:
        return {"text": None, "error": "no_key", "model": None, "message": ERROR_MESSAGES["no_key"]}

    http = http or requests
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    started = clock()
    last_error = "unavailable"

    for model in models or GEMINI_MODELS:
        for attempt in range(attempts_per_model):
            if clock() - started > max_total_seconds:
                return {"text": None, "error": "deadline", "model": None, "message": ERROR_MESSAGES["deadline"]}
            try:
                response = http.post(ENDPOINT.format(model=model), headers=headers, json=payload, timeout=timeout)
            except requests.Timeout:
                last_error = "timeout"
                sleep(retry_delay)
                continue
            except requests.RequestException:
                last_error = "network"
                sleep(retry_delay)
                continue

            if response.status_code == 200:
                try:
                    text = _extract_text(response.json())
                except ValueError:
                    text = ""
                if text:
                    return {"text": text, "error": None, "model": model, "message": None}
                last_error = "empty"
                break

            kind = classify_status(response.status_code, getattr(response, "text", ""))
            if kind == "invalid_key":
                return {"text": None, "error": kind, "model": model, "message": ERROR_MESSAGES[kind]}
            if kind == "model_not_found":
                last_error = "unavailable"
                break
            last_error = kind
            if response.status_code in RETRYABLE_STATUSES:
                sleep(retry_delay)
                continue
            break

    return {"text": None, "error": last_error, "model": None, "message": ERROR_MESSAGES.get(last_error, ERROR_MESSAGES["unavailable"])}
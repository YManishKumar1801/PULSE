import hashlib
from datetime import datetime, timezone

PLATFORMS = ("youtube", "telegram", "news")
SOURCE_TYPES = ("comment", "message", "article")
MAX_TEXT_LENGTH = 5000

REQUIRED_FIELDS = (
    "event_id",
    "platform",
    "source_type",
    "source_id",
    "text",
    "timestamp",
    "collected_at",
    "engagement",
)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_timestamp(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH]
    return text


def build_event_id(platform, source_id, author_id, timestamp, text):
    raw = f"{platform}|{source_id}|{author_id}|{timestamp}|{text}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:32]


def make_event(
    platform,
    source_type,
    source_id,
    text,
    author_id=None,
    author_name=None,
    timestamp=None,
    likes=0,
    replies=0,
    shares=0,
    views=0,
    source_url=None,
    source_name=None,
    keyword=None,
    language=None,
    collected_at=None,
):
    if platform not in PLATFORMS:
        raise ValueError(f"unsupported platform: {platform}")
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"unsupported source_type: {source_type}")

    cleaned = clean_text(text)
    if not cleaned:
        return None

    parsed_ts = parse_timestamp(timestamp)
    collected = collected_at or utc_now_iso()
    author_id_str = str(author_id) if author_id not in (None, "") else None
    source_id_str = str(source_id) if source_id not in (None, "") else "unknown"

    return {
        "event_id": build_event_id(platform, source_id_str, author_id_str, parsed_ts, cleaned),
        "platform": platform,
        "source_type": source_type,
        "source_id": source_id_str,
        "source_name": source_name,
        "author_id": author_id_str,
        "author_name": author_name,
        "text": cleaned,
        "timestamp": parsed_ts or collected,
        "collected_at": collected,
        "engagement": {
            "likes": to_int(likes),
            "replies": to_int(replies),
            "shares": to_int(shares),
            "views": to_int(views),
        },
        "source_url": source_url,
        "keyword": keyword,
        "language": language,
    }


def validate_event(event):
    if not isinstance(event, dict):
        return False
    for field in REQUIRED_FIELDS:
        if field not in event:
            return False
    if event["platform"] not in PLATFORMS:
        return False
    if event["source_type"] not in SOURCE_TYPES:
        return False
    if not event["text"]:
        return False
    if not isinstance(event["engagement"], dict):
        return False
    return True


def dedupe_events(events):
    seen = set()
    unique = []
    for event in events:
        event_id = event["event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        unique.append(event)
    return unique
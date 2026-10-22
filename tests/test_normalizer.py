import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.normalizer import normalize
from models.schemas import validate_event


def test_youtube():
    videos = [
        {
            "video_id": "abc123",
            "title": "AI layoffs explained",
            "channel": "TechChannel",
            "keyword": "AI layoffs",
            "comments": [
                {"text": "Ye bahut bura hua", "author": "user1", "likes": 5, "published_at": "2026-10-01T10:00:00Z"},
                {"text": "Ye bahut bura hua", "author": "user1", "likes": 5, "published_at": "2026-10-01T10:00:00Z"},
                {"text": "   ", "author": "user2", "likes": 1, "published_at": "2026-10-01T11:00:00Z"},
            ],
        }
    ]
    events = normalize("youtube", videos, "AI layoffs")
    assert len(events) == 1
    assert validate_event(events[0])
    assert events[0]["engagement"]["likes"] == 5
    assert events[0]["timestamp"] == "2026-10-01T10:00:00+00:00"
    print("youtube ok")


def test_news():
    articles = [
        {
            "title": "Tech firms cut jobs",
            "description": "Several companies announced layoffs.",
            "url": "https://example.com/a1",
            "source": {"name": "Example News"},
            "publishedAt": "2026-10-01T08:30:00Z",
        }
    ]
    events = normalize("news", articles, "AI layoffs")
    assert len(events) == 1
    assert events[0]["source_type"] == "article"
    assert events[0]["text"].startswith("Tech firms cut jobs.")
    assert validate_event(events[0])
    print("news ok")


def test_telegram():
    channels = [
        {
            "channel_title": "Tech Talks",
            "channel_username": "techtalks",
            "keyword": "AI layoffs",
            "messages": [
                {"text": "Layoffs badh rahe hain", "date": "2026-10-01T09:15:00+00:00", "sender_id": 998877, "views": 120},
                {"text": "", "date": "2026-10-01T09:20:00+00:00", "sender_id": 998878, "views": 10},
            ],
        }
    ]
    events = normalize("telegram", channels)
    assert len(events) == 1
    assert events[0]["author_id"] == "998877"
    assert events[0]["engagement"]["views"] == 120
    assert events[0]["source_url"] == "https://t.me/techtalks"
    assert validate_event(events[0])
    print("telegram ok")


if __name__ == "__main__":
    test_youtube()
    test_news()
    test_telegram()
    print("all normalizer tests passed")
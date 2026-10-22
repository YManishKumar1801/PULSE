import os
import sys
from types import SimpleNamespace

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion import sources
from ingestion.sources import SourceError, fetch_news, fetch_telegram_messages, fetch_youtube_comments, search_telegram_channels, search_youtube_videos
from workers.collector import collect_events


class Request:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def execute(self):
        if self.error:
            raise self.error
        return self.result


class FakeYouTube:
    def __init__(self, search=None, videos=None, comments=None, search_error=None):
        self._search = search
        self._videos = videos
        self._comments = comments
        self._search_error = search_error

    def search(self):
        return SimpleNamespace(list=lambda **kwargs: Request(self._search, self._search_error))

    def videos(self):
        return SimpleNamespace(list=lambda **kwargs: Request(self._videos))

    def commentThreads(self):
        return SimpleNamespace(list=lambda **kwargs: Request(self._comments))


def comment_payload(text="great video, really enjoyed the explanation"):
    return {"items": [{"snippet": {"topLevelComment": {"snippet": {
        "textDisplay": text, "authorDisplayName": "Asha", "likeCount": 3, "publishedAt": "2026-10-01T00:00:00Z"}}}}]}


def good_youtube():
    return FakeYouTube(
        search={"items": [{"id": {"videoId": "v1"}}, {"id": {"kind": "channel"}}]},
        videos={"items": [{"id": "v1", "snippet": {"title": "A video", "channelTitle": "Chan"}, "statistics": {"viewCount": "10"}}]},
        comments=comment_payload(),
    )


def test_youtube_paths():
    videos = search_youtube_videos(good_youtube(), "phone")
    assert videos == [{"video_id": "v1", "title": "A video", "channel": "Chan", "views": "10"}]
    assert fetch_youtube_comments(good_youtube(), "v1")[0]["author"] == "Asha"
    assert search_youtube_videos(FakeYouTube(search={"items": []}), "x") == []
    for message, kind in (("quotaExceeded", "quota"), ("403 forbidden", "forbidden"), ("socket closed", "unreachable")):
        try:
            search_youtube_videos(FakeYouTube(search_error=Exception(message)), "x")
            raise AssertionError("expected SourceError")
        except SourceError as error:
            assert error.kind == kind
    try:
        search_youtube_videos(None, "x")
        raise AssertionError("expected SourceError")
    except SourceError as error:
        assert error.kind == "unavailable"
    assert fetch_youtube_comments(None, "v1") == []
    broken = FakeYouTube(comments=None)
    assert fetch_youtube_comments(broken, "v1") == []
    print("youtube paths ok")


class FakeHttp:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    def get(self, url, params=None, timeout=None):
        if self.error:
            raise self.error
        return self.response


def news_response(status=200, articles=None):
    payload = {"articles": articles if articles is not None else []}
    return SimpleNamespace(status_code=status, json=lambda: payload)


def test_news_paths():
    article = {"title": "Phone launch", "description": "A new phone arrives.", "url": "http://x/1", "source": {"name": "Daily"}}
    short = {"title": "Hi", "description": "", "url": "http://x/2", "source": {"name": "Daily"}}
    articles, warning = fetch_news("key", "phone", http=FakeHttp(news_response(200, [article, short])))
    assert warning is None and len(articles) == 1 and articles[0]["raw"] == article
    assert fetch_news(None, "phone")[1].startswith("News search is disabled")
    assert "rejected" in fetch_news("k", "p", http=FakeHttp(news_response(401)))[1]
    assert "rate limit" in fetch_news("k", "p", http=FakeHttp(news_response(429)))[1]
    assert "status 500" in fetch_news("k", "p", http=FakeHttp(news_response(500)))[1]
    assert "timed out" in fetch_news("k", "p", http=FakeHttp(error=requests.Timeout()))[1]
    assert "could not be reached" in fetch_news("k", "p", http=FakeHttp(error=requests.ConnectionError()))[1]
    assert fetch_news("k", "p", http=FakeHttp(news_response(200, [])))[0] == []
    print("news paths ok")


def test_telegram_paths():
    assert search_telegram_channels(None, "x")[0] == []
    assert "not connected" in search_telegram_channels(None, "x")[1]
    messages, error = fetch_telegram_messages(None, "chan")
    assert messages == [] and error

    class Client:
        def get_entity(self, username):
            if username == "missing":
                raise ValueError("No user has this username")
            return username

        def iter_messages(self, entity, limit=0):
            return [SimpleNamespace(text="hello channel", date=None, sender_id=7, views=5), SimpleNamespace(text="", date=None, sender_id=8, views=1)]

    messages, error = fetch_telegram_messages(Client(), "chan")
    assert error is None and len(messages) == 1 and messages[0]["sender_id"] == 7
    messages, error = fetch_telegram_messages(Client(), "missing")
    assert messages == [] and "Couldn't read @missing" in error
    print("telegram paths ok")


def test_collect_events_with_fakes():
    class Fakes:
        SourceError = SourceError

        @staticmethod
        def search_youtube_videos(client, keyword, limit):
            return [{"video_id": "v1", "title": "T", "channel": "C", "views": "1"}]

        @staticmethod
        def fetch_youtube_comments(client, video_id, limit):
            return [{"text": "a long enough comment about this", "author": "A", "likes": 1, "published_at": "2026-10-01T00:00:00Z"}]

        @staticmethod
        def fetch_news(key, keyword, limit):
            return [{"raw": {"title": "Headline", "description": "Body of the story", "url": "http://n/1", "source": {"name": "Daily"}, "publishedAt": "2026-10-01T00:00:00Z"}}], None

        @staticmethod
        def search_telegram_channels(client, keyword, limit):
            return [{"id": 1, "title": "Chan", "username": "chan"}], None

        @staticmethod
        def fetch_telegram_messages(client, username, limit):
            return [{"text": "telegram message here", "date": None, "sender_id": 5, "views": 2}], "partial read problem"

    result = collect_events("phone", ("youtube", "news", "telegram"), source_functions=Fakes)
    assert result["counts"] == {"youtube": 1, "news": 1, "telegram": 1}
    assert len(result["events"]) == 3 and {e["platform"] for e in result["events"]} == {"youtube", "news", "telegram"}
    assert all(e["keyword"] == "phone" for e in result["events"])
    assert result["warnings"] == ["telegram: partial read problem"]

    only_news = collect_events("phone", ("news",), source_functions=Fakes)
    assert only_news["counts"]["youtube"] == 0 and len(only_news["events"]) == 1

    class Failing(Fakes):
        @staticmethod
        def search_youtube_videos(client, keyword, limit):
            raise SourceError("quota used up", "quota")

        @staticmethod
        def fetch_news(key, keyword, limit):
            return [], "News search was rejected."

    failed = collect_events("phone", ("youtube", "news"), source_functions=Failing)
    assert failed["events"] == [] and len(failed["warnings"]) == 2
    print("collect events ok")


if __name__ == "__main__":
    test_youtube_paths()
    test_news_paths()
    test_telegram_paths()
    test_collect_events_with_fakes()
    print("all ingestion tests passed")
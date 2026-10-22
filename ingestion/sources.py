import uuid

import requests

NEWS_URL = "https://newsapi.org/v2/everything"
MIN_NEWS_TEXT_LENGTH = 15


class SourceError(Exception):
    def __init__(self, message, kind="unavailable"):
        super().__init__(message)
        self.kind = kind


def search_youtube_videos(client, keyword, max_results=12):
    if client is None:
        raise SourceError("YouTube is not available. Check YOUTUBE_API_KEY in your .env file.", "unavailable")
    try:
        response = client.search().list(
            part="snippet", q=keyword, type="video",
            order="relevance", maxResults=max_results, regionCode="IN"
        ).execute()
    except Exception as error:
        text = str(error)
        lowered = text.lower()
        if "quotaexceeded" in lowered or "quota" in lowered:
            raise SourceError("YouTube API daily quota has been used up. Try again tomorrow, or use a different API key.", "quota")
        if "blocked" in lowered or "forbidden" in lowered:
            raise SourceError("YouTube API key isn't authorized for this request. Check API key restrictions in Google Cloud Console.", "forbidden")
        raise SourceError("Couldn't reach YouTube right now. Check your internet connection and try again.", "unreachable")

    video_ids = [item["id"]["videoId"] for item in response.get("items", []) if "videoId" in item.get("id", {})]
    if not video_ids:
        return []

    try:
        stats = client.videos().list(part="snippet,statistics", id=",".join(video_ids)).execute()
    except Exception:
        return []

    videos = []
    for item in stats.get("items", []):
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "views": item.get("statistics", {}).get("viewCount"),
        })
    return videos


def fetch_youtube_comments(client, video_id, max_results=40):
    comments = []
    if client is None:
        return comments
    try:
        response = client.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=max_results, order="relevance"
        ).execute()
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "text": snippet["textDisplay"],
                "author": snippet["authorDisplayName"],
                "likes": snippet["likeCount"],
                "published_at": snippet.get("publishedAt"),
            })
    except Exception:
        pass
    return comments


def fetch_news(api_key, keyword, max_results=15, http=None):
    if not api_key:
        return [], "News search is disabled because NEWS_API_KEY is not set."
    http = http or requests
    params = {
        "q": keyword,
        "apiKey": api_key,
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": max_results,
    }
    try:
        response = http.get(NEWS_URL, params=params, timeout=10)
        if response.status_code in (401, 403):
            return [], "News search was rejected. Check NEWS_API_KEY."
        if response.status_code == 429:
            return [], "The News API rate limit was reached. News results are skipped for now."
        if response.status_code != 200:
            return [], f"News search failed with status {response.status_code}."
        data = response.json()
    except requests.Timeout:
        return [], "News search timed out. News results are skipped for now."
    except Exception:
        return [], "News search could not be reached. News results are skipped for now."

    articles = []
    for item in data.get("articles", []):
        title = item.get("title") or ""
        description = item.get("description") or ""
        text = f"{title}. {description}".strip()
        source = (item.get("source") or {}).get("name") or "Unknown source"
        if len(text) < MIN_NEWS_TEXT_LENGTH:
            continue
        articles.append({
            "video_id": item.get("url") or f"news-{uuid.uuid4()}",
            "title": title,
            "channel": source,
            "views": None,
            "raw": item,
            "comments": [{"text": text, "author": source, "likes": 0}],
        })
    return articles, None


def search_telegram_channels(client, keyword, max_results=5):
    if client is None:
        return [], "Telegram is not connected. Check TELEGRAM_API_ID, TELEGRAM_API_HASH and your Telegram session."
    try:
        from telethon.tl.functions.contacts import SearchRequest

        result = client(SearchRequest(q=keyword, limit=max_results))
        channels = []
        for chat in result.chats:
            username = getattr(chat, "username", None)
            if username:
                channels.append({"id": chat.id, "title": getattr(chat, "title", ""), "username": username})
        return channels, None
    except Exception as error:
        return [], str(error)


def fetch_telegram_messages(client, username, limit=50):
    if client is None:
        return [], "Telegram is not connected."
    messages = []
    try:
        entity = client.get_entity(username)
        for message in client.iter_messages(entity, limit=limit):
            if message.text:
                messages.append({
                    "text": message.text,
                    "date": message.date,
                    "sender_id": message.sender_id,
                    "views": getattr(message, "views", None),
                })
    except Exception as error:
        return messages, f"Couldn't read @{username}: {error}"
    return messages, None

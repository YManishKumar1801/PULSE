from models.schemas import dedupe_events, make_event, utc_now_iso


def normalize_youtube_video(video, keyword=None):
    video_id = video.get("video_id")
    channel = video.get("channel")
    collected_at = utc_now_iso()
    events = []
    for comment in video.get("comments", []) or []:
        event = make_event(
            platform="youtube",
            source_type="comment",
            source_id=video_id,
            source_name=channel,
            text=comment.get("text"),
            author_id=comment.get("author"),
            author_name=comment.get("author"),
            timestamp=comment.get("published_at"),
            likes=comment.get("likes", 0),
            source_url=f"https://www.youtube.com/watch?v={video_id}" if video_id else None,
            keyword=keyword or video.get("keyword"),
            collected_at=collected_at,
        )
        if event:
            events.append(event)
    return events


def normalize_news_article(article, keyword=None):
    title = (article.get("title") or "").strip()
    description = (article.get("description") or "").strip()
    text = f"{title}. {description}" if title and description else title or description
    source = article.get("source") or {}
    source_name = source.get("name") if isinstance(source, dict) else str(source)
    url = article.get("url")
    author = article.get("author") or source_name
    event = make_event(
        platform="news",
        source_type="article",
        source_id=url or title,
        source_name=source_name,
        text=text,
        author_id=source_name,
        author_name=author,
        timestamp=article.get("publishedAt"),
        source_url=url,
        keyword=keyword,
    )
    return [event] if event else []


def normalize_telegram_channel(channel, keyword=None):
    channel_name = channel.get("channel_title") or channel.get("title") or channel.get("channel")
    channel_username = channel.get("channel_username") or channel.get("username")
    source_id = channel_username or channel_name
    collected_at = utc_now_iso()
    url = f"https://t.me/{channel_username}" if channel_username else None
    events = []
    for message in channel.get("messages", []) or []:
        event = make_event(
            platform="telegram",
            source_type="message",
            source_id=source_id,
            source_name=channel_name,
            text=message.get("text"),
            author_id=message.get("sender_id"),
            author_name=None,
            timestamp=message.get("date"),
            views=message.get("views", 0),
            source_url=url,
            keyword=keyword or channel.get("keyword"),
            collected_at=collected_at,
        )
        if event:
            events.append(event)
    return events


NORMALIZERS = {
    "youtube": normalize_youtube_video,
    "news": normalize_news_article,
    "telegram": normalize_telegram_channel,
}


def normalize(platform, raw_items, keyword=None):
    normalizer = NORMALIZERS.get(platform)
    if normalizer is None:
        raise ValueError(f"unsupported platform: {platform}")
    events = []
    for item in raw_items:
        events.extend(normalizer(item, keyword))
    return dedupe_events(events)
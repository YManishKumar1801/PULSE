import argparse
import os
import signal
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from ingestion import sources
from ingestion.normalizer import normalize
from models.schemas import dedupe_events

load_dotenv()

ALL_SOURCES = ("youtube", "news", "telegram")


def collect_events(
    keyword,
    selected,
    youtube_client=None,
    news_api_key=None,
    telegram_client=None,
    max_videos=12,
    comments_per_video=40,
    news_limit=15,
    channel_limit=5,
    messages_per_channel=50,
    source_functions=sources,
):
    events = []
    warnings = []
    counts = {name: 0 for name in ALL_SOURCES}

    if "youtube" in selected:
        try:
            videos = source_functions.search_youtube_videos(youtube_client, keyword, max_videos)
        except source_functions.SourceError as error:
            videos = []
            warnings.append(f"youtube: {error}")
        for video in videos:
            comments = source_functions.fetch_youtube_comments(youtube_client, video["video_id"], comments_per_video)
            found = normalize("youtube", [{**video, "comments": comments}], keyword)
            counts["youtube"] += len(found)
            events.extend(found)

    if "news" in selected:
        articles, warning = source_functions.fetch_news(news_api_key, keyword, news_limit)
        if warning:
            warnings.append(f"news: {warning}")
        for article in articles:
            if article.get("raw"):
                found = normalize("news", [article["raw"]], keyword)
                counts["news"] += len(found)
                events.extend(found)

    if "telegram" in selected:
        channels, error = source_functions.search_telegram_channels(telegram_client, keyword, channel_limit)
        if error:
            warnings.append(f"telegram: {error}")
        for channel in channels:
            messages, read_error = source_functions.fetch_telegram_messages(telegram_client, channel["username"], messages_per_channel)
            if read_error:
                warnings.append(f"telegram: {read_error}")
            found = normalize(
                "telegram",
                [{"channel_title": channel["title"], "channel_username": channel["username"], "messages": messages}],
                keyword,
            )
            counts["telegram"] += len(found)
            events.extend(found)

    return {"events": dedupe_events(events), "warnings": warnings, "counts": counts}


def build_clients(selected):
    youtube_client = None
    telegram_client = None
    notes = []
    if "youtube" in selected:
        key = os.getenv("YOUTUBE_API_KEY")
        if key:
            from googleapiclient.discovery import build

            youtube_client = build("youtube", "v3", developerKey=key)
        else:
            notes.append("youtube: YOUTUBE_API_KEY is not set")
    if "telegram" in selected:
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        if api_id and api_hash and api_id.strip().isdigit():
            from telethon.sync import TelegramClient

            client = TelegramClient("pulse_session", int(api_id), api_hash)
            client.connect()
            if client.is_user_authorized():
                telegram_client = client
            else:
                client.disconnect()
                notes.append("telegram: the session is not logged in, see the Telegram session step in the README")
        else:
            notes.append("telegram: TELEGRAM_API_ID or TELEGRAM_API_HASH is not set")
    return youtube_client, telegram_client, notes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("keywords", nargs="+")
    parser.add_argument("--sources", nargs="+", default=list(ALL_SOURCES), choices=ALL_SOURCES)
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--interval", type=int, default=0, help="repeat every N seconds; 0 runs once")
    parser.add_argument("--dry-run", action="store_true", help="collect and print counts without publishing")
    args = parser.parse_args()

    from workers.publish_events import publish

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    selected = tuple(args.sources)
    youtube_client, telegram_client, notes = build_clients(selected)
    for note in notes:
        print("Skipping:", note)

    while not stop.is_set():
        for keyword in args.keywords:
            result = collect_events(
                keyword,
                selected,
                youtube_client=youtube_client,
                news_api_key=os.getenv("NEWS_API_KEY"),
                telegram_client=telegram_client,
            )
            for warning in result["warnings"]:
                print("Warning:", warning)
            line = f"'{keyword}': collected {len(result['events'])} events {result['counts']}"
            if args.dry_run or not result["events"]:
                print(line, "(not published)")
                continue
            sent, skipped = publish(result["events"], args.broker)
            print(line, f"-> published {sent}, skipped {skipped}")
        if args.interval <= 0:
            break
        stop.wait(args.interval)

    if telegram_client is not None:
        telegram_client.disconnect()


if __name__ == "__main__":
    main()
from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_TOPIC = "pulse-youtube"
KAFKA_BROKER = "localhost:9092"

youtube = build("youtube", "v3", developerKey=API_KEY)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def get_trending_videos(region="IN", max_results=10):
    request = youtube.videos().list(
        part="snippet,statistics",
        chart="mostPopular",
        regionCode=region,
        maxResults=max_results
    )
    response = request.execute()

    videos = []
    for item in response["items"]:
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "views": item["statistics"].get("viewCount"),
            "likes": item["statistics"].get("likeCount")
        })
    return videos


def search_videos_by_keyword(keyword, max_results=10):
    request = youtube.search().list(
        part="snippet",
        q=keyword,
        type="video",
        order="relevance",
        maxResults=max_results,
        regionCode="IN"
    )
    response = request.execute()

    video_ids = [item["id"]["videoId"] for item in response["items"] if "videoId" in item["id"]]

    if not video_ids:
        return []

    stats_request = youtube.videos().list(
        part="snippet,statistics",
        id=",".join(video_ids)
    )
    stats_response = stats_request.execute()

    videos = []
    for item in stats_response["items"]:
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "views": item["statistics"].get("viewCount"),
            "likes": item["statistics"].get("likeCount")
        })
    return videos


def get_video_comments(video_id, max_results=20):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            order="relevance"
        )
        response = request.execute()

        for item in response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "text": comment["textDisplay"],
                "author": comment["authorDisplayName"],
                "likes": comment["likeCount"],
                "published_at": comment["publishedAt"]
            })
    except Exception as e:
        print(f"Comments disabled or error for video {video_id}: {e}")

    return comments


def send_to_kafka(keyword, video):
    message = {
        "keyword": keyword,
        "video_id": video["video_id"],
        "title": video["title"],
        "channel": video["channel"],
        "views": video["views"],
        "likes": video["likes"],
        "comments": video["comments"]
    }
    producer.send(KAFKA_TOPIC, value=message)
    print(f"  -> Sent to Kafka topic '{KAFKA_TOPIC}': {video['title'][:50]}")


if __name__ == "__main__":
    keyword = input("Enter a keyword/topic to search (or press Enter for trending): ").strip()

    if keyword:
        print(f"\nSearching videos for: '{keyword}'...\n")
        videos = search_videos_by_keyword(keyword)
        if not videos:
            print("No videos found for this keyword. Try a different or more popular topic.")
            videos = []
        used_keyword = keyword
    else:
        print("\nNo keyword entered. Fetching trending videos (India)...\n")
        videos = get_trending_videos()
        used_keyword = "trending"

    all_data = []

    for video in videos:
        print(f"- {video['title']} ({video['channel']}) | Views: {video['views']}")
        comments = get_video_comments(video["video_id"])
        video["comments"] = comments
        all_data.append(video)
        send_to_kafka(used_keyword, video)

    producer.flush()

    filename = "youtube_data.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(all_data)} videos with comments to {filename}")
    print(f"All videos also sent to Kafka topic '{KAFKA_TOPIC}'")
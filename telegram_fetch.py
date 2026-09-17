from telethon.sync import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

client = TelegramClient("pulse_session", API_ID, API_HASH)


def search_channels_by_keyword(keyword, max_results=5):
    result = client(SearchRequest(q=keyword, limit=max_results))
    channels = []
    for chat in result.chats:
        channels.append({
            "id": chat.id,
            "title": getattr(chat, "title", ""),
            "username": getattr(chat, "username", None)
        })
    return channels


def fetch_messages_from_channel(username, limit=50):
    messages = []
    try:
        entity = client.get_entity(username)
        for message in client.iter_messages(entity, limit=limit):
            if message.text:
                messages.append({
                    "text": message.text,
                    "date": str(message.date),
                    "sender_id": message.sender_id,
                    "views": getattr(message, "views", None)
                })
    except Exception as e:
        print(f"Could not fetch messages from {username}: {e}")
    return messages


if __name__ == "__main__":
    client.start()

    keyword = input("Enter a keyword/topic to search Telegram channels: ").strip()

    print(f"\nSearching Telegram channels for: '{keyword}'...\n")
    channels = search_channels_by_keyword(keyword)

    if not channels:
        print("No channels found for this keyword.")
    else:
        all_data = []
        for ch in channels:
            if not ch["username"]:
                continue
            print(f"- {ch['title']} (@{ch['username']})")
            messages = fetch_messages_from_channel(ch["username"])
            print(f"  -> Fetched {len(messages)} messages")
            all_data.append({
                "channel_title": ch["title"],
                "channel_username": ch["username"],
                "keyword": keyword,
                "messages": messages
            })

        filename = "telegram_data.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

        print(f"\nSaved data from {len(all_data)} channels to {filename}")

    client.disconnect()
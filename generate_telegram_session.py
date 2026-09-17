from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    session_string = client.session.save()
    print("\nYour session string (copy everything between the lines):\n")
    print("-" * 60)
    print(session_string)
    print("-" * 60)
    print("\nPaste this into your .env file as TELEGRAM_SESSION_STRING=<the string above>")
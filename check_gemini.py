import os
import requests
from dotenv import load_dotenv

load_dotenv()

MODEL = "gemini-3.5-flash"
BASE = "https://generativelanguage.googleapis.com/v1beta"

key = os.getenv("GEMINI_API_KEY")
print("GEMINI_API_KEY set:", bool(key))
if not key:
    raise SystemExit("Add GEMINI_API_KEY to your .env file and run again")

headers = {"x-goog-api-key": key, "Content-Type": "application/json"}

response = requests.post(
    f"{BASE}/models/{MODEL}:generateContent",
    headers=headers,
    json={"contents": [{"parts": [{"text": "Reply with the single word: ok"}]}]},
    timeout=30,
)
print("generateContent status:", response.status_code)
print(response.text[:600])

if response.status_code != 200:
    listing = requests.get(f"{BASE}/models", headers=headers, timeout=30)
    print("list models status:", listing.status_code)
    if listing.status_code == 200:
        for model in listing.json().get("models", []):
            if "generateContent" in model.get("supportedGenerationMethods", []):
                print(model["name"])
    else:
        print(listing.text[:600])
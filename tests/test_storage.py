import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.normalizer import normalize
from storage.mongodb import count_events, fetch_events, get_events_collection, save_events

TEST_KEYWORD = "__pulse_storage_test__"


def run():
    videos = [
        {
            "video_id": "vid_test_1",
            "title": "Storage test",
            "channel": "TestChannel",
            "comments": [
                {"text": "first test comment", "author": "tester1", "likes": 3, "published_at": "2026-10-01T10:00:00Z"},
                {"text": "second test comment", "author": "tester2", "likes": 1, "published_at": "2026-10-01T10:05:00Z"},
            ],
        }
    ]
    events = normalize("youtube", videos, TEST_KEYWORD)
    assert len(events) == 2

    first = save_events(events)
    print("first save:", first)
    assert first["inserted"] == 2

    second = save_events(events)
    print("second save:", second)
    assert second["inserted"] == 0

    assert count_events(keyword=TEST_KEYWORD) == 2

    fetched = fetch_events(keyword=TEST_KEYWORD)
    assert len(fetched) == 2
    assert fetched[0]["timestamp"] <= fetched[1]["timestamp"]
    assert TEST_KEYWORD in fetched[0]["keywords"]

    get_events_collection().delete_many({"keywords": TEST_KEYWORD})
    assert count_events(keyword=TEST_KEYWORD) == 0
    print("all storage tests passed")


if __name__ == "__main__":
    run()
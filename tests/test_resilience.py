import os
import sys
from types import SimpleNamespace

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.env import describe_missing, get_env, get_int_env, missing_env
from intelligence.gemini_client import classify_status, generate_text
from pymongo.errors import AutoReconnect, BulkWriteError, ServerSelectionTimeoutError
from storage import mongodb
from storage.mongodb import StorageUnavailable, bulk_write_safely, with_retry


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeHttp:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(url)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def good(text="A clear explanation."):
    return FakeResponse(200, {"candidates": [{"content": {"parts": [{"text": text}]}}]})


def call(outcomes, **kwargs):
    http = FakeHttp(outcomes)
    slept = []
    result = generate_text("prompt", "key", models=["m1", "m2"], http=http, sleep=slept.append, **kwargs)
    return result, http, slept


def test_gemini_success_and_no_key():
    result, http, _ = call([good()])
    assert result["text"] == "A clear explanation." and result["error"] is None and result["model"] == "m1"
    no_key = generate_text("prompt", None, http=FakeHttp([]))
    assert no_key["error"] == "no_key" and no_key["text"] is None
    print("gemini success and no key ok")


def test_gemini_invalid_key_stops_immediately():
    for response in (FakeResponse(403, text="forbidden"), FakeResponse(400, text="API key not valid. Please pass a valid API key.")):
        result, http, _ = call([response])
        assert result["error"] == "invalid_key" and len(http.calls) == 1
    print("gemini invalid key ok")


def test_gemini_quota_retries_then_falls_back():
    result, http, slept = call([FakeResponse(429), FakeResponse(429), good("from second model")])
    assert result["text"] == "from second model" and result["model"] == "m2"
    assert len(http.calls) == 3 and len(slept) == 2
    result, _, _ = call([FakeResponse(429)] * 4)
    assert result["text"] is None and result["error"] == "quota"
    print("gemini quota ok")


def test_gemini_timeout_network_and_unavailable():
    result, _, _ = call([requests.Timeout()] * 4)
    assert result["error"] == "timeout" and result["text"] is None
    result, _, _ = call([requests.ConnectionError()] * 4)
    assert result["error"] == "network"
    result, _, _ = call([FakeResponse(503), good("recovered")])
    assert result["text"] == "recovered" and result["model"] == "m1"
    result, _, _ = call([FakeResponse(404), good("other model")])
    assert result["text"] == "other model" and result["model"] == "m2"
    print("gemini timeout network unavailable ok")


def test_gemini_empty_results():
    empty_candidates = FakeResponse(200, {"candidates": []})
    blocked = FakeResponse(200, {"promptFeedback": {"blockReason": "SAFETY"}})
    bad_json = FakeResponse(200, None)
    result, _, _ = call([empty_candidates, blocked])
    assert result["error"] == "empty" and result["text"] is None
    result, _, _ = call([bad_json, good("second")])
    assert result["text"] == "second"
    assert classify_status(500) == "unavailable" and classify_status(429) == "quota"
    print("gemini empty ok")


def test_gemini_deadline():
    ticks = iter([0, 0, 100, 200, 300])
    http = FakeHttp([FakeResponse(503)] * 8)
    result = generate_text("p", "k", models=["m1", "m2"], http=http, sleep=lambda s: None, clock=lambda: next(ticks), max_total_seconds=45)
    assert result["error"] == "deadline" and len(http.calls) == 1
    print("gemini deadline ok")


def test_env_helpers():
    os.environ["PULSE_TEST_A"] = "  value  "
    os.environ["PULSE_TEST_B"] = "12"
    os.environ["PULSE_TEST_C"] = "abc"
    os.environ["PULSE_TEST_D"] = "   "
    assert get_env("PULSE_TEST_A") == "value"
    assert get_env("PULSE_TEST_D", "fallback") == "fallback"
    assert get_int_env("PULSE_TEST_B") == 12
    assert get_int_env("PULSE_TEST_C") is None and get_int_env("PULSE_TEST_MISSING", 5) == 5
    assert missing_env(["PULSE_TEST_A", "PULSE_TEST_MISSING"]) == ["PULSE_TEST_MISSING"]
    assert describe_missing("Feature", ["PULSE_TEST_A"]) is None
    assert "PULSE_TEST_MISSING" in describe_missing("Feature", ["PULSE_TEST_MISSING"])
    print("env helpers ok")


def test_retry_behaviour():
    attempts = []
    sleeps = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise AutoReconnect("blip")
        return "ok"

    assert with_retry(flaky, attempts=3, base_delay=1.0, sleep=sleeps.append) == "ok"
    assert len(attempts) == 3 and sleeps == [1.0, 2.0]

    def always_down():
        raise ServerSelectionTimeoutError("no primary")

    try:
        with_retry(always_down, attempts=2, base_delay=0, sleep=lambda s: None)
        raise AssertionError("expected StorageUnavailable")
    except StorageUnavailable as error:
        assert "MongoDB is unreachable" in str(error)

    def logic_error():
        raise ValueError("bug")

    try:
        with_retry(logic_error, attempts=3, sleep=lambda s: None)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    print("retry behaviour ok")


class FakeCollection:
    def __init__(self, behaviours):
        self.behaviours = list(behaviours)
        self.calls = 0

    def bulk_write(self, operations, ordered=False):
        self.calls += 1
        behaviour = self.behaviours.pop(0)
        if isinstance(behaviour, Exception):
            raise behaviour
        return behaviour


def test_bulk_write_duplicates_and_transient_failures():
    original_sleep = mongodb.time.sleep
    mongodb.time.sleep = lambda seconds: None
    try:
        ok = SimpleNamespace(upserted_count=2, modified_count=1)
        collection = FakeCollection([AutoReconnect("blip"), ok])
        assert bulk_write_safely(collection, ["op"]) == {"inserted": 2, "modified": 1, "duplicates": 0}
        assert collection.calls == 2

        duplicate = BulkWriteError({"writeErrors": [{"code": 11000}, {"code": 11000}], "nUpserted": 3, "nModified": 0})
        assert bulk_write_safely(FakeCollection([duplicate]), ["op"]) == {"inserted": 3, "modified": 0, "duplicates": 2}

        other = BulkWriteError({"writeErrors": [{"code": 11000}, {"code": 121}], "nUpserted": 0, "nModified": 0})
        try:
            bulk_write_safely(FakeCollection([other]), ["op"])
            raise AssertionError("expected BulkWriteError")
        except BulkWriteError:
            pass

        try:
            bulk_write_safely(FakeCollection([AutoReconnect("x")] * 3), ["op"])
            raise AssertionError("expected StorageUnavailable")
        except StorageUnavailable:
            pass
        assert bulk_write_safely(FakeCollection([]), []) == {"inserted": 0, "modified": 0, "duplicates": 0}
    finally:
        mongodb.time.sleep = original_sleep
    print("bulk write resilience ok")


def test_save_events_idempotent_and_malformed():
    from models.schemas import make_event

    captured = []

    class Collection:
        def bulk_write(self, operations, ordered=False):
            captured.append(operations)
            return SimpleNamespace(upserted_count=len(operations), modified_count=0)

    original = mongodb.get_events_collection
    mongodb.get_events_collection = lambda: Collection()
    try:
        event = make_event("telegram", "message", "chan", "same message text", author_id="a1", timestamp="2026-10-01T00:00:00+00:00", keyword="ipl")
        again = make_event("telegram", "message", "chan", "same message text", author_id="a1", timestamp="2026-10-01T00:00:00+00:00", keyword="ipl")
        assert event["event_id"] == again["event_id"]
        summary = mongodb.save_events([event, again, {"text": "malformed"}, {}])
        assert summary["received"] == 4 and summary["valid"] == 2
        ids = [op._filter["event_id"] for op in captured[0]]
        assert ids[0] == ids[1]
        assert mongodb.save_events([{"bad": 1}])["valid"] == 0
    finally:
        mongodb.get_events_collection = original
    print("save events idempotent and malformed ok")


if __name__ == "__main__":
    test_gemini_success_and_no_key()
    test_gemini_invalid_key_stops_immediately()
    test_gemini_quota_retries_then_falls_back()
    test_gemini_timeout_network_and_unavailable()
    test_gemini_empty_results()
    test_gemini_deadline()
    test_env_helpers()
    test_retry_behaviour()
    test_bulk_write_duplicates_and_transient_failures()
    test_save_events_idempotent_and_malformed()
    print("all resilience tests passed")
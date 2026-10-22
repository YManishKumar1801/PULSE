import json
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.schemas import make_event
from storage.mongodb import StorageUnavailable
from workers.processor import DEAD_LETTER_TOPIC, EventWorker, compute_lag, dead_letter_payload, decode_message
from workers.publish_events import stored_to_message, synthetic_events


def make_message(text="a perfectly normal message about the phone", index=0):
    event = make_event("telegram", "message", "chan", text, author_id=f"a{index}", timestamp="2026-10-01T00:00:00+00:00", keyword="phone")
    return event, json.dumps(event).encode("utf-8")


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0
        self.seeks = []
        self.position_value = 0
        self.end_value = 10

    def poll(self, timeout_ms=0, max_records=0):
        if not self.batches:
            return {}
        values = self.batches.pop(0)
        records = [SimpleNamespace(value=value, offset=index) for index, value in enumerate(values)]
        return {("pulse.events", 0): records}

    def commit(self):
        self.commits += 1

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))
        self.batches.insert(0, self._last)

    def assignment(self):
        return [("pulse.events", 0)]

    def end_offsets(self, partitions):
        return {partition: self.end_value for partition in partitions}

    def position(self, partition):
        return self.position_value


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value=None, key=None):
        self.sent.append((topic, value))


class Harness:
    def __init__(self, batches, analyze_error=None, storage_failures=0, analyzed_ids=()):
        self.consumer = FakeConsumer(batches)
        self.producer = FakeProducer()
        self.saved = []
        self.analyses = []
        self.statuses = []
        self.storage_failures = storage_failures
        self.analyze_error = analyze_error
        self.analyzed_ids = set(analyzed_ids)
        self.sleeps = []
        self.analyze_calls = 0

        def analyze(events):
            self.analyze_calls += 1
            if self.analyze_error:
                raise self.analyze_error
            return [(event["event_id"], {"sentiment": {"label": "Neutral"}}) for event in events]

        def save_events(events):
            if self.storage_failures > 0:
                self.storage_failures -= 1
                raise StorageUnavailable("down")
            self.saved.extend(events)
            return {"valid": len(events)}

        self.worker = EventWorker(
            consumer=self.consumer,
            dead_letter_producer=self.producer,
            analyze=analyze,
            save_events=save_events,
            save_analyses=lambda items: self.analyses.extend(items) or len(items),
            fetch_analyzed_ids=lambda ids: {i for i in ids if i in self.analyzed_ids},
            report_status=lambda worker_id, status: self.statuses.append((worker_id, status)),
            sleep=self.sleeps.append,
            status_interval_seconds=0,
        )
        original_poll = self.consumer.poll

        def tracking_poll(timeout_ms=0, max_records=0):
            result = original_poll(timeout_ms, max_records)
            if result:
                self.consumer._last = [record.value for records in result.values() for record in records]
            return result

        self.consumer.poll = tracking_poll


def test_decode_and_dead_letter_payload():
    event, raw = make_message()
    decoded, reason = decode_message(raw)
    assert reason is None and decoded["event_id"] == event["event_id"]
    assert decode_message(b"\xff\xfe not utf8")[1] == "not_json"
    assert decode_message(b"{broken json")[1] == "not_json"
    assert decode_message(b"[1, 2, 3]")[1] == "not_an_object"
    assert decode_message(json.dumps({"text": "missing fields"}).encode())[1] == "invalid_event"
    payload = dead_letter_payload(b"x" * 5000, "not_json")
    assert len(payload["raw"]) == 2000 and payload["reason"] == "not_json"
    print("decode and dead letter payload ok")


def test_happy_path_commits_and_analyzes():
    _, first = make_message(index=1)
    _, second = make_message("second distinct message about the battery", index=2)
    harness = Harness([[first, second]])
    harness.worker.run(max_batches=1)
    assert len(harness.saved) == 2 and len(harness.analyses) == 2
    assert harness.consumer.commits == 1
    snapshot = harness.worker.snapshot()
    assert snapshot["stored"] == 2 and snapshot["analyzed"] == 2 and snapshot["batches"] == 1
    assert snapshot["consumer_lag"] == 10
    harness.consumer.position_value = 10
    assert compute_lag(harness.consumer) == 0
    assert harness.statuses and harness.statuses[-1][0] == "worker-1"
    print("happy path ok")


def test_malformed_and_duplicate_messages():
    _, good = make_message(index=1)
    harness = Harness([[good, good, b"not json at all", json.dumps({"bad": 1}).encode()]])
    harness.worker.run(max_batches=1)
    assert len(harness.saved) == 1
    counters = harness.worker.counters
    assert counters["duplicates_in_batch"] == 1 and counters["malformed"] == 2
    topics = [topic for topic, _ in harness.producer.sent]
    assert topics == [DEAD_LETTER_TOPIC, DEAD_LETTER_TOPIC]
    assert harness.consumer.commits == 1
    print("malformed and duplicate ok")


def test_already_analyzed_events_are_skipped():
    event, raw = make_message(index=1)
    harness = Harness([[raw]], analyzed_ids={event["event_id"]})
    harness.worker.run(max_batches=1)
    assert harness.analyze_calls == 0 and harness.worker.counters["skipped_already_analyzed"] == 1
    assert harness.consumer.commits == 1
    print("already analyzed skipped ok")


def test_analysis_failure_does_not_block_progress():
    _, raw = make_message(index=1)
    harness = Harness([[raw]], analyze_error=RuntimeError("model crashed"))
    harness.worker.run(max_batches=1)
    assert len(harness.saved) == 1 and harness.analyses == []
    assert harness.worker.counters["analysis_failures"] == 1
    assert harness.consumer.commits == 1
    print("analysis failure ok")


def test_storage_outage_rewinds_and_retries_without_commit():
    _, raw = make_message(index=1)
    harness = Harness([[raw]], storage_failures=1)
    harness.worker.run_once()
    assert harness.consumer.commits == 0 and harness.consumer.seeks
    assert harness.sleeps == [5]
    assert harness.worker.counters["storage_retries"] == 1 and harness.worker.counters["messages"] == 0
    harness.worker.run_once()
    assert harness.consumer.commits == 1 and len(harness.saved) == 1 and len(harness.analyses) == 1
    assert harness.worker.counters["messages"] == 1
    print("storage outage retry ok")


def test_idle_poll_and_stop():
    harness = Harness([])
    assert harness.worker.run_once() == 0
    stops = iter([False, False, True])
    harness.worker.run(should_stop=lambda: next(stops))
    assert harness.consumer.commits == 0
    print("idle and stop ok")


def test_publisher_helpers():
    events = synthetic_events(25)
    assert len(events) == 25
    assert len({event["event_id"] for event in events}) == 25
    stored = {"event_id": "x", "text": "t", "keywords": ["ipl", "cricket"], "analysis": {"a": 1}, "_id": 1, "platform": "youtube"}
    message = stored_to_message(stored)
    assert "analysis" not in message and "keywords" not in message and message["keyword"] == "ipl"
    print("publisher helpers ok")


if __name__ == "__main__":
    test_decode_and_dead_letter_payload()
    test_happy_path_commits_and_analyzes()
    test_malformed_and_duplicate_messages()
    test_already_analyzed_events_are_skipped()
    test_analysis_failure_does_not_block_progress()
    test_storage_outage_rewinds_and_retries_without_commit()
    test_idle_poll_and_stop()
    test_publisher_helpers()
    print("all worker tests passed")
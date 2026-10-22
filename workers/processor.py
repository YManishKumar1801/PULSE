import json
import time
from datetime import datetime, timezone

from models.schemas import validate_event
from storage.mongodb import StorageUnavailable

EVENTS_TOPIC = "pulse.events"
DEAD_LETTER_TOPIC = "pulse.dead"
MAX_DEAD_LETTER_RAW_CHARS = 2000
STORAGE_RETRY_SECONDS = 5


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def decode_message(raw):
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        event = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        return None, "not_json"
    if not isinstance(event, dict):
        return None, "not_an_object"
    if not validate_event(event):
        return None, "invalid_event"
    return event, None


def dead_letter_payload(raw, reason):
    if isinstance(raw, (bytes, bytearray)):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = str(raw)
    return {"reason": reason, "raw": text[:MAX_DEAD_LETTER_RAW_CHARS], "at": utc_now_iso()}


def compute_lag(consumer):
    partitions = list(consumer.assignment())
    if not partitions:
        return None
    ends = consumer.end_offsets(partitions)
    lag = 0
    for partition in partitions:
        lag += max(0, ends[partition] - consumer.position(partition))
    return lag


class EventWorker:
    def __init__(
        self,
        consumer,
        dead_letter_producer,
        analyze,
        save_events,
        save_analyses,
        fetch_analyzed_ids,
        report_status=None,
        batch_size=32,
        poll_timeout_ms=1000,
        sleep=time.sleep,
        clock=time.monotonic,
        worker_id="worker-1",
        status_interval_seconds=5,
    ):
        self.consumer = consumer
        self.dead_letter_producer = dead_letter_producer
        self.analyze = analyze
        self.save_events = save_events
        self.save_analyses = save_analyses
        self.fetch_analyzed_ids = fetch_analyzed_ids
        self.report_status = report_status
        self.batch_size = batch_size
        self.poll_timeout_ms = poll_timeout_ms
        self.sleep = sleep
        self.clock = clock
        self.worker_id = worker_id
        self.status_interval_seconds = status_interval_seconds
        self.last_status_at = None
        self.counters = {
            "messages": 0,
            "stored": 0,
            "analyzed": 0,
            "skipped_already_analyzed": 0,
            "duplicates_in_batch": 0,
            "malformed": 0,
            "analysis_failures": 0,
            "storage_retries": 0,
            "batches": 0,
        }
        self.batch_seconds = []
        self.busy_seconds = 0.0
        self.started = clock()

    def _send_dead_letters(self, dead):
        for raw, reason in dead:
            self.counters["malformed"] += 1
            try:
                self.dead_letter_producer.send(DEAD_LETTER_TOPIC, value=dead_letter_payload(raw, reason))
            except Exception:
                pass

    def process_batch(self, raw_messages):
        begin = self.clock()
        events = []
        dead = []
        seen = set()
        for raw in raw_messages:
            self.counters["messages"] += 1
            event, reason = decode_message(raw)
            if reason:
                dead.append((raw, reason))
                continue
            if event["event_id"] in seen:
                self.counters["duplicates_in_batch"] += 1
                continue
            seen.add(event["event_id"])
            events.append(event)

        if events:
            summary = self.save_events(events)
            self.counters["stored"] += summary.get("valid", len(events))
            already = self.fetch_analyzed_ids([event["event_id"] for event in events])
            pending = [event for event in events if event["event_id"] not in already]
            self.counters["skipped_already_analyzed"] += len(events) - len(pending)
            if pending:
                try:
                    results = self.analyze(pending)
                    self.save_analyses(results)
                    self.counters["analyzed"] += len(results)
                except StorageUnavailable:
                    raise
                except Exception:
                    self.counters["analysis_failures"] += len(pending)

        self._send_dead_letters(dead)
        self.counters["batches"] += 1
        elapsed = self.clock() - begin
        self.batch_seconds.append(elapsed)
        self.busy_seconds += elapsed
        return elapsed

    def snapshot(self):
        elapsed = max(self.clock() - self.started, 1e-9)
        recent = self.batch_seconds[-200:]
        ordered = sorted(recent)
        p95 = None
        if ordered:
            p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
        try:
            lag = compute_lag(self.consumer)
        except Exception:
            lag = None
        return {
            **self.counters,
            "uptime_seconds": round(elapsed, 1),
            "busy_seconds": round(self.busy_seconds, 3),
            "events_per_busy_second": round(self.counters["stored"] / self.busy_seconds, 3) if self.busy_seconds else None,
            "batch_seconds_avg": round(sum(recent) / len(recent), 3) if recent else None,
            "batch_seconds_p95": round(p95, 3) if p95 is not None else None,
            "consumer_lag": lag,
        }

    def _publish_status(self, force=False):
        if self.report_status is None:
            return
        now = self.clock()
        if not force and self.last_status_at is not None and now - self.last_status_at < self.status_interval_seconds:
            return
        self.last_status_at = now
        try:
            self.report_status(self.worker_id, self.snapshot())
        except Exception:
            pass

    def _rewind(self, records_by_partition):
        for partition, records in records_by_partition.items():
            if records:
                self.consumer.seek(partition, records[0].offset)

    def run_once(self):
        polled = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=self.batch_size)
        if not polled:
            self._publish_status()
            return 0
        raw_messages = [record.value for records in polled.values() for record in records]
        before = dict(self.counters)
        try:
            self.process_batch(raw_messages)
        except StorageUnavailable:
            retries = before["storage_retries"] + 1
            self.counters = before
            self.counters["storage_retries"] = retries
            self._rewind(polled)
            self._publish_status()
            self.sleep(STORAGE_RETRY_SECONDS)
            return 0
        self.consumer.commit()
        self._publish_status()
        return len(raw_messages)

    def run(self, should_stop=lambda: False, max_batches=None):
        batches = 0
        while not should_stop():
            self.run_once()
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break
        self._publish_status(force=True)
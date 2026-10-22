import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError

from models.schemas import validate_event

load_dotenv()

DB_NAME = "pulse_db"
EVENTS_COLLECTION = "events"
SIGNALS_COLLECTION = "signals"
WORKER_STATUS_COLLECTION = "worker_status"

RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 1.0
DUPLICATE_KEY_CODE = 11000
TRANSIENT_ERRORS = (AutoReconnect, ServerSelectionTimeoutError, NetworkTimeout, ConnectionFailure)

_client = None
_indexes_ready = False
_signal_indexes_ready = False


class StorageUnavailable(RuntimeError):
    pass


def with_retry(operation, attempts=RETRY_ATTEMPTS, base_delay=RETRY_BASE_DELAY_SECONDS, sleep=time.sleep):
    last_error = None
    for attempt in range(attempts):
        try:
            return operation()
        except TRANSIENT_ERRORS as error:
            last_error = error
            if attempt < attempts - 1:
                sleep(base_delay * (2 ** attempt))
    raise StorageUnavailable(
        "MongoDB is unreachable. Check your internet connection and the Atlas network access list, then try again."
    ) from last_error


def get_client():
    global _client
    if _client is None:
        uri = os.getenv("MONGO_URI")
        if not uri:
            raise RuntimeError("MONGO_URI is not set")
        _client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=20000,
        )
    return _client


def get_events_collection():
    global _indexes_ready
    collection = get_client()[DB_NAME][EVENTS_COLLECTION]
    if not _indexes_ready:
        def create_indexes():
            collection.create_index([("event_id", ASCENDING)], unique=True)
            collection.create_index([("keywords", ASCENDING), ("timestamp", ASCENDING)])
            collection.create_index([("platform", ASCENDING), ("timestamp", ASCENDING)])

        with_retry(create_indexes)
        _indexes_ready = True
    return collection


def bulk_write_safely(collection, operations):
    if not operations:
        return {"inserted": 0, "modified": 0, "duplicates": 0}

    def write():
        try:
            result = collection.bulk_write(operations, ordered=False)
            return {"inserted": result.upserted_count, "modified": result.modified_count, "duplicates": 0}
        except BulkWriteError as error:
            details = error.details or {}
            write_errors = details.get("writeErrors", [])
            if write_errors and all(item.get("code") == DUPLICATE_KEY_CODE for item in write_errors):
                return {
                    "inserted": details.get("nUpserted", 0),
                    "modified": details.get("nModified", 0),
                    "duplicates": len(write_errors),
                }
            raise

    return with_retry(write)


def save_events(events):
    valid = [e for e in events if validate_event(e)]
    summary = {"received": len(events), "valid": len(valid), "inserted": 0, "updated": 0, "duplicates": 0}
    if not valid:
        return summary

    operations = []
    for event in valid:
        document = dict(event)
        keyword = document.pop("keyword", None)
        update = {"$set": document}
        if keyword:
            update["$addToSet"] = {"keywords": keyword}
        operations.append(UpdateOne({"event_id": document["event_id"]}, update, upsert=True))

    outcome = bulk_write_safely(get_events_collection(), operations)
    summary["inserted"] = outcome["inserted"]
    summary["updated"] = outcome["modified"]
    summary["duplicates"] = outcome["duplicates"]
    return summary


def _build_query(keyword=None, platform=None, since=None):
    query = {}
    if keyword:
        query["keywords"] = keyword
    if platform:
        query["platform"] = platform
    if since:
        query["timestamp"] = {"$gte": since}
    return query


def fetch_events(keyword=None, platform=None, since=None, limit=5000):
    def read():
        cursor = (
            get_events_collection()
            .find(_build_query(keyword, platform, since), {"_id": 0})
            .sort("timestamp", ASCENDING)
            .limit(limit)
        )
        return list(cursor)

    return with_retry(read)


def count_events(keyword=None, platform=None):
    return with_retry(lambda: get_events_collection().count_documents(_build_query(keyword, platform)))


def fetch_unanalyzed_events(keyword=None, platform=None, limit=500):
    query = _build_query(keyword, platform)
    query["analysis"] = {"$exists": False}
    return with_retry(lambda: list(get_events_collection().find(query, {"_id": 0}).limit(limit)))


def count_unanalyzed_events(keyword=None, platform=None):
    query = _build_query(keyword, platform)
    query["analysis"] = {"$exists": False}
    return with_retry(lambda: get_events_collection().count_documents(query))


def save_analyses(items):
    if not items:
        return 0
    operations = [
        UpdateOne({"event_id": event_id}, {"$set": {"analysis": analysis}})
        for event_id, analysis in items
    ]
    return bulk_write_safely(get_events_collection(), operations)["modified"]


def get_signals_collection():
    global _signal_indexes_ready
    collection = get_client()[DB_NAME][SIGNALS_COLLECTION]
    if not _signal_indexes_ready:
        def create_indexes():
            collection.create_index([("signal_id", ASCENDING)], unique=True)
            collection.create_index([("updated_at", DESCENDING)])
            collection.create_index([("priority", ASCENDING), ("score", DESCENDING)])

        with_retry(create_indexes)
        _signal_indexes_ready = True
    return collection


def save_signals(signals):
    if not signals:
        return {"received": 0, "inserted": 0, "updated": 0}
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    operations = []
    for signal in signals:
        document = dict(signal)
        first_seen = document.pop("created_at", now)
        document["updated_at"] = now
        operations.append(
            UpdateOne(
                {"signal_id": document["signal_id"]},
                {"$set": document, "$setOnInsert": {"created_at": first_seen}},
                upsert=True,
            )
        )
    outcome = bulk_write_safely(get_signals_collection(), operations)
    return {"received": len(signals), "inserted": outcome["inserted"], "updated": outcome["modified"]}


def fetch_signals(priority=None, limit=50):
    query = {}
    if priority:
        query["priority"] = priority

    def read():
        cursor = (
            get_signals_collection()
            .find(query, {"_id": 0})
            .sort("updated_at", DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    return with_retry(read)


def count_signals():
    return with_retry(lambda: get_signals_collection().count_documents({}))


def fetch_analyzed_ids(event_ids):
    if not event_ids:
        return set()
    query = {"event_id": {"$in": list(event_ids)}, "analysis": {"$exists": True}}
    documents = with_retry(lambda: list(get_events_collection().find(query, {"_id": 0, "event_id": 1})))
    return {document["event_id"] for document in documents}


def save_worker_status(worker_id, status):
    document = dict(status)
    document["worker_id"] = worker_id
    document["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def write():
        get_client()[DB_NAME][WORKER_STATUS_COLLECTION].update_one(
            {"worker_id": worker_id}, {"$set": document}, upsert=True
        )

    with_retry(write)


def fetch_worker_status():
    def read():
        return list(get_client()[DB_NAME][WORKER_STATUS_COLLECTION].find({}, {"_id": 0}).sort("updated_at", DESCENDING))

    return with_retry(read)
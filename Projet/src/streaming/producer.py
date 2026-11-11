"""
IMDb Big Data Processing — Kafka Producer
==========================================
Connects to the Wikipedia SSE stream (recentchange) and publishes
English-Wikipedia edit events to the Kafka topic ``wikipedia-edits``.

Usage (inside the imdb_spark container):
    python -m streaming.producer

Environment variables (with defaults):
    KAFKA_BOOTSTRAP_SERVERS  kafka:19092
    KAFKA_TOPIC              wikipedia-edits
    SSE_URL                  https://stream.wikimedia.org/v2/stream/recentchange
"""

import json
import logging
import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC: str = os.environ.get("KAFKA_TOPIC", "wikipedia-edits")
SSE_URL: str = os.environ.get(
    "SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)

# Only ingest edits from English Wikipedia (broad enough to capture many events)
TARGET_SERVER = "en.wikipedia.org"
TARGET_TYPE = "edit"

# Namespaces: 0 = article, 4 = project, 10 = template, 14 = category, …
# Keep all namespaces so the streaming job can filter further if needed.
ALLOWED_NAMESPACES = {0, 4, 10, 14}

RETRY_SLEEP_SECONDS: int = 5
MAX_RETRIES: int = int(os.environ.get("SSE_MAX_RETRIES", "0"))  # 0 = unlimited

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _delivery_report(err, msg):
    if err is not None:
        log.warning("Delivery failed for key %s: %s", msg.key(), err)


def _ensure_topic(bootstrap_servers: str, topic: str) -> None:
    """Create the topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        fs = admin.create_topics(
            [NewTopic(topic, num_partitions=3, replication_factor=1)]
        )
        for t, f in fs.items():
            try:
                f.result()
                log.info("Topic '%s' created.", t)
            except Exception as exc:  # already exists or other benign error
                log.debug("Topic creation note for '%s': %s", t, exc)


def _build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "linger.ms": 50,
            "batch.size": 32768,
        }
    )


# ---------------------------------------------------------------------------
# SSE parsing
# ---------------------------------------------------------------------------

def _parse_sse_lines(lines) -> dict | None:
    """
    Accumulate SSE ``data:`` lines and return the parsed JSON object, or
    ``None`` if the chunk has no data payload.
    """
    data_parts = []
    for raw in lines:
        line: str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if line.startswith("data:"):
            data_parts.append(line[5:].strip())
    if not data_parts:
        return None
    try:
        return json.loads(" ".join(data_parts))
    except json.JSONDecodeError:
        return None


def _extract_event(event: dict) -> dict | None:
    """
    Return a minimal, serialisable dict for qualifying edit events, or
    ``None`` if the event should be dropped.
    """
    if event.get("type") != TARGET_TYPE:
        return None
    if event.get("server_name") != TARGET_SERVER:
        return None
    ns = event.get("namespace", -1)
    if ns not in ALLOWED_NAMESPACES:
        return None

    return {
        "title": event.get("title", ""),
        "wiki": event.get("wiki", ""),
        "timestamp": event.get("timestamp", 0),
        "user": event.get("user", ""),
        "comment": (event.get("comment") or "")[:512],  # cap length
        "server_name": event.get("server_name", ""),
        "namespace": ns,
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(producer: Producer) -> None:
    """Stream SSE events from Wikipedia and publish qualifying edits to Kafka."""
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "IMDb-BigData-Producer/1.0 (academic project)",
    }

    retries = 0
    published = 0

    while True:
        try:
            log.info("Connecting to SSE stream: %s", SSE_URL)
            with requests.get(SSE_URL, stream=True, headers=headers, timeout=30) as resp:
                resp.raise_for_status()
                retries = 0  # reset on successful connect

                # Wikipedia SSE stream separates events with blank lines.
                # requests.iter_lines() already strips blank lines, so we
                # collect non-blank lines per event manually.
                pending: list[str] = []
                for raw_line in resp.iter_lines(decode_unicode=False):
                    line = (
                        raw_line.decode("utf-8")
                        if isinstance(raw_line, bytes)
                        else raw_line
                    )

                    if line == "":
                        # Blank line → end of one SSE event block
                        if pending:
                            event_obj = _parse_sse_lines(pending)
                            pending = []
                            if event_obj:
                                record = _extract_event(event_obj)
                                if record:
                                    payload = json.dumps(record, ensure_ascii=False)
                                    producer.produce(
                                        TOPIC,
                                        key=record["title"].encode("utf-8"),
                                        value=payload.encode("utf-8"),
                                        callback=_delivery_report,
                                    )
                                    producer.poll(0)  # non-blocking flush
                                    published += 1
                                    if published % 100 == 0:
                                        log.info("Published %d events so far.", published)
                    else:
                        pending.append(line)

        except requests.exceptions.RequestException as exc:
            retries += 1
            if MAX_RETRIES and retries > MAX_RETRIES:
                log.error("Max retries (%d) exceeded. Stopping.", MAX_RETRIES)
                break
            log.warning(
                "SSE connection error (attempt %d): %s — retrying in %ds …",
                retries,
                exc,
                RETRY_SLEEP_SECONDS,
            )
            producer.flush()
            time.sleep(RETRY_SLEEP_SECONDS)

        except KeyboardInterrupt:
            log.info("Interrupted by user. Flushing and exiting …")
            break

    producer.flush()
    log.info("Producer stopped. Total events published: %d", published)


def main() -> None:
    log.info("Bootstrap servers: %s  |  Topic: %s", BOOTSTRAP_SERVERS, TOPIC)
    _ensure_topic(BOOTSTRAP_SERVERS, TOPIC)
    producer = _build_producer()
    try:
        run(producer)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()

"""Kafka producer: Wikimedia EventStreams (recentchange) -> Kafka topic.

Keeps English Wikipedia article edits and page creations (namespace 0) and
publishes one JSON message per event. The event time ("timestamp", epoch
seconds) is kept so Spark can work in event time; Kafka adds its own
ingestion timestamp to each message.

Usage (inside the imdb_spark container):
    python -m streaming.producer [--duration SECONDS] [--wiki enwiki]

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS   default kafka:19092
    KAFKA_TOPIC               default wikipedia-edits
"""
import argparse
import json
import logging
import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-edits")
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = ("ece-imdb-streaming/1.0 "
              "(https://github.com/Alexhuang03/Big-Data-Processing-application; "
              "student project)")
EVENT_TYPES = {"edit", "new"}
ARTICLE_NAMESPACE = 0
RETRY_SECONDS = 5

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [producer] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def ensure_topic(topic, partitions=3):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    if topic in admin.list_topics(timeout=10).topics:
        return
    for name, future in admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]).items():
        try:
            future.result()
            log.info("Topic %s created", name)
        except Exception as exc:  # created concurrently by another client
            log.info("Topic %s not created: %s", name, exc)


def to_message(event, wiki):
    """Keep only the fields used downstream, or None to drop the event."""
    if (event.get("wiki") != wiki or event.get("type") not in EVENT_TYPES
            or event.get("namespace") != ARTICLE_NAMESPACE):
        return None
    length = event.get("length") or {}
    return {
        "event_id": event.get("meta", {}).get("id"),
        "title": event.get("title"),
        "wiki": event.get("wiki"),
        "type": event.get("type"),
        "timestamp": event.get("timestamp"),
        "user": event.get("user"),
        "bot": event.get("bot"),
        "minor": event.get("minor"),
        "comment": (event.get("comment") or "")[:500],
        "length_old": length.get("old"),
        "length_new": length.get("new"),
    }


def read_events(deadline):
    """Yield decoded events from the SSE stream until the deadline."""
    headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    last_event_id = None
    while time.time() < deadline:
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id  # resume after a disconnect
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True,
                              timeout=(10, 60)) as response:
                response.raise_for_status()
                data_lines = []
                for raw in response.iter_lines(decode_unicode=True):
                    if time.time() >= deadline:
                        return
                    if raw.startswith("id:"):
                        last_event_id = raw[3:].strip()
                    elif raw.startswith("data:"):
                        data_lines.append(raw[5:].strip())
                    elif raw == "" and data_lines:  # blank line ends an event
                        try:
                            yield json.loads("".join(data_lines))
                        except json.JSONDecodeError:
                            pass
                        data_lines = []
        except requests.RequestException as exc:
            log.warning("Stream error: %s, reconnecting in %ss", exc, RETRY_SECONDS)
            time.sleep(RETRY_SECONDS)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duration", type=int, default=1800,
                        help="seconds to run (default 1800)")
    parser.add_argument("--wiki", default="enwiki", help="wiki database name")
    args = parser.parse_args()

    ensure_topic(TOPIC)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS,
                         "acks": "all", "linger.ms": 50})
    deadline = time.time() + args.duration
    seen = published = 0
    log.info("Publishing %s article edits to %s for %ss", args.wiki, TOPIC, args.duration)
    try:
        for event in read_events(deadline):
            seen += 1
            message = to_message(event, args.wiki)
            if message is None:
                continue
            producer.produce(TOPIC, key=message["title"].encode("utf-8"),
                             value=json.dumps(message, ensure_ascii=False).encode("utf-8"))
            producer.poll(0)
            published += 1
            if published % 500 == 0:
                log.info("%d events read, %d published", seen, published)
    except KeyboardInterrupt:
        log.info("Interrupted")
    finally:
        producer.flush(30)
        log.info("Stopped: %d events read, %d published", seen, published)


if __name__ == "__main__":
    main()

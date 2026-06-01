from __future__ import annotations

import json
import time
from typing import Iterable

from confluent_kafka import Producer

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger


logger = get_logger(__name__)


def build_kafka_producer() -> Producer:
    settings = get_settings()
    return Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})


def emit_events(
    *,
    topic: str,
    events: Iterable[dict],
    key_field: str,
    interval_seconds: int,
    dry_run: bool,
) -> int:
    producer = None if dry_run else build_kafka_producer()
    produced = 0

    for event in events:
        produced += 1
        payload = event.get("payload") or {}
        key = str(payload.get(key_field, ""))
        serialized = json.dumps(event)

        if dry_run:
            logger.info("DRY RUN topic=%s key=%s payload=%s", topic, key, serialized)
        else:
            producer.produce(topic=topic, key=key, value=serialized)
            producer.poll(0)
            logger.info("Produced topic=%s key=%s event_id=%s", topic, key, event["event_id"])

        if interval_seconds > 0:
            time.sleep(interval_seconds)

    if producer is not None:
        producer.flush()

    return produced

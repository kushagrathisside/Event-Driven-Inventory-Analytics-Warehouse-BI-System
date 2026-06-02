from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Iterable

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger

if TYPE_CHECKING:
    from confluent_kafka import Producer
    from typing import Iterable as _Iterable


logger = get_logger(__name__)


def build_kafka_producer() -> Producer:
    from confluent_kafka import Producer as _Producer

    settings = get_settings()
    return _Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})


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


def run_producer(
    *,
    topic: str,
    key_field: str,
    generate: "Iterable[dict]",
    max_events: int | None = None,
    dry_run: bool | None = None,
    label: str = "producer",
) -> int:
    """
    Shared runner for all domain producers.
    Resolves max_events and dry_run from settings if not supplied, then calls emit_events.
    """
    settings = get_settings()
    event_count = max(settings.producer.max_events if max_events is None else max_events, 1)
    dry_run = settings.producer.dry_run if dry_run is None else dry_run
    logger.info("Starting %s topic=%s count=%s dry_run=%s", label, topic, event_count, dry_run)
    return emit_events(
        topic=topic,
        events=generate,
        key_field=key_field,
        interval_seconds=settings.producer.interval_seconds,
        dry_run=dry_run,
    )

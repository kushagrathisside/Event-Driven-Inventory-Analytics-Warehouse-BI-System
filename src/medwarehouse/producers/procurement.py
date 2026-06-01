from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid5, NAMESPACE_URL

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.producers.common import emit_events


logger = get_logger(__name__)


def _sample_procurement_events(count: int, seed: str, product_id: str, supplier_id: str):
    base_time = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    for index in range(count):
        event_time = base_time + timedelta(minutes=index * 10)
        po_id = str(uuid5(NAMESPACE_URL, f"{seed}:po:{index}"))
        yield {
            "event_id": str(uuid5(NAMESPACE_URL, f"{seed}:procurement:{index}")),
            "event_type": "PO_CREATED",
            "event_time": event_time.isoformat(),
            "producer": "procurement_service",
            "schema_version": 1,
            "payload": {
                "po_id": po_id,
                "product_id": product_id,
                "ordered_quantity": 200 + index,
                "supplier_id": supplier_id,
                "trigger_reason": "AUTO_REORDER",
            },
        }


def run_procurement_producer(*, max_events: int | None = None, dry_run: bool | None = None) -> int:
    settings = get_settings()
    count = settings.producer.max_events if max_events is None else max_events
    dry_run = settings.producer.dry_run if dry_run is None else dry_run
    logger.info("Starting procurement producer count=%s dry_run=%s", count, dry_run)
    return emit_events(
        topic=settings.kafka.procurement_topic,
        events=_sample_procurement_events(
            count=max(count, 1),
            seed=settings.producer.inventory_seed,
            product_id=settings.producer.sample_product_id,
            supplier_id=settings.producer.sample_supplier_id,
        ),
        key_field="product_id",
        interval_seconds=settings.producer.interval_seconds,
        dry_run=dry_run,
    )

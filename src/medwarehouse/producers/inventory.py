from __future__ import annotations

from datetime import datetime

from medwarehouse.config import get_settings
from medwarehouse.contracts.inventory import generate_inventory_sample_events
from medwarehouse.logging import get_logger
from medwarehouse.producers.common import emit_events


logger = get_logger(__name__)


def run_inventory_producer(*, max_events: int | None = None, dry_run: bool | None = None) -> int:
    settings = get_settings()
    configured_count = settings.producer.max_events if max_events is None else max_events
    event_count = max(configured_count, 1)
    dry_run = settings.producer.dry_run if dry_run is None else dry_run

    start_time = datetime.fromisoformat(settings.producer.inventory_start_time)
    events = generate_inventory_sample_events(
        seed=settings.producer.inventory_seed,
        start_time=start_time,
        product_id=settings.producer.sample_product_id,
        warehouse_id=settings.producer.sample_warehouse_id,
        supplier_id=settings.producer.sample_supplier_id,
        batch_number=settings.producer.sample_batch_number,
        expiry_date=settings.producer.sample_expiry_date,
        count=event_count,
    )

    logger.info(
        "Starting inventory producer topic=%s count=%s dry_run=%s",
        settings.kafka.inventory_topic,
        event_count,
        dry_run,
    )
    return emit_events(
        topic=settings.kafka.inventory_topic,
        events=events,
        key_field="product_id",
        interval_seconds=settings.producer.interval_seconds,
        dry_run=dry_run,
    )

from __future__ import annotations

from datetime import datetime

from medwarehouse.config import get_settings
from medwarehouse.contracts.inventory import generate_inventory_sample_events
from medwarehouse.producers.common import run_producer


def run_inventory_producer(*, max_events: int | None = None, dry_run: bool | None = None) -> int:
    settings = get_settings()
    event_count = max(settings.producer.max_events if max_events is None else max_events, 1)
    start_time = datetime.fromisoformat(settings.sample_data.start_time)
    return run_producer(
        topic=settings.kafka.inventory_topic,
        key_field="product_id",
        generate=generate_inventory_sample_events(
            seed=settings.sample_data.seed,
            start_time=start_time,
            product_id=settings.sample_data.product_id,
            warehouse_id=settings.sample_data.warehouse_id,
            supplier_id=settings.sample_data.supplier_id,
            batch_number=settings.sample_data.batch_number,
            expiry_date=settings.sample_data.expiry_date,
            count=event_count,
        ),
        max_events=max_events,
        dry_run=dry_run,
        label="inventory producer",
    )

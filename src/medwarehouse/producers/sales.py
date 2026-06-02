from __future__ import annotations

from datetime import datetime

from medwarehouse.config import get_settings
from medwarehouse.contracts.sales import generate_sales_sample_events
from medwarehouse.producers.common import run_producer


def run_sales_producer(*, max_events: int | None = None, dry_run: bool | None = None) -> int:
    settings = get_settings()
    event_count = max(settings.producer.max_events if max_events is None else max_events, 1)
    start_time = datetime.fromisoformat(settings.sample_data.start_time)
    return run_producer(
        topic=settings.kafka.sales_topic,
        key_field="sale_id",
        generate=generate_sales_sample_events(
            seed=settings.sample_data.seed,
            start_time=start_time,
            product_id=settings.sample_data.product_id,
            warehouse_id=settings.sample_data.warehouse_id,
            count=event_count,
            currency=settings.sample_data.currency,
        ),
        max_events=max_events,
        dry_run=dry_run,
        label="sales producer",
    )

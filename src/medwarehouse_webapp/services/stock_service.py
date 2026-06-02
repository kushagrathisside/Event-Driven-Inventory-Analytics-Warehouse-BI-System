from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

import os

import requests

from medwarehouse.config import get_settings
from medwarehouse.contracts._utils import deterministic_uuid
from medwarehouse.contracts.inventory import (
    build_inventory_event,
    validate_inventory_event_dict,
)
from medwarehouse.logging import get_logger
from medwarehouse.producers.common import emit_events
from medwarehouse_webapp.db import connect, get_master_db


logger = get_logger(__name__)

_MAX_TRIGGER_RETRIES = 3
_TRIGGER_BACKOFF_BASE = 0.5  # seconds; doubles on each retry (0.5s, 1s, 2s)


def _pipeline_trigger_url() -> str:
    host = os.environ.get("MW_PLATFORM_HOST", "127.0.0.1")
    port = os.environ.get("MW_PLATFORM_PORT", "8787")
    return f"http://{host}:{port}/api/jobs/build_gold/start"


def _trigger_pipeline() -> None:
    """Attempt to trigger the Gold pipeline with exponential backoff. Fails silently after retries."""
    url = _pipeline_trigger_url()
    for attempt in range(_MAX_TRIGGER_RETRIES):
        try:
            resp = requests.post(url, timeout=3)
            resp.raise_for_status()
            return
        except requests.RequestException as exc:
            wait = _TRIGGER_BACKOFF_BASE * (2 ** attempt)
            if attempt < _MAX_TRIGGER_RETRIES - 1:
                logger.warning(
                    "Pipeline trigger attempt %d/%d failed, retrying in %.1fs: %s",
                    attempt + 1, _MAX_TRIGGER_RETRIES, wait, exc,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Pipeline trigger failed after %d attempts — pipeline must be run manually. Error: %s",
                    _MAX_TRIGGER_RETRIES, exc,
                )


def _async_trigger_pipeline() -> None:
    threading.Thread(target=_trigger_pipeline, daemon=True).start()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _emit(event: dict[str, Any]) -> None:
    settings = get_settings()
    emit_events(
        topic=settings.kafka.inventory_topic,
        events=[event],
        key_field="product_id",
        interval_seconds=0,
        dry_run=settings.producer.dry_run,
    )


def record_stock_received(
    *,
    product_id: str,
    warehouse_id: str,
    batch_number: str,
    expiry_date: str,
    quantity: int,
    supplier_id: str | None = None,
    cost_price_per_unit: float | None = None,
    sale_price_per_unit: float | None = None,
    operator: str = "webapp",
) -> dict[str, Any]:
    """
    UPSERT a lot into inventory_lots (create or add to existing batch).
    warehouse_id maps to warehouse_locations.location_id.
    """
    settings = get_settings()
    event_time = _now_utc()
    event_id = deterministic_uuid(
        settings.sample_data.seed,
        f"received:{product_id}:{batch_number}:{event_time.isoformat()}",
    )

    event = build_inventory_event(
        event_id=event_id,
        event_type="STOCK_RECEIVED",
        event_time=event_time,
        producer=operator,
        product_id=product_id,
        warehouse_id=warehouse_id,
        batch_number=batch_number,
        expiry_date=expiry_date,
        quantity_delta=quantity,
        supplier_id=supplier_id,
    )
    errors = validate_inventory_event_dict(event)
    if errors:
        raise ValueError(f"Invalid STOCK_RECEIVED: {errors}")

    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inventory_lots
                    (product_id, location_id, batch_number, quantity_on_hand,
                     cost_price_per_unit, sale_price_per_unit, expiry_date, received_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, location_id, batch_number)
                    DO UPDATE SET
                        quantity_on_hand = inventory_lots.quantity_on_hand + EXCLUDED.quantity_on_hand,
                        received_at = EXCLUDED.received_at
                """,
                (
                    product_id, warehouse_id, batch_number, quantity,
                    cost_price_per_unit or 0, sale_price_per_unit or 0,
                    expiry_date, event_time,
                ),
            )
        conn.commit()

    _emit(event)
    _async_trigger_pipeline()
    logger.info("STOCK_RECEIVED event_id=%s product=%s batch=%s qty=+%s",
                event_id, product_id, batch_number, quantity)
    return {"event_id": event_id, "event_type": "STOCK_RECEIVED", "status": "accepted"}


def record_stock_sold(
    *,
    product_id: str,
    warehouse_id: str,
    batch_number: str,
    expiry_date: str,
    quantity: int,
    sale_id: str,
    operator: str = "webapp",
) -> dict[str, Any]:
    settings = get_settings()
    event_time = _now_utc()
    event_id = deterministic_uuid(
        settings.sample_data.seed,
        f"sold:{sale_id}:{event_time.isoformat()}",
    )

    event = build_inventory_event(
        event_id=event_id,
        event_type="STOCK_SOLD",
        event_time=event_time,
        producer=operator,
        product_id=product_id,
        warehouse_id=warehouse_id,
        batch_number=batch_number,
        expiry_date=expiry_date,
        quantity_delta=-abs(quantity),
        sale_id=sale_id,
    )
    errors = validate_inventory_event_dict(event)
    if errors:
        raise ValueError(f"Invalid STOCK_SOLD: {errors}")

    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inventory_lots
                SET quantity_on_hand = quantity_on_hand - %s
                WHERE product_id = %s
                  AND location_id = %s
                  AND batch_number = %s
                  AND quantity_on_hand >= %s
                """,
                (abs(quantity), product_id, warehouse_id, batch_number, abs(quantity)),
            )
            if cur.rowcount == 0:
                raise ValueError(
                    f"Insufficient stock or lot not found: product={product_id} "
                    f"batch={batch_number} warehouse={warehouse_id} qty={quantity}"
                )
        conn.commit()

    _emit(event)
    _async_trigger_pipeline()
    logger.info("STOCK_SOLD event_id=%s product=%s batch=%s qty=-%s sale=%s",
                event_id, product_id, batch_number, quantity, sale_id)
    return {"event_id": event_id, "event_type": "STOCK_SOLD", "status": "accepted"}


def record_stock_adjusted(
    *,
    product_id: str,
    warehouse_id: str,
    batch_number: str,
    quantity_delta: int,
    reason: str,
    operator: str = "webapp",
) -> dict[str, Any]:
    settings = get_settings()
    event_time = _now_utc()
    event_id = deterministic_uuid(
        settings.sample_data.seed,
        f"adjusted:{product_id}:{batch_number}:{event_time.isoformat()}",
    )

    event = build_inventory_event(
        event_id=event_id,
        event_type="STOCK_ADJUSTED",
        event_time=event_time,
        producer=operator,
        product_id=product_id,
        warehouse_id=warehouse_id,
        batch_number=batch_number,
        expiry_date=None,
        quantity_delta=quantity_delta,
        reason=reason,
    )
    errors = validate_inventory_event_dict(event)
    if errors:
        raise ValueError(f"Invalid STOCK_ADJUSTED: {errors}")

    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inventory_lots
                SET quantity_on_hand = GREATEST(0, quantity_on_hand + %s)
                WHERE product_id = %s
                  AND location_id = %s
                  AND batch_number = %s
                """,
                (quantity_delta, product_id, warehouse_id, batch_number),
            )
            if cur.rowcount == 0:
                raise ValueError(
                    f"Lot not found: product={product_id} "
                    f"batch={batch_number} warehouse={warehouse_id}"
                )
        conn.commit()

    _emit(event)
    _async_trigger_pipeline()
    logger.info("STOCK_ADJUSTED event_id=%s product=%s delta=%s reason=%s",
                event_id, product_id, quantity_delta, reason)
    return {"event_id": event_id, "event_type": "STOCK_ADJUSTED", "status": "accepted"}


def record_stock_expired(
    *,
    product_id: str,
    warehouse_id: str,
    batch_number: str,
    expiry_date: str,
    quantity: int,
    operator: str = "webapp",
) -> dict[str, Any]:
    settings = get_settings()
    event_time = _now_utc()
    event_id = deterministic_uuid(
        settings.sample_data.seed,
        f"expired:{product_id}:{batch_number}:{event_time.isoformat()}",
    )

    event = build_inventory_event(
        event_id=event_id,
        event_type="STOCK_EXPIRED",
        event_time=event_time,
        producer=operator,
        product_id=product_id,
        warehouse_id=warehouse_id,
        batch_number=batch_number,
        expiry_date=expiry_date,
        quantity_delta=-abs(quantity),
    )
    errors = validate_inventory_event_dict(event)
    if errors:
        raise ValueError(f"Invalid STOCK_EXPIRED: {errors}")

    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE inventory_lots
                SET quantity_on_hand = quantity_on_hand - %s
                WHERE product_id = %s
                  AND location_id = %s
                  AND batch_number = %s
                  AND quantity_on_hand >= %s
                """,
                (abs(quantity), product_id, warehouse_id, batch_number, abs(quantity)),
            )
            if cur.rowcount == 0:
                raise ValueError(
                    f"Insufficient stock or lot not found for expiry write-off: "
                    f"product={product_id} batch={batch_number} warehouse={warehouse_id}"
                )
        conn.commit()

    _emit(event)
    _async_trigger_pipeline()
    logger.info("STOCK_EXPIRED event_id=%s product=%s batch=%s qty=-%s",
                event_id, product_id, batch_number, quantity)
    return {"event_id": event_id, "event_type": "STOCK_EXPIRED", "status": "accepted"}

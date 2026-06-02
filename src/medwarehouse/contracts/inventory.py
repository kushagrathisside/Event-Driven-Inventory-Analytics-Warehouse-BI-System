from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from medwarehouse.contracts._utils import deterministic_uuid, to_utc_iso as _to_utc_iso


INVENTORY_SCHEMA_VERSION = 1
INVENTORY_EVENT_TYPES = frozenset(
    {"STOCK_RECEIVED", "STOCK_SOLD", "STOCK_ADJUSTED", "STOCK_EXPIRED"}
)


@dataclass(frozen=True)
class InventoryRule:
    required_payload_fields: tuple[str, ...]
    quantity_direction: str


RULES: dict[str, InventoryRule] = {
    "STOCK_RECEIVED": InventoryRule(
        required_payload_fields=("product_id", "warehouse_id", "batch_number", "expiry_date"),
        quantity_direction="positive",
    ),
    "STOCK_SOLD": InventoryRule(
        required_payload_fields=(
            "product_id",
            "warehouse_id",
            "batch_number",
            "expiry_date",
            "sale_id",
        ),
        quantity_direction="negative",
    ),
    "STOCK_ADJUSTED": InventoryRule(
        required_payload_fields=(
            "product_id",
            "warehouse_id",
            "batch_number",
            "reason",
        ),
        quantity_direction="non_zero",
    ),
    "STOCK_EXPIRED": InventoryRule(
        required_payload_fields=("product_id", "warehouse_id", "batch_number", "expiry_date"),
        quantity_direction="negative",
    ),
}




def build_inventory_event(
    *,
    event_id: str,
    event_type: str,
    event_time: datetime,
    producer: str,
    product_id: str,
    warehouse_id: str,
    batch_number: str,
    expiry_date: str | None,
    quantity_delta: int,
    supplier_id: str | None = None,
    sale_id: str | None = None,
    reason: str | None = None,
    schema_version: int = INVENTORY_SCHEMA_VERSION,
) -> dict[str, Any]:
    payload = {
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "batch_number": batch_number,
        "expiry_date": expiry_date,
        "quantity_delta": quantity_delta,
        "supplier_id": supplier_id,
        "sale_id": sale_id,
        "reason": reason,
    }

    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": _to_utc_iso(event_time),
        "producer": producer,
        "schema_version": schema_version,
        "payload": payload,
    }


def validate_inventory_event_dict(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    event_type = event.get("event_type")
    payload = event.get("payload") or {}
    quantity_delta = payload.get("quantity_delta")

    for top_level_field in ("event_id", "event_type", "event_time", "producer", "schema_version"):
        if not event.get(top_level_field):
            errors.append(f"missing_{top_level_field}")

    schema_version = event.get("schema_version")
    if schema_version is not None and int(schema_version) != INVENTORY_SCHEMA_VERSION:
        errors.append("unsupported_schema_version")

    if event_type not in INVENTORY_EVENT_TYPES:
        errors.append("unsupported_event_type")
        return errors

    rule = RULES[event_type]
    for field in rule.required_payload_fields:
        if not payload.get(field):
            errors.append(f"missing_payload_{field}")

    if quantity_delta is None:
        errors.append("missing_payload_quantity_delta")
        return errors

    if rule.quantity_direction == "positive" and int(quantity_delta) <= 0:
        errors.append("invalid_quantity_sign")
    if rule.quantity_direction == "negative" and int(quantity_delta) >= 0:
        errors.append("invalid_quantity_sign")
    if rule.quantity_direction == "non_zero" and int(quantity_delta) == 0:
        errors.append("invalid_quantity_zero")

    return errors


def generate_inventory_sample_events(
    *,
    seed: str,
    start_time: datetime,
    product_id: str,
    warehouse_id: str,
    supplier_id: str,
    batch_number: str,
    expiry_date: str,
    count: int,
) -> Iterable[dict[str, Any]]:
    sequence = [
        ("STOCK_RECEIVED", 200, None, None),
        ("STOCK_ADJUSTED", -10, None, "AUDIT"),
        ("STOCK_SOLD", -24, "SALE-LOCAL-001", None),
        ("STOCK_RECEIVED", 120, None, None),
        ("STOCK_EXPIRED", -5, None, None),
    ]

    for index in range(count):
        event_type, quantity_delta, sale_id, reason = sequence[index % len(sequence)]
        event_time = start_time + timedelta(minutes=index * 5)
        event_name = f"inventory:{event_type}:{index}"
        event_id = deterministic_uuid(seed, event_name)
        yield build_inventory_event(
            event_id=event_id,
            event_type=event_type,
            event_time=event_time,
            producer="inventory_service",
            product_id=product_id,
            warehouse_id=warehouse_id,
            batch_number=batch_number,
            expiry_date=expiry_date,
            quantity_delta=quantity_delta,
            supplier_id=supplier_id if event_type == "STOCK_RECEIVED" else None,
            sale_id=sale_id,
            reason=reason,
        )

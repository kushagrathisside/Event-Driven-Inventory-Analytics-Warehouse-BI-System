from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from medwarehouse.contracts._utils import deterministic_uuid, to_utc_iso as _to_utc_iso


PROCUREMENT_SCHEMA_VERSION = 1
PROCUREMENT_EVENT_TYPES = frozenset({
    "PO_CREATED", "PO_APPROVED", "PO_RECEIVED", "PO_CANCELLED",
})
VALID_TRIGGER_REASONS = frozenset({"AUTO_REORDER", "MANUAL"})


@dataclass(frozen=True)
class ProcurementRule:
    required_payload_fields: tuple[str, ...]


RULES: dict[str, ProcurementRule] = {
    "PO_CREATED": ProcurementRule(
        required_payload_fields=("po_id", "product_id", "ordered_quantity", "supplier_id", "trigger_reason"),
    ),
    "PO_APPROVED": ProcurementRule(
        required_payload_fields=("po_id", "approved_by"),
    ),
    "PO_RECEIVED": ProcurementRule(
        required_payload_fields=("po_id", "product_id", "received_quantity", "warehouse_id"),
    ),
    "PO_CANCELLED": ProcurementRule(
        required_payload_fields=("po_id", "reason"),
    ),
}




def build_procurement_event(
    *,
    event_id: str,
    event_type: str,
    event_time: datetime,
    producer: str,
    po_id: str,
    product_id: str | None = None,
    ordered_quantity: int | None = None,
    supplier_id: str | None = None,
    trigger_reason: str | None = None,
    approved_by: str | None = None,
    received_quantity: int | None = None,
    warehouse_id: str | None = None,
    reason: str | None = None,
    schema_version: int = PROCUREMENT_SCHEMA_VERSION,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"po_id": po_id}
    if product_id is not None:
        payload["product_id"] = product_id
    if ordered_quantity is not None:
        payload["ordered_quantity"] = ordered_quantity
    if supplier_id is not None:
        payload["supplier_id"] = supplier_id
    if trigger_reason is not None:
        payload["trigger_reason"] = trigger_reason
    if approved_by is not None:
        payload["approved_by"] = approved_by
    if received_quantity is not None:
        payload["received_quantity"] = received_quantity
    if warehouse_id is not None:
        payload["warehouse_id"] = warehouse_id
    if reason is not None:
        payload["reason"] = reason

    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": _to_utc_iso(event_time),
        "producer": producer,
        "schema_version": schema_version,
        "payload": payload,
    }


def validate_procurement_event_dict(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    event_type = event.get("event_type")
    payload = event.get("payload") or {}

    for field in ("event_id", "event_type", "event_time", "producer", "schema_version"):
        if not event.get(field):
            errors.append(f"missing_{field}")

    schema_version = event.get("schema_version")
    if schema_version is not None and int(schema_version) != PROCUREMENT_SCHEMA_VERSION:
        errors.append("unsupported_schema_version")

    if event_type not in PROCUREMENT_EVENT_TYPES:
        errors.append("unsupported_event_type")
        return errors

    rule = RULES[event_type]
    for field in rule.required_payload_fields:
        if not payload.get(field):
            errors.append(f"missing_payload_{field}")

    if event_type == "PO_CREATED":
        qty = payload.get("ordered_quantity")
        if qty is not None and int(qty) <= 0:
            errors.append("ordered_quantity_must_be_positive")
        trigger = payload.get("trigger_reason")
        if trigger and trigger not in VALID_TRIGGER_REASONS:
            errors.append("invalid_trigger_reason")

    if event_type == "PO_RECEIVED":
        qty = payload.get("received_quantity")
        if qty is not None and int(qty) <= 0:
            errors.append("received_quantity_must_be_positive")

    return errors


def generate_procurement_sample_events(
    *,
    seed: str,
    start_time: datetime,
    product_id: str,
    supplier_id: str,
    warehouse_id: str,
    count: int,
) -> Iterable[dict[str, Any]]:
    """
    Generate a realistic PO lifecycle sequence:
      PO_CREATED → PO_APPROVED → PO_RECEIVED  (happy path, repeats)
      PO_CREATED → PO_CANCELLED               (every 5th PO)
    """
    po_index = 0
    event_index = 0
    event_time = start_time
    produced = 0

    while produced < count:
        po_id = deterministic_uuid(seed, f"po:{po_index}")
        cancelled = (po_index % 5 == 4)  # every 5th PO is cancelled

        # PO_CREATED
        if produced < count:
            yield build_procurement_event(
                event_id=deterministic_uuid(seed, f"proc:created:{po_index}:{event_index}"),
                event_type="PO_CREATED",
                event_time=event_time,
                producer="procurement_service",
                po_id=po_id,
                product_id=product_id,
                ordered_quantity=100 + po_index * 10,
                supplier_id=supplier_id,
                trigger_reason="MANUAL" if cancelled else "AUTO_REORDER",
            )
            produced += 1
            event_index += 1
            event_time = event_time + timedelta(minutes=10)

        if cancelled:
            # PO_CANCELLED path
            if produced < count:
                yield build_procurement_event(
                    event_id=deterministic_uuid(seed, f"proc:cancelled:{po_index}:{event_index}"),
                    event_type="PO_CANCELLED",
                    event_time=event_time,
                    producer="procurement_service",
                    po_id=po_id,
                    reason="SUPPLIER_UNAVAILABLE",
                )
                produced += 1
                event_index += 1
                event_time = event_time + timedelta(minutes=5)
        else:
            # PO_APPROVED path
            if produced < count:
                yield build_procurement_event(
                    event_id=deterministic_uuid(seed, f"proc:approved:{po_index}:{event_index}"),
                    event_type="PO_APPROVED",
                    event_time=event_time,
                    producer="procurement_service",
                    po_id=po_id,
                    approved_by="manager_01",
                )
                produced += 1
                event_index += 1
                event_time = event_time + timedelta(hours=2)

            # PO_RECEIVED
            if produced < count:
                yield build_procurement_event(
                    event_id=deterministic_uuid(seed, f"proc:received:{po_index}:{event_index}"),
                    event_type="PO_RECEIVED",
                    event_time=event_time,
                    producer="procurement_service",
                    po_id=po_id,
                    product_id=product_id,
                    received_quantity=100 + po_index * 10,
                    warehouse_id=warehouse_id,
                )
                produced += 1
                event_index += 1
                event_time = event_time + timedelta(days=po_index % 3 + 1)

        po_index += 1

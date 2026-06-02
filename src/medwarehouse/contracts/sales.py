from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

from medwarehouse.contracts._utils import deterministic_uuid, to_utc_iso as _to_utc_iso


SALES_SCHEMA_VERSION = 1
SALES_EVENT_TYPES = frozenset({"SALE_CREATED", "SALE_CANCELLED"})
VALID_CUSTOMER_TYPES = frozenset({"RETAIL", "HOSPITAL", "DISTRIBUTOR"})
VALID_CURRENCIES = frozenset({"INR", "USD", "EUR", "GBP"})


@dataclass(frozen=True)
class SalesRule:
    required_payload_fields: tuple[str, ...]


RULES: dict[str, SalesRule] = {
    "SALE_CREATED": SalesRule(
        required_payload_fields=(
            "sale_id", "product_id", "warehouse_id",
            "quantity", "unit_price", "currency", "customer_type",
        ),
    ),
    "SALE_CANCELLED": SalesRule(
        required_payload_fields=(
            "sale_id", "product_id", "warehouse_id", "quantity", "original_event_id",
        ),
    ),
}




def build_sales_event(
    *,
    event_id: str,
    event_type: str,
    event_time: datetime,
    producer: str,
    sale_id: str,
    product_id: str,
    warehouse_id: str,
    quantity: int,
    unit_price: float | None = None,
    currency: str | None = None,
    customer_type: str | None = None,
    original_event_id: str | None = None,
    schema_version: int = SALES_SCHEMA_VERSION,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sale_id": sale_id,
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "quantity": quantity,
    }
    if unit_price is not None:
        payload["unit_price"] = unit_price
    if currency is not None:
        payload["currency"] = currency
    if customer_type is not None:
        payload["customer_type"] = customer_type
    if original_event_id is not None:
        payload["original_event_id"] = original_event_id

    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": _to_utc_iso(event_time),
        "producer": producer,
        "schema_version": schema_version,
        "payload": payload,
    }


def validate_sales_event_dict(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    event_type = event.get("event_type")
    payload = event.get("payload") or {}

    for field in ("event_id", "event_type", "event_time", "producer", "schema_version"):
        if not event.get(field):
            errors.append(f"missing_{field}")

    schema_version = event.get("schema_version")
    if schema_version is not None and int(schema_version) != SALES_SCHEMA_VERSION:
        errors.append("unsupported_schema_version")

    if event_type not in SALES_EVENT_TYPES:
        errors.append("unsupported_event_type")
        return errors

    rule = RULES[event_type]
    for field in rule.required_payload_fields:
        if not payload.get(field):
            errors.append(f"missing_payload_{field}")

    qty = payload.get("quantity")
    if qty is not None and int(qty) <= 0:
        errors.append("quantity_must_be_positive")

    if event_type == "SALE_CREATED":
        price = payload.get("unit_price")
        if price is not None and float(price) <= 0:
            errors.append("unit_price_must_be_positive")
        ctype = payload.get("customer_type")
        if ctype and ctype not in VALID_CUSTOMER_TYPES:
            errors.append("invalid_customer_type")
        currency = payload.get("currency")
        if currency and currency not in VALID_CURRENCIES:
            errors.append("invalid_currency")

    return errors


# Realistic price tiers per customer type
_PRICES = {
    "RETAIL":      120.50,
    "HOSPITAL":    105.00,
    "DISTRIBUTOR":  95.00,
}
_CUSTOMER_TYPES = ["RETAIL", "RETAIL", "RETAIL", "HOSPITAL", "DISTRIBUTOR"]  # weighted


def generate_sales_sample_events(
    *,
    seed: str,
    start_time: datetime,
    product_id: str,
    warehouse_id: str,
    count: int,
    currency: str = "INR",
) -> Iterable[dict[str, Any]]:
    """
    Generate realistic SALE_CREATED events with occasional SALE_CANCELLED (1 in 8).
    Each cancellation references its original SALE_CREATED event_id.
    """
    event_time = start_time
    sale_history: list[tuple[str, str]] = []  # (sale_id, event_id)

    for index in range(count):
        customer_type = _CUSTOMER_TYPES[index % len(_CUSTOMER_TYPES)]
        unit_price = _PRICES[customer_type]
        sale_id = deterministic_uuid(seed, f"sale:{index}")
        event_id = deterministic_uuid(seed, f"sales:created:{index}")

        sale_history.append((sale_id, event_id))

        yield build_sales_event(
            event_id=event_id,
            event_type="SALE_CREATED",
            event_time=event_time,
            producer="sales_service",
            sale_id=sale_id,
            product_id=product_id,
            warehouse_id=warehouse_id,
            quantity=2 + (index % 5),
            unit_price=unit_price,
            currency=currency,
            customer_type=customer_type,
        )
        event_time = event_time + timedelta(minutes=15 + index % 30)

        # Generate a cancellation for every 8th sale
        if index > 0 and index % 8 == 0:
            orig_sale_id, orig_event_id = sale_history[index - 3]
            cancel_event_id = deterministic_uuid(seed, f"sales:cancelled:{index}")
            yield build_sales_event(
                event_id=cancel_event_id,
                event_type="SALE_CANCELLED",
                event_time=event_time,
                producer="sales_service",
                sale_id=orig_sale_id,
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity=2 + ((index - 3) % 5),
                original_event_id=orig_event_id,
            )
            event_time = event_time + timedelta(minutes=5)

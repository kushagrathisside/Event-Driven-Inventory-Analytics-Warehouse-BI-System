"""
Contract tests for procurement and sales domains.
Mirrors the pattern from test_contracts.py (inventory).
"""
import unittest
from datetime import datetime, timezone

from medwarehouse.contracts.procurement import (
    generate_procurement_sample_events,
    validate_procurement_event_dict,
    build_procurement_event,
)
from medwarehouse.contracts.sales import (
    generate_sales_sample_events,
    validate_sales_event_dict,
    build_sales_event,
)


class ProcurementContractTests(unittest.TestCase):
    def test_generated_sample_events_are_all_valid(self) -> None:
        events = list(
            generate_procurement_sample_events(
                seed="unit-test",
                start_time=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
                product_id="P-001",
                supplier_id="SUP-001",
                warehouse_id="W-001",
                count=12,
            )
        )
        self.assertGreater(len(events), 0)
        unique_ids = {e["event_id"] for e in events}
        self.assertEqual(len(unique_ids), len(events), "Duplicate event_ids generated")
        for event in events:
            errors = validate_procurement_event_dict(event)
            self.assertEqual(errors, [], f"Event {event['event_type']} failed: {errors}")

    def test_all_four_event_types_are_produced(self) -> None:
        events = list(
            generate_procurement_sample_events(
                seed="types-test",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                product_id="P-001",
                supplier_id="SUP-001",
                warehouse_id="W-001",
                count=20,
            )
        )
        types_produced = {e["event_type"] for e in events}
        self.assertIn("PO_CREATED", types_produced)
        self.assertIn("PO_APPROVED", types_produced)
        self.assertIn("PO_RECEIVED", types_produced)
        self.assertIn("PO_CANCELLED", types_produced)

    def test_po_created_zero_quantity_is_rejected(self) -> None:
        event = build_procurement_event(
            event_id="evt-1",
            event_type="PO_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            po_id="PO-001",
            product_id="P-001",
            ordered_quantity=0,
            supplier_id="SUP-001",
            trigger_reason="AUTO_REORDER",
        )
        errors = validate_procurement_event_dict(event)
        self.assertIn("ordered_quantity_must_be_positive", errors)

    def test_po_created_invalid_trigger_reason_is_rejected(self) -> None:
        event = build_procurement_event(
            event_id="evt-2",
            event_type="PO_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            po_id="PO-002",
            product_id="P-001",
            ordered_quantity=50,
            supplier_id="SUP-001",
            trigger_reason="UNKNOWN_REASON",
        )
        errors = validate_procurement_event_dict(event)
        self.assertIn("invalid_trigger_reason", errors)

    def test_po_approved_missing_approved_by_is_rejected(self) -> None:
        event = {
            "event_id": "evt-3",
            "event_type": "PO_APPROVED",
            "event_time": "2026-01-01T09:00:00+00:00",
            "producer": "test",
            "schema_version": 1,
            "payload": {"po_id": "PO-003"},  # missing approved_by
        }
        errors = validate_procurement_event_dict(event)
        self.assertIn("missing_payload_approved_by", errors)

    def test_po_received_zero_received_quantity_is_rejected(self) -> None:
        event = build_procurement_event(
            event_id="evt-4",
            event_type="PO_RECEIVED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            po_id="PO-004",
            product_id="P-001",
            received_quantity=0,
            warehouse_id="W-001",
        )
        errors = validate_procurement_event_dict(event)
        self.assertIn("received_quantity_must_be_positive", errors)

    def test_unsupported_event_type_is_rejected(self) -> None:
        event = {
            "event_id": "evt-5",
            "event_type": "PO_MODIFIED",
            "event_time": "2026-01-01T09:00:00+00:00",
            "producer": "test",
            "schema_version": 1,
            "payload": {"po_id": "PO-005"},
        }
        errors = validate_procurement_event_dict(event)
        self.assertIn("unsupported_event_type", errors)

    def test_wrong_schema_version_is_rejected(self) -> None:
        event = build_procurement_event(
            event_id="evt-6",
            event_type="PO_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            po_id="PO-006",
            product_id="P-001",
            ordered_quantity=50,
            supplier_id="SUP-001",
            trigger_reason="AUTO_REORDER",
            schema_version=99,
        )
        errors = validate_procurement_event_dict(event)
        self.assertIn("unsupported_schema_version", errors)


class SalesContractTests(unittest.TestCase):
    def test_generated_sample_events_are_all_valid(self) -> None:
        events = list(
            generate_sales_sample_events(
                seed="unit-test",
                start_time=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
                product_id="P-001",
                warehouse_id="W-001",
                count=20,
            )
        )
        self.assertGreater(len(events), 0)
        unique_ids = {e["event_id"] for e in events}
        self.assertEqual(len(unique_ids), len(events), "Duplicate event_ids generated")
        for event in events:
            errors = validate_sales_event_dict(event)
            self.assertEqual(errors, [], f"Event {event['event_type']} failed: {errors}")

    def test_both_event_types_are_produced(self) -> None:
        events = list(
            generate_sales_sample_events(
                seed="types-test",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                product_id="P-001",
                warehouse_id="W-001",
                count=25,
            )
        )
        types = {e["event_type"] for e in events}
        self.assertIn("SALE_CREATED", types)
        self.assertIn("SALE_CANCELLED", types)

    def test_all_three_customer_types_are_produced(self) -> None:
        events = list(
            generate_sales_sample_events(
                seed="customer-types-test",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                product_id="P-001",
                warehouse_id="W-001",
                count=20,
            )
        )
        customer_types = {
            e["payload"]["customer_type"]
            for e in events
            if e["event_type"] == "SALE_CREATED"
        }
        self.assertIn("RETAIL", customer_types)
        self.assertIn("HOSPITAL", customer_types)
        self.assertIn("DISTRIBUTOR", customer_types)

    def test_sale_created_zero_quantity_is_rejected(self) -> None:
        event = build_sales_event(
            event_id="evt-1",
            event_type="SALE_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            sale_id="SALE-001",
            product_id="P-001",
            warehouse_id="W-001",
            quantity=0,
            unit_price=100.0,
            currency="INR",
            customer_type="RETAIL",
        )
        errors = validate_sales_event_dict(event)
        self.assertIn("quantity_must_be_positive", errors)

    def test_sale_created_invalid_customer_type_is_rejected(self) -> None:
        event = build_sales_event(
            event_id="evt-2",
            event_type="SALE_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            sale_id="SALE-002",
            product_id="P-001",
            warehouse_id="W-001",
            quantity=5,
            unit_price=100.0,
            currency="INR",
            customer_type="WHOLESALE",  # not a valid type
        )
        errors = validate_sales_event_dict(event)
        self.assertIn("invalid_customer_type", errors)

    def test_sale_created_negative_price_is_rejected(self) -> None:
        event = build_sales_event(
            event_id="evt-3",
            event_type="SALE_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            sale_id="SALE-003",
            product_id="P-001",
            warehouse_id="W-001",
            quantity=5,
            unit_price=-10.0,
            currency="INR",
            customer_type="RETAIL",
        )
        errors = validate_sales_event_dict(event)
        self.assertIn("unit_price_must_be_positive", errors)

    def test_sale_cancelled_missing_original_event_id_is_rejected(self) -> None:
        event = {
            "event_id": "evt-4",
            "event_type": "SALE_CANCELLED",
            "event_time": "2026-01-01T10:00:00+00:00",
            "producer": "test",
            "schema_version": 1,
            "payload": {
                "sale_id": "SALE-004",
                "product_id": "P-001",
                "warehouse_id": "W-001",
                "quantity": 5,
                # missing original_event_id
            },
        }
        errors = validate_sales_event_dict(event)
        self.assertIn("missing_payload_original_event_id", errors)

    def test_invalid_currency_is_rejected(self) -> None:
        event = build_sales_event(
            event_id="evt-5",
            event_type="SALE_CREATED",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            producer="test",
            sale_id="SALE-005",
            product_id="P-001",
            warehouse_id="W-001",
            quantity=3,
            unit_price=100.0,
            currency="BTC",
            customer_type="RETAIL",
        )
        errors = validate_sales_event_dict(event)
        self.assertIn("invalid_currency", errors)

    def test_sale_cancelled_references_original_event_id(self) -> None:
        events = list(
            generate_sales_sample_events(
                seed="cancel-ref-test",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                product_id="P-001",
                warehouse_id="W-001",
                count=25,
            )
        )
        created_ids = {
            e["event_id"]
            for e in events
            if e["event_type"] == "SALE_CREATED"
        }
        for event in events:
            if event["event_type"] == "SALE_CANCELLED":
                orig = event["payload"].get("original_event_id")
                self.assertIn(orig, created_ids,
                              "SALE_CANCELLED references an event_id not in SALE_CREATED set")

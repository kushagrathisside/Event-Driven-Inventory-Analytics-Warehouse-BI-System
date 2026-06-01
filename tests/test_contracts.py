import unittest
from datetime import datetime, timezone

from medwarehouse.contracts.inventory import (
    generate_inventory_sample_events,
    validate_inventory_event_dict,
)


class InventoryContractTests(unittest.TestCase):
    def test_generated_sample_events_are_valid(self) -> None:
        events = list(
            generate_inventory_sample_events(
                seed="unit-test",
                start_time=datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc),
                product_id="P-001",
                warehouse_id="W-001",
                supplier_id="SUP-001",
                batch_number="BATCH-001",
                expiry_date="2027-12-31",
                count=5,
            )
        )

        self.assertEqual(len(events), 5)
        self.assertEqual(len({event["event_id"] for event in events}), 5)
        for event in events:
            self.assertEqual(validate_inventory_event_dict(event), [])

    def test_invalid_quantity_sign_is_rejected(self) -> None:
        event = {
            "event_id": "evt-1",
            "event_type": "STOCK_RECEIVED",
            "event_time": "2026-01-01T00:00:00+00:00",
            "producer": "inventory_service",
            "schema_version": 1,
            "payload": {
                "product_id": "P-001",
                "warehouse_id": "W-001",
                "batch_number": "B-001",
                "expiry_date": "2027-01-01",
                "quantity_delta": -1,
            },
        }

        self.assertIn("invalid_quantity_sign", validate_inventory_event_dict(event))

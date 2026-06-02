"""
Unit tests for medwarehouse_webapp service layer and supporting infrastructure.

All tests mock the database and Kafka layers so they run without a live stack.
"""
from __future__ import annotations

import os
import time
import threading
import unittest
from unittest.mock import MagicMock, call, patch, PropertyMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(rows=None, description=None):
    """Build a mock psycopg2 cursor suitable for use in a with-statement."""
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows or []
    cur.description = description or []
    cur.rowcount = 1
    return cur


def _make_conn(cursor=None):
    """Build a mock psycopg2 connection suitable for use in a with-statement."""
    conn = MagicMock()
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    if cursor:
        conn.cursor = MagicMock(return_value=cursor)
    return conn


# ---------------------------------------------------------------------------
# stock_service tests
# ---------------------------------------------------------------------------

class TestRecordStockReceived(unittest.TestCase):
    def setUp(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_returns_accepted_dict(self):
        cur = _make_cursor()
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.stock_service.connect", return_value=conn), \
             patch("medwarehouse_webapp.services.stock_service.emit_events"):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_received
            result = record_stock_received(
                product_id="P-001",
                warehouse_id="W-001",
                batch_number="BATCH-001",
                expiry_date="2027-12-31",
                quantity=100,
                supplier_id="SUP-001",
            )

        self.assertEqual(result["event_type"], "STOCK_RECEIVED")
        self.assertEqual(result["status"], "accepted")
        self.assertIn("event_id", result)

    def test_no_uuid_cast_in_sql(self):
        """SQL must not contain ::uuid so it works with non-UUID text IDs."""
        cur = _make_cursor()
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.stock_service.connect", return_value=conn), \
             patch("medwarehouse_webapp.services.stock_service.emit_events"):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_received
            record_stock_received(
                product_id="P-LOCAL-CALPOL-500",
                warehouse_id="W-LOCAL-RECEIVING-01",
                batch_number="BATCH-LOCAL-001",
                expiry_date="2027-12-31",
                quantity=50,
            )

        # Inspect the actual SQL that was passed to cur.execute
        executed_sql = cur.execute.call_args[0][0]
        self.assertNotIn("::uuid", executed_sql)

    def test_negative_quantity_raises_value_error(self):
        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_received
            with self.assertRaises(ValueError):
                record_stock_received(
                    product_id="P-001",
                    warehouse_id="W-001",
                    batch_number="B-001",
                    expiry_date="2027-12-31",
                    quantity=-5,  # must be positive for STOCK_RECEIVED
                )

    def test_emits_kafka_event(self):
        cur = _make_cursor()
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.stock_service.connect", return_value=conn), \
             patch("medwarehouse_webapp.services.stock_service.emit_events") as mock_emit:
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_received
            record_stock_received(
                product_id="P-001",
                warehouse_id="W-001",
                batch_number="B-001",
                expiry_date="2027-12-31",
                quantity=10,
            )

        mock_emit.assert_called_once()
        call_kwargs = mock_emit.call_args[1]
        self.assertEqual(len(call_kwargs["events"]), 1)
        self.assertEqual(call_kwargs["events"][0]["event_type"], "STOCK_RECEIVED")


class TestRecordStockSold(unittest.TestCase):
    def setUp(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_raises_on_insufficient_stock(self):
        cur = _make_cursor()
        cur.rowcount = 0  # simulate no matching row
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.stock_service.connect", return_value=conn):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_sold
            with self.assertRaises(ValueError, msg="Should raise ValueError on rowcount=0"):
                record_stock_sold(
                    product_id="P-001",
                    warehouse_id="W-001",
                    batch_number="B-001",
                    expiry_date="2027-12-31",
                    quantity=5,
                    sale_id="SALE-001",
                )

    def test_success_returns_accepted(self):
        cur = _make_cursor()
        cur.rowcount = 1
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.stock_service.connect", return_value=conn), \
             patch("medwarehouse_webapp.services.stock_service.emit_events"):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.stock_service import record_stock_sold
            result = record_stock_sold(
                product_id="P-001",
                warehouse_id="W-001",
                batch_number="B-001",
                expiry_date="2027-12-31",
                quantity=5,
                sale_id="SALE-001",
            )

        self.assertEqual(result["event_type"], "STOCK_SOLD")
        self.assertEqual(result["status"], "accepted")


# ---------------------------------------------------------------------------
# order_service tests
# ---------------------------------------------------------------------------

class TestApproveOrder(unittest.TestCase):
    def test_approve_updates_to_approved(self):
        cur = _make_cursor(rows=[("po-123",)])
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.order_service.connect", return_value=conn):
            from medwarehouse_webapp.services.order_service import approve_order
            result = approve_order("po-123", "operator@example.com")

        self.assertEqual(result["status"], "approved")
        self.assertEqual(result["po_id"], "po-123")
        sql = cur.execute.call_args[0][0]
        self.assertIn("APPROVED", sql)
        self.assertIn("DRAFT", sql)

    def test_approve_not_found_returns_error_status(self):
        cur = _make_cursor(rows=[])
        cur.fetchone.return_value = None
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.order_service.connect", return_value=conn):
            from medwarehouse_webapp.services.order_service import approve_order
            result = approve_order("nonexistent", "operator")

        self.assertIn("not_found", result["status"])


class TestCancelOrder(unittest.TestCase):
    def test_cancel_updates_to_cancelled(self):
        cur = _make_cursor(rows=[("po-456",)])
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.order_service.connect", return_value=conn):
            from medwarehouse_webapp.services.order_service import cancel_order
            result = cancel_order("po-456")

        self.assertEqual(result["status"], "cancelled")
        sql = cur.execute.call_args[0][0]
        self.assertIn("CANCELLED", sql)

    def test_cancel_not_found_returns_error_status(self):
        cur = _make_cursor(rows=[])
        cur.fetchone.return_value = None
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.order_service.connect", return_value=conn):
            from medwarehouse_webapp.services.order_service import cancel_order
            result = cancel_order("nonexistent")

        self.assertIn("not_found", result["status"])


class TestReceiveOrder(unittest.TestCase):
    def setUp(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_receive_emits_stock_received_event(self):
        cur = _make_cursor(rows=[("P-001", "W-001", "SUP-001", 50)])
        conn = _make_conn(cur)

        with patch.dict(os.environ, {"MW_ENV": "local", "MW_PRODUCER_DRY_RUN": "true"}), \
             patch("medwarehouse_webapp.services.order_service.connect", return_value=conn), \
             patch("medwarehouse_webapp.services.order_service.emit_events") as mock_emit:
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.services.order_service import receive_order
            result = receive_order("po-789", "operator")

        self.assertEqual(result["status"], "received")
        mock_emit.assert_called_once()
        event = mock_emit.call_args[1]["events"][0]
        self.assertEqual(event["event_type"], "STOCK_RECEIVED")
        self.assertIn("stock_event_id", result)


# ---------------------------------------------------------------------------
# reorder_service tests
# ---------------------------------------------------------------------------

class TestUpdateReorderPolicy(unittest.TestCase):
    def test_no_changes_returns_early(self):
        with patch("medwarehouse_webapp.services.reorder_service.connect") as mock_connect:
            from medwarehouse_webapp.services.reorder_service import update_reorder_policy
            result = update_reorder_policy(1, unknown_field="ignored")

        self.assertEqual(result["status"], "no_changes")
        mock_connect.assert_not_called()

    def test_valid_fields_build_correct_sql(self):
        cur = _make_cursor()
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.reorder_service.connect", return_value=conn):
            from medwarehouse_webapp.services.reorder_service import update_reorder_policy
            result = update_reorder_policy(5, reorder_point=100, reorder_quantity=200)

        self.assertEqual(result["status"], "updated")
        sql = cur.execute.call_args[0][0]
        self.assertIn("reorder_point = %s", sql)
        self.assertIn("reorder_quantity = %s", sql)
        self.assertIn("updated_at = timezone('UTC', now())", sql)
        # Values: [100, 200, 5] — no spurious string from the old dict-manipulation approach
        values = cur.execute.call_args[0][1]
        self.assertEqual(values[-1], 5)  # policy_id is last
        self.assertNotIn("timezone('UTC', now())", values)  # must not be a param value

    def test_only_allowed_fields_are_used(self):
        cur = _make_cursor()
        conn = _make_conn(cur)

        with patch("medwarehouse_webapp.services.reorder_service.connect", return_value=conn):
            from medwarehouse_webapp.services.reorder_service import update_reorder_policy
            update_reorder_policy(1, reorder_point=50, injected_field="hack")

        sql = cur.execute.call_args[0][0]
        self.assertNotIn("injected_field", sql)


# ---------------------------------------------------------------------------
# IdempotencyStore tests
# ---------------------------------------------------------------------------

class TestIdempotencyStore(unittest.TestCase):
    def test_set_and_get_returns_same_value(self):
        from medwarehouse_webapp.services.idempotency import IdempotencyStore
        store = IdempotencyStore(ttl_seconds=60, eviction_interval=3600)
        store.set("key1", {"result": "ok"})
        self.assertEqual(store.get("key1"), {"result": "ok"})

    def test_expired_entry_returns_none(self):
        from medwarehouse_webapp.services.idempotency import IdempotencyStore
        store = IdempotencyStore(ttl_seconds=0.01, eviction_interval=3600)
        store.set("key1", {"result": "ok"})
        time.sleep(0.05)
        self.assertIsNone(store.get("key1"))

    def test_evict_expired_removes_stale_entries(self):
        from medwarehouse_webapp.services.idempotency import IdempotencyStore
        store = IdempotencyStore(ttl_seconds=0.01, eviction_interval=3600)
        store.set("stale", "old_value")
        store.set("fresh", "new_value")
        time.sleep(0.05)
        store.set("fresh", "new_value")  # re-set to keep fresh
        removed = store.evict_expired()
        self.assertGreaterEqual(removed, 1)
        self.assertIsNone(store.get("stale"))

    def test_get_missing_key_returns_none(self):
        from medwarehouse_webapp.services.idempotency import IdempotencyStore
        store = IdempotencyStore(ttl_seconds=60, eviction_interval=3600)
        self.assertIsNone(store.get("does-not-exist"))

    def test_eviction_thread_is_daemon(self):
        from medwarehouse_webapp.services.idempotency import IdempotencyStore
        store = IdempotencyStore(ttl_seconds=60, eviction_interval=3600)
        self.assertTrue(store._eviction_thread.daemon)
        self.assertTrue(store._eviction_thread.is_alive())


# ---------------------------------------------------------------------------
# TTLCache stampede-protection tests
# ---------------------------------------------------------------------------

class TestTTLCacheStampede(unittest.TestCase):
    def test_only_one_builder_runs_on_concurrent_miss(self):
        from medwarehouse.platform.cache.ttl import TTLCache

        build_count = 0
        build_lock = threading.Event()

        def slow_builder():
            nonlocal build_count
            build_count += 1
            time.sleep(0.05)
            return "computed"

        cache = TTLCache(ttl_seconds=60.0)
        results = []
        errors = []

        def worker():
            try:
                val, _ = cache.get_or_create("k", slow_builder)
                results.append(val)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        self.assertEqual(errors, [], f"Unexpected exceptions: {errors}")
        self.assertEqual(build_count, 1, "Builder must run exactly once per concurrent miss")
        self.assertTrue(all(v == "computed" for v in results))

    def test_invalidate_all_clears_cache(self):
        from medwarehouse.platform.cache.ttl import TTLCache

        cache = TTLCache(ttl_seconds=60.0)
        cache.get_or_create("k", lambda: "v1")
        cache.invalidate()
        val, from_cache = cache.get_or_create("k", lambda: "v2")
        self.assertFalse(from_cache)
        self.assertEqual(val, "v2")

    def test_force_refresh_bypasses_valid_cache(self):
        from medwarehouse.platform.cache.ttl import TTLCache

        call_count = 0

        def counting_builder():
            nonlocal call_count
            call_count += 1
            return call_count

        cache = TTLCache(ttl_seconds=60.0)
        cache.get_or_create("k", counting_builder)
        _, from_cache = cache.get_or_create("k", counting_builder, force_refresh=True)
        self.assertFalse(from_cache)
        self.assertEqual(call_count, 2)


if __name__ == "__main__":
    unittest.main()

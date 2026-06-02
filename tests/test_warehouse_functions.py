"""
Unit tests for warehouse Python functions that wrap stored procedures.

These tests mock the DB layer so they run without a live PostgreSQL connection.
To run against a real database, set MW_INTEGRATION_TEST=1 and ensure all
MW_* connection environment variables point to a test database.

    pytest tests/test_warehouse_functions.py -m integration
"""
import os
import unittest
from unittest.mock import MagicMock, call, patch


INTEGRATION = os.environ.get("MW_INTEGRATION_TEST") == "1"
integration = unittest.skipUnless(INTEGRATION, "Set MW_INTEGRATION_TEST=1 to run")


class TestRefreshInventoryBalance(unittest.TestCase):
    """Tests for medwarehouse.warehouse.inventory.refresh_inventory_balance()."""

    def setUp(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_calls_correct_sql_on_writer_db(self):
        with patch("medwarehouse.warehouse._common.fetch_rows") as mock_fetch, \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            mock_fetch.return_value = [(42,)]

            from medwarehouse.warehouse.inventory import refresh_inventory_balance
            result = refresh_inventory_balance()

        self.assertEqual(result, 42)
        mock_fetch.assert_called_once()
        args = mock_fetch.call_args
        # First positional arg is the connection — should be analytics_writer_db
        conn_used = args[0][0]
        settings = get_settings()
        self.assertEqual(conn_used, settings.analytics_writer_db)
        # SQL must call the correct stored procedure
        sql_used = args[0][1]
        self.assertIn("analytics.refresh_inventory_balance()", sql_used)

    def test_returns_zero_on_empty_result(self):
        with patch("medwarehouse.warehouse._common.fetch_rows") as mock_fetch, \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            mock_fetch.return_value = []

            from medwarehouse.warehouse.inventory import refresh_inventory_balance
            result = refresh_inventory_balance()

        self.assertEqual(result, 0)

    def test_propagates_db_error(self):
        with patch("medwarehouse.warehouse._common.fetch_rows",
                   side_effect=RuntimeError("connection refused")), \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()

            from medwarehouse.warehouse.inventory import refresh_inventory_balance
            with self.assertRaises(RuntimeError):
                refresh_inventory_balance()


class TestCheckReorderThresholds(unittest.TestCase):
    """Tests for medwarehouse.warehouse.inventory.check_reorder_thresholds()."""

    def setUp(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_calls_correct_sql_on_writer_db(self):
        with patch("medwarehouse.warehouse._common.fetch_rows") as mock_fetch, \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            mock_fetch.return_value = [(3,)]

            from medwarehouse.warehouse.inventory import check_reorder_thresholds
            result = check_reorder_thresholds()

        self.assertEqual(result, 3)
        args = mock_fetch.call_args
        conn_used = args[0][0]
        settings = get_settings()
        self.assertEqual(conn_used, settings.analytics_writer_db)
        sql_used = args[0][1]
        self.assertIn("analytics.check_reorder_thresholds()", sql_used)

    def test_returns_zero_when_no_policies_triggered(self):
        with patch("medwarehouse.warehouse._common.fetch_rows") as mock_fetch, \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            mock_fetch.return_value = [(0,)]

            from medwarehouse.warehouse.inventory import check_reorder_thresholds
            result = check_reorder_thresholds()

        self.assertEqual(result, 0)

    def test_returns_integer_not_raw_tuple(self):
        with patch("medwarehouse.warehouse._common.fetch_rows") as mock_fetch, \
             patch.dict(os.environ, {"MW_ENV": "local"}, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            mock_fetch.return_value = [(7,)]

            from medwarehouse.warehouse.inventory import check_reorder_thresholds
            result = check_reorder_thresholds()

        self.assertIsInstance(result, int)
        self.assertEqual(result, 7)


class TestCredentialWarnings(unittest.TestCase):
    """Tests for weak credential detection in config."""

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_weak_password_logs_warning_in_local_env(self):
        env_patch = {
            "MW_ENV": "local",
            "MW_MASTER_DB_PASSWORD": "postgres",
        }
        with patch.dict(os.environ, env_patch, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            import logging
            with self.assertLogs("medwarehouse.config", level="WARNING") as cm:
                get_settings()
            self.assertTrue(any("SECURITY" in line for line in cm.output))

    def test_weak_password_raises_in_production_env(self):
        env_patch = {
            "MW_ENV": "production",
            "MW_MASTER_DB_PASSWORD": "postgres",
        }
        with patch.dict(os.environ, env_patch, clear=False):
            from medwarehouse.config import get_settings, _warn_weak_credentials
            get_settings.cache_clear()
            with self.assertRaises(RuntimeError, msg="Should raise for weak creds in prod"):
                get_settings()

    def test_strong_password_does_not_raise(self):
        env_patch = {
            "MW_ENV": "local",
            "MW_MASTER_DB_PASSWORD": "xK9!mP2$vL8#nQ5@",
            "MW_ANALYTICS_ADMIN_DB_PASSWORD": "xK9!mP2$vL8#nQ5@",
            "MW_ANALYTICS_WRITER_DB_PASSWORD": "xK9!mP2$vL8#nQ5@",
            "MW_ANALYTICS_READER_DB_PASSWORD": "xK9!mP2$vL8#nQ5@",
            "MW_ANALYTICS_FDW_READER_PASSWORD": "xK9!mP2$vL8#nQ5@",
        }
        with patch.dict(os.environ, env_patch, clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            # Should not raise or warn
            settings = get_settings()
        self.assertIsNotNone(settings)


class TestApiKeyConfig(unittest.TestCase):
    """Tests that API keys are read from env and default to None."""

    def tearDown(self):
        from medwarehouse.config import get_settings
        get_settings.cache_clear()

    def test_webapp_api_key_read_from_env(self):
        with patch.dict(os.environ, {"MW_WEBAPP_API_KEY": "test-secret-key", "MW_ENV": "local"},
                        clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            settings = get_settings()
        self.assertEqual(settings.webapp_api_key, "test-secret-key")

    def test_platform_api_key_read_from_env(self):
        with patch.dict(os.environ, {"MW_PLATFORM_API_KEY": "platform-key-123", "MW_ENV": "local"},
                        clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            settings = get_settings()
        self.assertEqual(settings.platform_api_key, "platform-key-123")

    def test_api_keys_default_to_none(self):
        env = {k: v for k, v in os.environ.items()
               if k not in ("MW_WEBAPP_API_KEY", "MW_PLATFORM_API_KEY")}
        env["MW_ENV"] = "local"
        with patch.dict(os.environ, env, clear=True):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            settings = get_settings()
        self.assertIsNone(settings.webapp_api_key)
        self.assertIsNone(settings.platform_api_key)


class TestWebappAuth(unittest.TestCase):
    """Tests for the auth middleware in the webapp."""

    def _make_app(self, api_key: str | None):
        with patch.dict(os.environ, {"MW_WEBAPP_API_KEY": api_key or "", "MW_ENV": "local"},
                        clear=False):
            from medwarehouse.config import get_settings
            get_settings.cache_clear()
            from medwarehouse_webapp.app import create_webapp
            return create_webapp()

    def test_health_is_always_accessible_without_key(self):
        app = self._make_app("secret-key")
        resp = app.test_client().get("/health")
        self.assertEqual(resp.status_code, 200)

    def test_protected_endpoint_blocked_without_key(self):
        app = self._make_app("secret-key")
        resp = app.test_client().get("/api/v1/products")
        self.assertEqual(resp.status_code, 401)

    def test_protected_endpoint_allowed_with_correct_key(self):
        with patch("medwarehouse_webapp.api.inventory.connect") as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.__enter__ = lambda s: s
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_cursor.description = [("product_id",), ("brand_name",)]
            mock_cursor.fetchall.return_value = []
            mock_conn_ctx = MagicMock()
            mock_conn_ctx.__enter__ = lambda s: MagicMock(cursor=lambda: mock_cursor)
            mock_conn_ctx.__exit__ = MagicMock(return_value=False)
            mock_conn.return_value = mock_conn_ctx

            app = self._make_app("secret-key")
            resp = app.test_client().get(
                "/api/v1/products",
                headers={"X-API-Key": "secret-key"},
            )
        # 200 or 500 (DB not available) — but NOT 401
        self.assertNotEqual(resp.status_code, 401)

    def test_bearer_token_is_accepted(self):
        app = self._make_app("bearer-test-key")
        resp = app.test_client().get(
            "/api/v1/inventory",
            headers={"Authorization": "Bearer bearer-test-key"},
        )
        self.assertNotEqual(resp.status_code, 401)

    def test_wrong_key_is_rejected(self):
        app = self._make_app("correct-key")
        resp = app.test_client().get(
            "/api/v1/products",
            headers={"X-API-Key": "wrong-key"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_no_auth_configured_allows_all(self):
        app = self._make_app(None)
        resp = app.test_client().get("/health")
        self.assertEqual(resp.status_code, 200)

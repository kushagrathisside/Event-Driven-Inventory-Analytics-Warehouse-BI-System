import os
import unittest
from unittest.mock import patch

from medwarehouse.cli import main
from medwarehouse.config import get_settings


class CliTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_inventory_producer_dry_run_smoke(self) -> None:
        with patch.dict(
            os.environ,
            {
                "MW_PRODUCER_INTERVAL_SECONDS": "0",
                "MW_PRODUCER_DRY_RUN": "true",
            },
            clear=False,
        ):
            get_settings.cache_clear()
            result = main(
                [
                    "--log-level",
                    "ERROR",
                    "produce",
                    "inventory",
                    "--max-events",
                    "2",
                    "--dry-run",
                ]
            )

        self.assertEqual(result, 0)

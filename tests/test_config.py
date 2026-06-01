import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from medwarehouse.config import get_settings


class ConfigTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_project_root_and_defaults_are_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(os.environ, {"MW_PROJECT_ROOT": tmp_dir}, clear=False):
                get_settings.cache_clear()
                settings = get_settings()
                expected_root = Path(tmp_dir).resolve()

        self.assertEqual(settings.paths.project_root, expected_root)
        self.assertEqual(settings.kafka.inventory_topic, "inventory_events")
        self.assertEqual(settings.analytics_writer_db.user, "spark_writer")

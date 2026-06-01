import unittest

from medwarehouse.warehouse.sql_runner import render_sql_template


class SqlTemplateTests(unittest.TestCase):
    def test_render_sql_template_replaces_placeholders(self) -> None:
        rendered = render_sql_template(
            "SELECT '{{VALUE}}'::text;",
            {"VALUE": "warehouse"},
        )
        self.assertEqual(rendered, "SELECT 'warehouse'::text;")

from __future__ import annotations

from typing import Any

from medwarehouse.config import AppSettings, get_settings


EXPECTED_TABLES = [
    "dim_supplier",
    "dim_product",
    "dim_warehouse",
    "stg_inventory_events",
    "fact_inventory_events",
]

EXPECTED_VIEWS = [
    "v_dim_supplier_current",
    "v_dim_product_current",
    "v_dim_warehouse_current",
    "v_inventory_balance",
    "v_inventory_snapshot",
]

TRACKED_ROLES = [
    "fdw_reader",
    "spark_writer",
    "analytics_reader",
]

COUNT_QUERIES = {
    "analytics.stg_inventory_events": "event_time",
    "analytics.fact_inventory_events": "event_time",
    "analytics.v_inventory_snapshot": "last_event_time",
}


def collect_inventory_warehouse_state(
    *,
    settings: AppSettings | None = None,
    infra_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    current_infra = infra_status or {}

    try:
        from medwarehouse.warehouse.postgres import connect

        with connect(active_settings.analytics_admin_db) as conn:
            with conn.cursor() as cursor:
                schemas = _fetch_scalar_list(
                    cursor,
                    """
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name IN ('master', 'analytics')
                    ORDER BY schema_name
                    """,
                )
                analytics_tables = _fetch_scalar_list(
                    cursor,
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'analytics'
                    ORDER BY table_name
                    """,
                )
                analytics_views = _fetch_scalar_list(
                    cursor,
                    """
                    SELECT table_name
                    FROM information_schema.views
                    WHERE table_schema = 'analytics'
                    ORDER BY table_name
                    """,
                )
                roles = _fetch_scalar_list(
                    cursor,
                    """
                    SELECT rolname
                    FROM pg_roles
                    WHERE rolname IN ('fdw_reader', 'spark_writer', 'analytics_reader')
                    ORDER BY rolname
                    """,
                )

                counts: dict[str, int | None] = {}
                last_updates: dict[str, str | None] = {}
                for relation, timestamp_column in COUNT_QUERIES.items():
                    try:
                        cursor.execute(
                            f"SELECT count(*)::bigint, max({timestamp_column}) FROM {relation}"
                        )
                        row = cursor.fetchone()
                        counts[relation] = int(row[0]) if row is not None else 0
                        last_updates[relation] = row[1].isoformat() if row and row[1] else None
                    except Exception:
                        conn.rollback()
                        counts[relation] = None
                        last_updates[relation] = None

                quality_issues = []
                quality_issue_total = 0
                try:
                    cursor.execute(
                        """
                        SELECT issue_name, issue_count
                        FROM analytics.run_inventory_quality_checks()
                        """
                    )
                    for issue_name, issue_count in cursor.fetchall():
                        item = {
                            "issue_name": issue_name,
                            "issue_count": int(issue_count),
                        }
                        quality_issues.append(item)
                        quality_issue_total += int(issue_count)
                except Exception:
                    conn.rollback()

        missing_tables = [
            table_name for table_name in EXPECTED_TABLES if table_name not in analytics_tables
        ]
        missing_views = [
            view_name for view_name in EXPECTED_VIEWS if view_name not in analytics_views
        ]
        return {
            "reachable": True,
            "target": active_settings.analytics_admin_db.jdbc_url,
            "schemas": schemas,
            "roles": roles,
            "expected_tables": EXPECTED_TABLES,
            "expected_views": EXPECTED_VIEWS,
            "present_tables": analytics_tables,
            "present_views": analytics_views,
            "missing_tables": missing_tables,
            "missing_views": missing_views,
            "counts": counts,
            "last_updates": last_updates,
            "quality_issues": quality_issues,
            "quality_issue_total": quality_issue_total,
            "error": None,
        }
    except Exception as exc:
        return {
            "reachable": False,
            "target": active_settings.analytics_admin_db.jdbc_url,
            "schemas": [],
            "roles": [],
            "expected_tables": EXPECTED_TABLES,
            "expected_views": EXPECTED_VIEWS,
            "present_tables": [],
            "present_views": [],
            "missing_tables": EXPECTED_TABLES,
            "missing_views": EXPECTED_VIEWS,
            "counts": {},
            "last_updates": {},
            "quality_issues": [],
            "quality_issue_total": 0,
            "error": warehouse_error_message(
                exc=exc,
                infra_status=current_infra,
                settings=active_settings,
            ),
        }


def warehouse_error_message(
    *,
    exc: Exception,
    infra_status: dict[str, Any],
    settings: AppSettings,
) -> str:
    raw = str(exc)
    hints: list[str] = []

    if "password authentication failed" in raw.lower():
        hints.append(
            "The configured analytics database points to "
            f"{settings.analytics_admin_db.host}:{settings.analytics_admin_db.port} "
            f"as user '{settings.analytics_admin_db.user}', but authentication failed."
        )
        if settings.analytics_admin_db.host in {"localhost", "127.0.0.1"}:
            hints.append(
                "This often means the dashboard is reaching a different local PostgreSQL "
                "instance than the bundled medical-warehouse stack."
            )

    if infra_status.get("overall_status") in {"unavailable", "stopped", "partial"}:
        hints.append("Start the local infrastructure stack and refresh the dashboard.")
        if infra_status.get("error"):
            hints.append(f"Infrastructure issue: {infra_status['error']}")

    if not hints:
        return raw
    return raw + "\n\n" + "\n".join(hints)


def _fetch_scalar_list(cursor, sql: str) -> list[str]:
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]

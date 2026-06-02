from __future__ import annotations

from typing import Any

from medwarehouse.logging import get_logger
from medwarehouse_webapp.db import connect, get_analytics_reader, get_analytics_writer


logger = get_logger(__name__)


def list_reorder_policies() -> list[dict[str, Any]]:
    with connect(get_analytics_reader()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, product_id, warehouse_id, reorder_point, reorder_quantity,
                       preferred_supplier_id, lead_time_days, active, created_at, updated_at
                FROM analytics.reorder_policies
                WHERE active = TRUE
                ORDER BY product_id, warehouse_id
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def create_reorder_policy(
    *,
    product_id: str,
    warehouse_id: str,
    reorder_point: int,
    reorder_quantity: int,
    preferred_supplier_id: str | None = None,
    lead_time_days: int = 7,
) -> dict[str, Any]:
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            # Deactivate any existing policy for this product+warehouse first
            cur.execute(
                """
                UPDATE analytics.reorder_policies
                SET active = FALSE, updated_at = timezone('UTC', now())
                WHERE product_id = %s AND warehouse_id = %s AND active = TRUE
                """,
                (product_id, warehouse_id),
            )
            cur.execute(
                """
                INSERT INTO analytics.reorder_policies
                    (product_id, warehouse_id, reorder_point, reorder_quantity,
                     preferred_supplier_id, lead_time_days, active)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                RETURNING id
                """,
                (product_id, warehouse_id, reorder_point, reorder_quantity,
                 preferred_supplier_id, lead_time_days),
            )
            row_id = cur.fetchone()[0]
        conn.commit()
    logger.info("Reorder policy created id=%s product=%s warehouse=%s threshold=%s",
                row_id, product_id, warehouse_id, reorder_point)
    return {"id": row_id, "status": "created"}


def update_reorder_policy(policy_id: int, **fields: Any) -> dict[str, Any]:
    allowed = {"reorder_point", "reorder_quantity", "preferred_supplier_id", "lead_time_days"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return {"id": policy_id, "status": "no_changes"}

    # Build aligned column list and value list explicitly — avoids dict-manipulation bugs.
    columns = list(updates.keys())
    values = [updates[c] for c in columns] + [policy_id]
    set_clause = ", ".join(f"{c} = %s" for c in columns) + ", updated_at = timezone('UTC', now())"

    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE analytics.reorder_policies SET {set_clause} WHERE id = %s",
                values,
            )
        conn.commit()
    return {"id": policy_id, "status": "updated"}


def deactivate_reorder_policy(policy_id: int) -> dict[str, Any]:
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.reorder_policies
                SET active = FALSE, updated_at = timezone('UTC', now())
                WHERE id = %s
                """,
                (policy_id,),
            )
        conn.commit()
    return {"id": policy_id, "status": "deactivated"}

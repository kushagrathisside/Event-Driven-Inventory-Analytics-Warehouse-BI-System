from __future__ import annotations

import smtplib
import os
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

from medwarehouse.contracts.inventory import build_inventory_event, deterministic_uuid
from medwarehouse.logging import get_logger
from medwarehouse.producers.common import emit_events
from medwarehouse.config import get_settings
from medwarehouse_webapp.db import connect, get_analytics_reader, get_analytics_writer


logger = get_logger(__name__)


def list_orders(status: str | None = None) -> list[dict[str, Any]]:
    query = """
        SELECT po_id, product_id, warehouse_id, supplier_id, ordered_quantity,
               trigger_reason, status, created_at, approved_at, sent_at,
               received_at, approved_by, notes,
               brand_name, generic_name, warehouse_name, supplier_name
        FROM analytics.v_purchase_order_status
    """
    params: list[Any] = []
    if status:
        query += " WHERE status = %s"
        params.append(status)
    query += " ORDER BY created_at DESC"

    with connect(get_analytics_reader()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def approve_order(po_id: str, approved_by: str) -> dict[str, Any]:
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.pending_purchase_orders
                SET status = 'APPROVED',
                    approved_at = timezone('UTC', now()),
                    approved_by = %s
                WHERE po_id = %s AND status = 'DRAFT'
                RETURNING po_id
                """,
                (approved_by, po_id),
            )
            if not cur.fetchone():
                return {"po_id": po_id, "status": "not_found_or_wrong_state"}
        conn.commit()
    logger.info("Purchase order approved po_id=%s by=%s", po_id, approved_by)
    return {"po_id": po_id, "status": "approved"}


def send_order(po_id: str) -> dict[str, Any]:
    """Mark order as SENT and dispatch supplier email if SMTP is configured."""
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.pending_purchase_orders
                SET status = 'SENT', sent_at = timezone('UTC', now())
                WHERE po_id = %s AND status = 'APPROVED'
                RETURNING po_id, product_id, warehouse_id, supplier_id, ordered_quantity
                """,
                (po_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"po_id": po_id, "status": "not_found_or_not_approved"}
        conn.commit()

    _, product_id, warehouse_id, supplier_id, qty = row
    _try_send_supplier_email(po_id, product_id, warehouse_id, supplier_id, qty)
    logger.info("Purchase order sent po_id=%s supplier=%s qty=%s", po_id, supplier_id, qty)
    return {"po_id": po_id, "status": "sent"}


def cancel_order(po_id: str) -> dict[str, Any]:
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.pending_purchase_orders
                SET status = 'CANCELLED'
                WHERE po_id = %s AND status IN ('DRAFT', 'APPROVED')
                RETURNING po_id
                """,
                (po_id,),
            )
            if not cur.fetchone():
                return {"po_id": po_id, "status": "not_found_or_already_terminal"}
        conn.commit()
    return {"po_id": po_id, "status": "cancelled"}


def receive_order(po_id: str, operator: str = "webapp") -> dict[str, Any]:
    """Mark order as RECEIVED and emit a STOCK_RECEIVED event to close the loop."""
    with connect(get_analytics_writer()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.pending_purchase_orders
                SET status = 'RECEIVED', received_at = timezone('UTC', now())
                WHERE po_id = %s AND status = 'SENT'
                RETURNING product_id, warehouse_id, supplier_id, ordered_quantity
                """,
                (po_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"po_id": po_id, "status": "not_found_or_not_sent"}
        conn.commit()

    product_id, warehouse_id, supplier_id, qty = row
    settings = get_settings()
    event_time = datetime.now(timezone.utc)
    event_id = deterministic_uuid(settings.sample_data.seed, f"po_received:{po_id}")
    event = build_inventory_event(
        event_id=event_id,
        event_type="STOCK_RECEIVED",
        event_time=event_time,
        producer=operator,
        product_id=product_id,
        warehouse_id=warehouse_id,
        batch_number=f"PO-{po_id[:8].upper()}",
        expiry_date=None,
        quantity_delta=qty,
        supplier_id=supplier_id,
    )
    emit_events(
        topic=settings.kafka.inventory_topic,
        events=[event],
        key_field="product_id",
        interval_seconds=0,
        dry_run=settings.producer.dry_run,
    )
    logger.info("PO received po_id=%s → STOCK_RECEIVED event_id=%s", po_id, event_id)
    return {"po_id": po_id, "status": "received", "stock_event_id": event_id}


def _try_send_supplier_email(
    po_id: str,
    product_id: str,
    warehouse_id: str,
    supplier_id: str,
    qty: int,
) -> None:
    """Send email via SMTP if MW_SMTP_HOST is configured. Fails silently otherwise."""
    smtp_host = os.environ.get("MW_SMTP_HOST")
    smtp_port = int(os.environ.get("MW_SMTP_PORT", "587"))
    smtp_user = os.environ.get("MW_SMTP_USER", "")
    smtp_password = os.environ.get("MW_SMTP_PASSWORD", "")
    from_addr = os.environ.get("MW_SMTP_FROM", smtp_user)
    to_addr = os.environ.get("MW_SUPPLIER_EMAIL_OVERRIDE") or f"orders+{supplier_id}@supplier.local"

    if not smtp_host:
        logger.info("SMTP not configured — skipping supplier email for po_id=%s", po_id)
        return

    msg = EmailMessage()
    msg["Subject"] = f"Purchase Order {po_id} — {product_id}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(
        f"Dear Supplier,\n\n"
        f"Please supply {qty} units of product {product_id} to warehouse {warehouse_id}.\n\n"
        f"Purchase Order Reference: {po_id}\n\n"
        f"Regards,\nMedWarehouse Procurement System"
    )

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.starttls()
            if smtp_user:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        logger.info("Supplier email sent po_id=%s to=%s", po_id, to_addr)
    except Exception as exc:
        logger.warning("Supplier email failed po_id=%s error=%s", po_id, exc)

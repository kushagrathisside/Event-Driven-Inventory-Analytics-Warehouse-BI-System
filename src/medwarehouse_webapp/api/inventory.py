from __future__ import annotations

from flask import Blueprint, jsonify

from medwarehouse_webapp.db import connect, get_analytics_reader, get_master_db


blueprint = Blueprint("inventory", __name__, url_prefix="/api/v1")


@blueprint.get("/products")
def list_products():
    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, brand_name, generic_name, form_factor, hsn_code, supplier_id FROM products ORDER BY brand_name"
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)


@blueprint.get("/suppliers")
def list_suppliers():
    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT supplier_id, name, gstin FROM suppliers ORDER BY name")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)


@blueprint.get("/warehouses")
def list_warehouses():
    with connect(get_master_db()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT location_id AS warehouse_id, name AS warehouse_name, temperature_range FROM warehouse_locations ORDER BY name"
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)


@blueprint.get("/inventory")
def current_inventory():
    with connect(get_analytics_reader()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT product_id, brand_name, generic_name, warehouse_id, warehouse_name,
                       batch_number, expiry_date, current_quantity, last_event_time
                FROM analytics.v_inventory_snapshot
                ORDER BY brand_name, warehouse_name, batch_number
                """
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)


@blueprint.get("/inventory/risk")
def inventory_risk():
    with connect(get_analytics_reader()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT product_id, brand_name, warehouse_id, warehouse_name,
                       batch_number, expiry_date, current_quantity, reorder_point,
                       quantity_gap, risk_status, lead_time_days
                FROM analytics.v_reorder_risk
                ORDER BY
                    CASE risk_status
                        WHEN 'OUT_OF_STOCK' THEN 1
                        WHEN 'BELOW_THRESHOLD' THEN 2
                        WHEN 'APPROACHING_THRESHOLD' THEN 3
                        ELSE 4
                    END,
                    brand_name
                """
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)

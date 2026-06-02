from __future__ import annotations

from functools import wraps
from typing import Callable

from flask import Blueprint, jsonify, request

from medwarehouse_webapp.db import connect, get_analytics_reader
from medwarehouse_webapp.services.idempotency import get_idempotency_store
from medwarehouse_webapp.services.stock_service import (
    record_stock_adjusted,
    record_stock_expired,
    record_stock_received,
    record_stock_sold,
)


blueprint = Blueprint("stock", __name__, url_prefix="/api/v1/stock")


def _operator() -> str:
    return request.headers.get("X-Operator", "webapp")


def _idempotent(fn: Callable) -> Callable:
    """
    Decorator: if the request carries an `Idempotency-Key` header and that key
    has been seen before within the TTL window, return the cached response
    immediately without re-executing the handler.

    This protects against duplicate submissions when a client retries after a
    timeout or network failure.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        key = request.headers.get("Idempotency-Key", "").strip()
        if key:
            cached = get_idempotency_store().get(key)
            if cached is not None:
                return jsonify(cached), 200  # 200 signals a replayed response

        result_tuple = fn(*args, **kwargs)
        response, status = result_tuple if isinstance(result_tuple, tuple) else (result_tuple, 200)

        if key and status in (200, 201):
            get_idempotency_store().set(key, response.get_json())

        return response, status

    return wrapper


@blueprint.post("/received")
@_idempotent
def stock_received():
    body = request.get_json(force=True) or {}
    try:
        result = record_stock_received(
            product_id=body["product_id"],
            warehouse_id=body["warehouse_id"],
            batch_number=body["batch_number"],
            expiry_date=body["expiry_date"],
            quantity=int(body["quantity"]),
            supplier_id=body.get("supplier_id"),
            operator=_operator(),
        )
    except (KeyError, TypeError) as exc:
        return jsonify({"error": f"Missing required field: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(result), 201


@blueprint.post("/sold")
@_idempotent
def stock_sold():
    body = request.get_json(force=True) or {}
    try:
        result = record_stock_sold(
            product_id=body["product_id"],
            warehouse_id=body["warehouse_id"],
            batch_number=body["batch_number"],
            expiry_date=body["expiry_date"],
            quantity=int(body["quantity"]),
            sale_id=body["sale_id"],
            operator=_operator(),
        )
    except (KeyError, TypeError) as exc:
        return jsonify({"error": f"Missing required field: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(result), 201


@blueprint.post("/adjusted")
@_idempotent
def stock_adjusted():
    body = request.get_json(force=True) or {}
    try:
        result = record_stock_adjusted(
            product_id=body["product_id"],
            warehouse_id=body["warehouse_id"],
            batch_number=body["batch_number"],
            quantity_delta=int(body["quantity_delta"]),
            reason=body["reason"],
            operator=_operator(),
        )
    except (KeyError, TypeError) as exc:
        return jsonify({"error": f"Missing required field: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(result), 201


@blueprint.post("/expired")
@_idempotent
def stock_expired():
    body = request.get_json(force=True) or {}
    try:
        result = record_stock_expired(
            product_id=body["product_id"],
            warehouse_id=body["warehouse_id"],
            batch_number=body["batch_number"],
            expiry_date=body["expiry_date"],
            quantity=int(body["quantity"]),
            operator=_operator(),
        )
    except (KeyError, TypeError) as exc:
        return jsonify({"error": f"Missing required field: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(result), 201


@blueprint.get("/movements")
def stock_movements():
    limit = min(int(request.args.get("limit", 50)), 500)
    with connect(get_analytics_reader()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, event_type, event_time, product_id, warehouse_id,
                       batch_number, quantity_delta, supplier_id, sale_id, adjustment_reason
                FROM analytics.stg_inventory_events
                ORDER BY event_time DESC
                LIMIT %s
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return jsonify(rows)

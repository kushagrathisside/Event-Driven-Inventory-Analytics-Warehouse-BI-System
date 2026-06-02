from __future__ import annotations

from flask import Blueprint, jsonify, request

from medwarehouse_webapp.services.order_service import (
    approve_order,
    cancel_order,
    list_orders,
    receive_order,
    send_order,
)


blueprint = Blueprint("orders", __name__, url_prefix="/api/v1/orders")


@blueprint.get("")
def get_orders():
    status = request.args.get("status")
    return jsonify(list_orders(status=status))


@blueprint.post("/<po_id>/approve")
def approve(po_id: str):
    body = request.get_json(force=True) or {}
    approved_by = body.get("approved_by") or request.headers.get("X-Operator", "webapp")
    result = approve_order(po_id, approved_by=approved_by)
    return jsonify(result)


@blueprint.post("/<po_id>/send")
def send(po_id: str):
    result = send_order(po_id)
    return jsonify(result)


@blueprint.post("/<po_id>/cancel")
def cancel(po_id: str):
    result = cancel_order(po_id)
    return jsonify(result)


@blueprint.post("/<po_id>/receive")
def receive(po_id: str):
    operator = request.headers.get("X-Operator", "webapp")
    result = receive_order(po_id, operator=operator)
    return jsonify(result)

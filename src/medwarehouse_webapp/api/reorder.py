from __future__ import annotations

from flask import Blueprint, jsonify, request

from medwarehouse_webapp.services.reorder_service import (
    create_reorder_policy,
    deactivate_reorder_policy,
    list_reorder_policies,
    update_reorder_policy,
)


blueprint = Blueprint("reorder", __name__, url_prefix="/api/v1/reorder-policies")


@blueprint.get("")
def get_policies():
    return jsonify(list_reorder_policies())


@blueprint.post("")
def create_policy():
    body = request.get_json(force=True) or {}
    try:
        result = create_reorder_policy(
            product_id=body["product_id"],
            warehouse_id=body["warehouse_id"],
            reorder_point=int(body["reorder_point"]),
            reorder_quantity=int(body["reorder_quantity"]),
            preferred_supplier_id=body.get("preferred_supplier_id"),
            lead_time_days=int(body.get("lead_time_days", 7)),
        )
    except (KeyError, TypeError) as exc:
        return jsonify({"error": f"Missing required field: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    return jsonify(result), 201


@blueprint.put("/<int:policy_id>")
def update_policy(policy_id: int):
    body = request.get_json(force=True) or {}
    result = update_reorder_policy(policy_id, **body)
    return jsonify(result)


@blueprint.delete("/<int:policy_id>")
def delete_policy(policy_id: int):
    result = deactivate_reorder_policy(policy_id)
    return jsonify(result)

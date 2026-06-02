from __future__ import annotations

from functools import partial
from typing import Any

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from medwarehouse.platform.control import ControlPlaneService
from medwarehouse.platform.services import StatusService

_ALLOWED_INFRA_ACTIONS = frozenset({"start", "stop"})


def create_pages_blueprint(
    *,
    control_plane: ControlPlaneService,
    status_service: StatusService,
) -> Blueprint:
    blueprint = Blueprint("platform_pages", __name__)

    def render_page(page_key: str, title: str, subtitle: str):
        status = status_service.get_full_status()
        return render_template(
            f"{page_key}.html",
            page_key=page_key,
            page_title=title,
            page_subtitle=subtitle,
            status=status,
            control_plane=control_plane,
        )

    routes = {
        "dashboard": (
            "/",
            "Operational Monitoring Dashboard",
            "Data-first visibility into freshness, quality, infrastructure, and recovery paths.",
        ),
        "pipeline": (
            "/pipeline",
            "Pipeline Flow",
            "Trace Bronze to Warehouse movement, freshness, and consistency gaps.",
        ),
        "warehouse": (
            "/warehouse",
            "Warehouse State",
            "Inspect reachability, counts, semantic views, and quality-check outputs.",
        ),
        "artifacts": (
            "/artifacts",
            "Artifact Inventory",
            "Review local Bronze, Silver, and Quarantine filesystem outputs.",
        ),
        "infrastructure": (
            "/infrastructure",
            "Infrastructure Status",
            "Watch the local service stack and recent infrastructure control activity.",
        ),
        "jobs": (
            "/jobs",
            "Jobs And Logs",
            "Operate predefined commands safely and inspect recent execution details.",
        ),
        "runbook": (
            "/runbook",
            "Runbook",
            "Use the operator guide to diagnose common failures and recover the pipeline.",
        ),
    }

    for page_key, (path, title, subtitle) in routes.items():
        blueprint.add_url_rule(
            path,
            endpoint=page_key,
            view_func=partial(render_page, page_key, title, subtitle),
        )

    @blueprint.get("/health")
    def health():
        return {"status": "ok"}

    @blueprint.post("/jobs/<job_id>/start")
    def start_job(job_id: str):
        ok, message = control_plane.start_job(job_id)
        flash(message, "success" if ok else "error")
        return redirect(url_for("platform_pages.jobs"))

    @blueprint.post("/jobs/<job_id>/stop")
    def stop_job(job_id: str):
        ok, message = control_plane.stop_job(job_id)
        flash(message, "success" if ok else "error")
        return redirect(url_for("platform_pages.jobs"))

    @blueprint.post("/infra/<action>")
    def infra_action(action: str):
        if action not in _ALLOWED_INFRA_ACTIONS:
            flash(f"Unknown infrastructure action '{action}'.", "error")
            return redirect(url_for("platform_pages.infrastructure"))
        ok, message = control_plane.run_infra_action(action)
        status_service.invalidate()
        flash(message, "success" if ok else "error")
        return redirect(url_for("platform_pages.infrastructure"))

    return blueprint


def create_api_blueprint(
    *,
    control_plane: ControlPlaneService,
    status_service: StatusService,
) -> Blueprint:
    blueprint = Blueprint("platform_api", __name__, url_prefix="/api")

    @blueprint.get("/status")
    def legacy_status():
        full = status_service.get_full_status(force_refresh=_force_refresh_requested())
        return jsonify(
            {
                "snapshot": full["legacy"]["snapshot"],
                "jobs": control_plane.job_snapshots(),
                "infra": control_plane.infra_snapshot(),
                "alerts": full["alerts"],
            }
        )

    @blueprint.get("/status/full")
    def full_status():
        return jsonify(status_service.get_full_status(force_refresh=_force_refresh_requested()))

    @blueprint.get("/status/pipeline")
    def pipeline_status():
        return jsonify(status_service.get_pipeline_status(force_refresh=_force_refresh_requested()))

    @blueprint.get("/status/infra")
    def infra_status():
        return jsonify(status_service.get_infra_status(force_refresh=_force_refresh_requested()))

    @blueprint.get("/status/alerts")
    def alerts_status():
        return jsonify(status_service.get_alerts(force_refresh=_force_refresh_requested()))

    @blueprint.post("/jobs/<job_id>/start")
    def api_start_job(job_id: str):
        ok, message = control_plane.start_job(job_id)
        status_service.invalidate()
        return jsonify(
            {
                "ok": ok,
                "message": message,
                "jobs": control_plane.job_snapshots(),
                "job": control_plane.run_job_snapshot(job_id),
            }
        )

    @blueprint.post("/jobs/<job_id>/stop")
    def api_stop_job(job_id: str):
        ok, message = control_plane.stop_job(job_id)
        status_service.invalidate()
        return jsonify(
            {
                "ok": ok,
                "message": message,
                "jobs": control_plane.job_snapshots(),
                "job": control_plane.run_job_snapshot(job_id),
            }
        )

    @blueprint.post("/infra/start")
    def api_infra_start():
        return _infra_action(control_plane, status_service, "start")

    @blueprint.post("/infra/stop")
    def api_infra_stop():
        return _infra_action(control_plane, status_service, "stop")

    @blueprint.post("/infra/<action>")
    def api_infra_action(action: str):
        return _infra_action(control_plane, status_service, action)

    return blueprint


def _infra_action(
    control_plane: ControlPlaneService,
    status_service: StatusService,
    action: str,
):
    ok, message = control_plane.run_infra_action(action)
    status_service.invalidate()
    return jsonify(
        {
            "ok": ok,
            "message": message,
            "infra": control_plane.infra_snapshot(),
            "status": status_service.get_infra_status(force_refresh=True),
        }
    )


def _force_refresh_requested() -> bool:
    refresh = (request.args.get("refresh") or "").strip().lower()
    return refresh in {"hard", "true", "1", "yes"}


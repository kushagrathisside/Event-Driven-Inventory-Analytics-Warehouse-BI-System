from __future__ import annotations

import copy
from functools import lru_cache
from typing import Any

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.cache import TTLCache
from medwarehouse.platform.control import (
    ControlPlaneService,
    get_control_plane_service,
    job_specs,
)
from medwarehouse.platform.probes import (
    AirflowProbe,
    ArtifactProbe,
    InfraProbe,
    JobProbe,
    PipelineProbe,
    WarehouseProbe,
)
from medwarehouse.platform.services.alerts import AlertEngine, build_default_alert_engine
from medwarehouse.platform.utils.time import lag_seconds, parse_timestamp, utc_now


class StatusService:
    def __init__(
        self,
        *,
        control_plane: ControlPlaneService | None = None,
        settings: AppSettings | None = None,
        ttl_seconds: float = 60.0,
        alert_engine: AlertEngine | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._control_plane = control_plane or get_control_plane_service()
        self._cache = TTLCache(ttl_seconds=ttl_seconds)
        self._alert_engine = alert_engine or build_default_alert_engine()
        self._probes = {
            "infra": InfraProbe(),
            "warehouse": WarehouseProbe(settings=self._settings),
            "pipeline": PipelineProbe(settings=self._settings),
            "artifacts": ArtifactProbe(settings=self._settings),
            "jobs": JobProbe(control_plane=self._control_plane),
            "airflow": AirflowProbe(settings=self._settings),
        }

    def invalidate(self) -> None:
        self._cache.invalidate()

    def get_full_status(self, *, force_refresh: bool = False) -> dict[str, Any]:
        payload, served_from_cache = self._cache.get_or_create(
            "full",
            self._build_full_status,
            force_refresh=force_refresh,
        )
        result = copy.deepcopy(payload)
        result.setdefault("cache", {})
        result["cache"]["served_from_cache"] = served_from_cache
        return result

    def get_pipeline_status(self, *, force_refresh: bool = False) -> dict[str, Any]:
        full = self.get_full_status(force_refresh=force_refresh)
        return {
            "generated_at": full["generated_at"],
            "health": full["health"],
            "pipeline": full["metrics"]["pipeline"],
            "alerts": self._filter_alerts(full["alerts"], {"pipeline", "bronze", "silver", "warehouse"}),
        }

    def get_infra_status(self, *, force_refresh: bool = False) -> dict[str, Any]:
        full = self.get_full_status(force_refresh=force_refresh)
        return {
            "generated_at": full["generated_at"],
            "health": full["health"],
            "infra": full["metrics"]["infra"],
            "control": full["controls"]["infra"],
            "alerts": self._filter_alerts(full["alerts"], {"infra", "warehouse"}),
        }

    def get_alerts(self, *, force_refresh: bool = False) -> dict[str, Any]:
        full = self.get_full_status(force_refresh=force_refresh)
        return full["alerts"]

    def platform_snapshot(self, *, force_refresh: bool = False) -> dict[str, Any]:
        full = self.get_full_status(force_refresh=force_refresh)
        return full["legacy"]["snapshot"]

    def _build_full_status(self) -> dict[str, Any]:
        generated_at = utc_now()
        probes = {name: probe.collect() for name, probe in self._probes.items()}
        metrics = self._build_metrics(probes)
        self._alert_engine.evaluate(metrics)
        alerts = self._alert_engine.snapshot()
        health = self._build_health(alerts)

        full_status = {
            "generated_at": generated_at,
            "cache": {"ttl_seconds": self._cache.ttl_seconds},
            "health": health,
            "environment": self._environment_payload(),
            "coverage": self._coverage_status(),
            "roles": {
                "configured": {
                    "fdw_reader": self._settings.warehouse_roles.fdw_reader_user,
                    "spark_writer": self._settings.warehouse_roles.spark_writer_user,
                    "analytics_reader": self._settings.warehouse_roles.analytics_reader_user,
                },
                "detected": probes["warehouse"].get("roles", []),
            },
            "orchestration": self._orchestration_payload(probes["airflow"]),
            "metrics": metrics,
            "alerts": alerts,
            "controls": {
                "jobs": probes["jobs"]["items"],
                "infra": self._control_plane.infra_snapshot(),
            },
            "catalog": {"jobs": self._configured_jobs()},
            "probes": probes,
        }
        full_status["legacy"] = {"snapshot": self._legacy_snapshot(full_status)}
        return full_status

    def _build_metrics(self, probes: dict[str, dict[str, Any]]) -> dict[str, Any]:
        pipeline_probe = probes["pipeline"]
        warehouse_probe = probes["warehouse"]
        artifact_probe = probes["artifacts"]["artifacts"]
        infra_probe = probes["infra"]
        airflow_probe = probes["airflow"]
        job_probe = probes["jobs"]

        bronze = pipeline_probe["stages"]["bronze"]
        silver = pipeline_probe["stages"]["silver"]
        staging = pipeline_probe["stages"]["staging"]
        warehouse_stage = pipeline_probe["stages"]["warehouse"]

        pipeline_metrics = {
            "bronze": self._stage_metric(bronze, expected_seconds=600.0),
            "silver": self._stage_metric(
                silver,
                expected_seconds=1800.0,
                extra={"quarantine": silver.get("quarantine", 0) or 0},
            ),
            "staging": self._stage_metric(staging, expected_seconds=1800.0),
            "warehouse": self._warehouse_stage_metric(warehouse_stage, expected_seconds=3600.0),
            "consistency": pipeline_probe["consistency"],
        }

        artifacts_metrics = {
            name: {
                **artifact,
                "lag_seconds": lag_seconds(artifact.get("updated_at")),
            }
            for name, artifact in artifact_probe.items()
        }

        services = infra_probe.get("services", [])
        service_map = {service["name"]: service["status"] for service in services}
        running_services = sum(1 for service in services if service["status"] == "running")
        infra_metrics = {
            "available": infra_probe.get("available", False),
            "overall_status": infra_probe.get("overall_status", "unknown"),
            "error": infra_probe.get("error"),
            "compose_command": infra_probe.get("compose_command"),
            "services": services,
            "service_map": service_map,
            "running_services": running_services,
            "total_services": len(services),
            "kafka_running": service_map.get("kafka") == "running",
            "postgres_running": service_map.get("postgres") == "running",
            "airflow_running": all(
                service_map.get(name) == "running"
                for name in ("airflow-webserver", "airflow-scheduler")
            ),
        }

        last_run = airflow_probe.get("last_run")
        airflow_metrics = {
            "dag_id": airflow_probe.get("dag_id"),
            "dag_exists": airflow_probe.get("dag_exists", False),
            "dag_path": airflow_probe.get("dag_path"),
            "schedule": airflow_probe.get("schedule"),
            "metadata_available": airflow_probe.get("metadata_available", False),
            "metadata_path": airflow_probe.get("metadata_path"),
            "last_run": last_run,
            "last_run_at": self._airflow_last_run_at(last_run),
            "lag_seconds": lag_seconds(self._airflow_last_run_at(last_run)),
            "error": airflow_probe.get("error"),
        }

        warehouse_metrics = {
            **warehouse_probe,
            "staging_count": warehouse_probe.get("counts", {}).get("analytics.stg_inventory_events"),
            "fact_count": warehouse_probe.get("counts", {}).get("analytics.fact_inventory_events"),
            "snapshot_count": warehouse_probe.get("counts", {}).get("analytics.v_inventory_snapshot"),
        }

        jobs_metrics = {
            **job_probe["summary"],
            "items": job_probe["items"],
        }

        return {
            "pipeline": pipeline_metrics,
            "infra": infra_metrics,
            "artifacts": artifacts_metrics,
            "jobs": jobs_metrics,
            "warehouse": warehouse_metrics,
            "airflow": airflow_metrics,
        }

    def _stage_metric(
        self,
        stage_data: dict[str, Any],
        *,
        expected_seconds: float,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metric = {
            "count": stage_data.get("count"),
            "last_update": stage_data.get("last_update"),
            "lag_seconds": lag_seconds(stage_data.get("last_update")),
            "expected_interval_seconds": expected_seconds,
            "status": self._freshness_status(
                last_update=stage_data.get("last_update"),
                expected_seconds=expected_seconds,
            ),
        }
        if extra:
            metric.update(extra)
        return metric

    def _warehouse_stage_metric(
        self,
        stage_data: dict[str, Any],
        *,
        expected_seconds: float,
    ) -> dict[str, Any]:
        last_update = stage_data.get("last_update")
        reachable = stage_data.get("reachable", False)
        return {
            "fact_count": stage_data.get("fact_count"),
            "snapshot_count": stage_data.get("snapshot_count"),
            "reachable": reachable,
            "last_update": last_update,
            "lag_seconds": lag_seconds(last_update),
            "expected_interval_seconds": expected_seconds,
            "status": "critical"
            if not reachable
            else self._freshness_status(last_update=last_update, expected_seconds=expected_seconds),
        }

    @staticmethod
    def _freshness_status(*, last_update: str | None, expected_seconds: float) -> str:
        lag = lag_seconds(last_update)
        if last_update is None or lag is None:
            return "idle"
        if lag > (expected_seconds * 5):
            return "critical"
        if lag > (expected_seconds * 2):
            return "warning"
        return "healthy"

    def _environment_payload(self) -> dict[str, Any]:
        return {
            "env_name": self._settings.env_name,
            "project_root": str(self._settings.paths.project_root),
            "env_file_exists": (self._settings.paths.project_root / ".env").exists(),
            "kafka_bootstrap_servers": self._settings.kafka.bootstrap_servers,
            "master_db": self._settings.master_db.jdbc_url,
            "analytics_db": self._settings.analytics_admin_db.jdbc_url,
        }

    def _orchestration_payload(self, airflow_probe: dict[str, Any]) -> dict[str, Any]:
        return {
            "dag_id": airflow_probe.get("dag_id"),
            "dag_path": airflow_probe.get("dag_path"),
            "dag_exists": airflow_probe.get("dag_exists", False),
            "schedule": airflow_probe.get("schedule"),
            "metadata_available": airflow_probe.get("metadata_available", False),
            "metadata_path": airflow_probe.get("metadata_path"),
            "last_run": airflow_probe.get("last_run"),
            "error": airflow_probe.get("error"),
        }

    @staticmethod
    def _airflow_last_run_at(last_run: dict[str, Any] | None) -> str | None:
        if not last_run:
            return None
        for key in ("end_date", "start_date", "logical_date", "execution_date"):
            value = last_run.get(key)
            if value and parse_timestamp(str(value)) is not None:
                return str(value)
        return None

    @staticmethod
    def _coverage_status() -> list[dict[str, str]]:
        return [
            {
                "domain": "inventory",
                "coverage": "End-to-end implemented",
                "details": "Producer, Bronze, Silver, staging, warehouse facts/views, and Airflow DAG exist.",
            },
            {
                "domain": "procurement",
                "coverage": "Producer only",
                "details": "Producer exists, but no Bronze/Silver/warehouse pipeline is implemented yet.",
            },
            {
                "domain": "sales",
                "coverage": "Producer only",
                "details": "Producer exists, but no Bronze/Silver/warehouse pipeline is implemented yet.",
            },
        ]

    @staticmethod
    def _build_health(alerts: dict[str, Any]) -> dict[str, Any]:
        summary = alerts["summary"]
        if summary["critical"] > 0:
            status = "critical"
        elif summary["warning"] > 0:
            status = "warning"
        else:
            status = "healthy"
        return {
            "status": status,
            "summary": (
                f"{summary['critical']} critical, {summary['warning']} warning, "
                f"{summary['info']} info alerts"
            ),
            **summary,
        }

    def _legacy_snapshot(self, full: dict[str, Any]) -> dict[str, Any]:
        return {
            "environment": full["environment"],
            "infrastructure": full["metrics"]["infra"],
            "artifacts": full["metrics"]["artifacts"],
            "warehouse": full["metrics"]["warehouse"],
            "coverage": full["coverage"],
            "roles": full["roles"]["configured"],
            "orchestration": full["orchestration"],
            "jobs": self._configured_jobs(),
        }

    @staticmethod
    def _filter_alerts(alerts: dict[str, Any], allowed_sources: set[str]) -> dict[str, Any]:
        active = [alert for alert in alerts["active"] if alert["source"] in allowed_sources]
        recent = [alert for alert in alerts["recent"] if alert["source"] in allowed_sources]
        return {
            "active": active,
            "recent": recent,
            "summary": {
                "total": len(active),
                "critical": sum(1 for alert in active if alert["severity"] == "CRITICAL"),
                "warning": sum(1 for alert in active if alert["severity"] == "WARNING"),
                "info": sum(1 for alert in active if alert["severity"] == "INFO"),
                "sources": sorted({alert["source"] for alert in active}),
            },
        }

    @staticmethod
    def _configured_jobs() -> list[dict[str, Any]]:
        return [
            {
                "job_id": spec.job_id,
                "name": spec.name,
                "domain": spec.domain,
                "stage": spec.stage,
                "description": spec.description,
                "long_running": spec.long_running,
                "command": "python -m medwarehouse " + " ".join(spec.command),
            }
            for spec in job_specs()
        ]


@lru_cache(maxsize=1)
def get_status_service() -> StatusService:
    return StatusService()


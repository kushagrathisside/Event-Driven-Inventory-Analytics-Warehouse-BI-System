import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from medwarehouse.cli import main
from medwarehouse.config import get_settings
from medwarehouse.platform.app import create_platform_app
from medwarehouse.platform.infra import docker_service_status
from medwarehouse.platform.services import ControlPlaneService
from medwarehouse.platform.services.status import StatusService
from medwarehouse.platform.store import ControlPlaneStore


def _sample_full_status() -> dict:
    full = {
        "generated_at": "2026-04-27T00:00:00+00:00",
        "cache": {"ttl_seconds": 60.0, "served_from_cache": False},
        "health": {
            "status": "warning",
            "summary": "0 critical, 1 warning, 0 info alerts",
            "total": 1,
            "critical": 0,
            "warning": 1,
            "info": 0,
            "sources": ["silver"],
        },
        "environment": {
            "env_name": "test",
            "project_root": "/tmp/project",
            "env_file_exists": True,
            "kafka_bootstrap_servers": "localhost:9092",
            "master_db": "jdbc:postgresql://localhost/master",
            "analytics_db": "jdbc:postgresql://localhost/analytics",
        },
        "coverage": [
            {
                "domain": "inventory",
                "coverage": "End-to-end implemented",
                "details": "Inventory path is implemented.",
            }
        ],
        "roles": {
            "configured": {
                "fdw_reader": "fdw_reader",
                "spark_writer": "spark_writer",
                "analytics_reader": "analytics_reader",
            },
            "detected": ["fdw_reader"],
        },
        "orchestration": {
            "dag_id": "inventory_gold_pipeline",
            "dag_path": "/tmp/dag.py",
            "dag_exists": True,
            "schedule": "manual",
            "metadata_available": False,
            "metadata_path": None,
            "last_run": None,
            "error": None,
        },
        "metrics": {
            "pipeline": {
                "bronze": {
                    "count": 4,
                    "last_update": "2026-04-27T00:00:00+00:00",
                    "lag_seconds": 10.0,
                    "expected_interval_seconds": 600.0,
                    "status": "healthy",
                },
                "silver": {
                    "count": 2,
                    "last_update": "2026-04-26T21:00:00+00:00",
                    "lag_seconds": 7200.0,
                    "expected_interval_seconds": 1800.0,
                    "status": "warning",
                    "quarantine": 1,
                },
                "staging": {
                    "count": 12,
                    "last_update": "2026-04-27T00:00:00+00:00",
                    "lag_seconds": 10.0,
                    "expected_interval_seconds": 1800.0,
                    "status": "healthy",
                },
                "warehouse": {
                    "fact_count": 12,
                    "snapshot_count": 10,
                    "reachable": True,
                    "last_update": "2026-04-27T00:00:00+00:00",
                    "lag_seconds": 10.0,
                    "expected_interval_seconds": 3600.0,
                    "status": "healthy",
                },
                "consistency": {
                    "bronze_vs_silver_gap": 2,
                    "silver_vs_staging_gap": -10,
                    "staging_vs_fact_gap": 0,
                    "fact_vs_snapshot_gap": 2,
                },
            },
            "infra": {
                "available": True,
                "overall_status": "stopped",
                "error": None,
                "compose_command": "docker-compose",
                "services": [
                    {"name": "postgres", "status": "stopped"},
                    {"name": "kafka", "status": "stopped"},
                ],
                "service_map": {"postgres": "stopped", "kafka": "stopped"},
                "running_services": 0,
                "total_services": 2,
                "kafka_running": False,
                "postgres_running": False,
                "airflow_running": False,
            },
            "artifacts": {
                "bronze": {
                    "path": "/tmp/bronze",
                    "exists": True,
                    "parquet_files": 4,
                    "size_bytes": 64,
                    "updated_at": "2026-04-27T00:00:00+00:00",
                    "count": 4,
                    "lag_seconds": 10.0,
                },
                "silver": {
                    "path": "/tmp/silver",
                    "exists": True,
                    "parquet_files": 2,
                    "size_bytes": 32,
                    "updated_at": "2026-04-26T21:00:00+00:00",
                    "count": 2,
                    "lag_seconds": 7200.0,
                },
                "quarantine": {
                    "path": "/tmp/quarantine",
                    "exists": True,
                    "parquet_files": 1,
                    "size_bytes": 8,
                    "updated_at": "2026-04-26T21:00:00+00:00",
                    "count": 1,
                    "lag_seconds": 7200.0,
                },
            },
            "jobs": {
                "total": 1,
                "running": 0,
                "failed": 0,
                "idle": 1,
                "by_status": {"idle": 1},
                "items": [],
            },
            "warehouse": {
                "reachable": True,
                "target": "jdbc:postgresql://localhost/analytics",
                "schemas": ["analytics"],
                "roles": ["fdw_reader"],
                "expected_tables": ["fact_inventory_events"],
                "expected_views": ["v_inventory_snapshot"],
                "present_tables": ["fact_inventory_events"],
                "present_views": ["v_inventory_snapshot"],
                "missing_tables": [],
                "missing_views": [],
                "counts": {
                    "analytics.stg_inventory_events": 12,
                    "analytics.fact_inventory_events": 12,
                    "analytics.v_inventory_snapshot": 10,
                },
                "last_updates": {
                    "analytics.stg_inventory_events": "2026-04-27T00:00:00+00:00",
                    "analytics.fact_inventory_events": "2026-04-27T00:00:00+00:00",
                    "analytics.v_inventory_snapshot": "2026-04-27T00:00:00+00:00",
                },
                "quality_issues": [{"issue_name": "staged_events_not_loaded", "issue_count": 0}],
                "quality_issue_total": 0,
                "status": "ok",
                "staging_count": 12,
                "fact_count": 12,
                "snapshot_count": 10,
                "error": None,
                "checked_at": "2026-04-27T00:00:00+00:00",
                "probe": "warehouse",
            },
            "airflow": {
                "dag_id": "inventory_gold_pipeline",
                "dag_exists": True,
                "dag_path": "/tmp/dag.py",
                "schedule": "manual",
                "metadata_available": False,
                "metadata_path": None,
                "last_run": None,
                "last_run_at": None,
                "lag_seconds": None,
                "error": None,
            },
        },
        "alerts": {
            "active": [
                {
                    "id": "quality:quarantine",
                    "name": "Quarantine Volume",
                    "severity": "WARNING",
                    "status": "ACTIVE",
                    "message": "Quarantine contains 1 parquet files pending investigation.",
                    "source": "silver",
                    "timestamp": "2026-04-27T00:00:00+00:00",
                    "metadata": {"quarantine_count": 1},
                }
            ],
            "recent": [],
            "summary": {
                "total": 1,
                "critical": 0,
                "warning": 1,
                "info": 0,
                "sources": ["silver"],
            },
        },
        "controls": {
            "jobs": [
                {
                    "run_id": None,
                    "job_id": "inventory_producer",
                    "name": "Inventory Producer",
                    "stage": "producer",
                    "domain": "inventory",
                    "description": "Emit deterministic inventory events into Kafka.",
                    "command": "python -m medwarehouse produce inventory --max-events 10",
                    "long_running": False,
                    "status": "idle",
                    "started_at": None,
                    "finished_at": None,
                    "return_code": None,
                    "pid": None,
                    "message": None,
                    "logs": [],
                }
            ],
            "infra": {
                "run_id": None,
                "status": "idle",
                "current_action": None,
                "started_at": None,
                "finished_at": None,
                "return_code": None,
                "pid": None,
                "message": None,
                "logs": [],
            },
        },
        "catalog": {
            "jobs": [
                {
                    "job_id": "inventory_producer",
                    "name": "Inventory Producer",
                    "domain": "inventory",
                    "stage": "producer",
                    "description": "Emit deterministic inventory events into Kafka.",
                    "long_running": False,
                    "command": "python -m medwarehouse produce inventory --max-events 10",
                }
            ]
        },
        "probes": {},
    }
    full["legacy"] = {
        "snapshot": {
            "environment": full["environment"],
            "infrastructure": full["metrics"]["infra"],
            "artifacts": full["metrics"]["artifacts"],
            "warehouse": full["metrics"]["warehouse"],
            "coverage": full["coverage"],
            "roles": full["roles"]["configured"],
            "orchestration": full["orchestration"],
            "jobs": full["catalog"]["jobs"],
        }
    }
    return full


class FakeControlPlane:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def job_snapshots(self) -> list[dict]:
        return _sample_full_status()["controls"]["jobs"]

    def run_job_snapshot(self, job_id: str) -> dict:
        return self.job_snapshots()[0]

    def infra_snapshot(self) -> dict:
        return _sample_full_status()["controls"]["infra"]

    def start_job(self, job_id: str) -> tuple[bool, str]:
        self.calls.append(("start_job", job_id))
        return True, f"started {job_id}"

    def stop_job(self, job_id: str) -> tuple[bool, str]:
        self.calls.append(("stop_job", job_id))
        return True, f"stopped {job_id}"

    def run_infra_action(self, action: str) -> tuple[bool, str]:
        self.calls.append(("infra", action))
        return True, f"infra {action}"


class FakeStatusService:
    def __init__(self) -> None:
        self.invalidate_calls = 0

    def get_full_status(self, *, force_refresh: bool = False) -> dict:
        status = _sample_full_status()
        status["cache"]["served_from_cache"] = not force_refresh
        return status

    def get_pipeline_status(self, *, force_refresh: bool = False) -> dict:
        full = self.get_full_status(force_refresh=force_refresh)
        return {
            "generated_at": full["generated_at"],
            "health": full["health"],
            "pipeline": full["metrics"]["pipeline"],
            "alerts": full["alerts"],
        }

    def get_infra_status(self, *, force_refresh: bool = False) -> dict:
        full = self.get_full_status(force_refresh=force_refresh)
        return {
            "generated_at": full["generated_at"],
            "health": full["health"],
            "infra": full["metrics"]["infra"],
            "control": full["controls"]["infra"],
            "alerts": full["alerts"],
        }

    def get_alerts(self, *, force_refresh: bool = False) -> dict:
        return self.get_full_status(force_refresh=force_refresh)["alerts"]

    def platform_snapshot(self, *, force_refresh: bool = False) -> dict:
        return self.get_full_status(force_refresh=force_refresh)["legacy"]["snapshot"]

    def invalidate(self) -> None:
        self.invalidate_calls += 1


class StaticProbe:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def collect(self) -> dict:
        return self._payload


class PlatformStatusTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_status_service_aggregates_probe_payloads_and_alerts(self) -> None:
        control_plane = FakeControlPlane()
        service = StatusService(control_plane=control_plane, ttl_seconds=0)
        service._probes = {
            "infra": StaticProbe(
                {
                    "available": True,
                    "overall_status": "stopped",
                    "error": None,
                    "compose_command": "docker-compose",
                    "services": [
                        {"name": "postgres", "status": "stopped"},
                        {"name": "kafka", "status": "stopped"},
                    ],
                }
            ),
            "warehouse": StaticProbe(
                {
                    "reachable": False,
                    "target": "jdbc:postgresql://localhost/analytics",
                    "schemas": [],
                    "roles": [],
                    "expected_tables": ["fact_inventory_events"],
                    "expected_views": ["v_inventory_snapshot"],
                    "present_tables": [],
                    "present_views": [],
                    "missing_tables": ["fact_inventory_events"],
                    "missing_views": ["v_inventory_snapshot"],
                    "counts": {},
                    "last_updates": {},
                    "quality_issues": [],
                    "quality_issue_total": 0,
                    "error": "offline",
                    "status": "warning",
                    "checked_at": "2026-04-27T00:00:00+00:00",
                    "probe": "warehouse",
                }
            ),
            "pipeline": StaticProbe(
                {
                    "stages": {
                        "bronze": {
                            "count": 8,
                            "last_update": "2026-04-20T00:00:00+00:00",
                            "path": "/tmp/bronze",
                        },
                        "silver": {
                            "count": 0,
                            "last_update": "2026-04-20T00:00:00+00:00",
                            "path": "/tmp/silver",
                            "quarantine": 3,
                            "quarantine_last_update": "2026-04-20T00:00:00+00:00",
                        },
                        "staging": {"count": 0, "last_update": None},
                        "warehouse": {
                            "reachable": False,
                            "fact_count": None,
                            "snapshot_count": None,
                            "last_update": None,
                        },
                    },
                    "consistency": {
                        "bronze_vs_silver_gap": 8,
                        "silver_vs_staging_gap": 0,
                        "staging_vs_fact_gap": None,
                        "fact_vs_snapshot_gap": None,
                    },
                }
            ),
            "artifacts": StaticProbe(
                {
                    "artifacts": {
                        "bronze": {
                            "path": "/tmp/bronze",
                            "exists": True,
                            "parquet_files": 8,
                            "size_bytes": 80,
                            "updated_at": "2026-04-20T00:00:00+00:00",
                            "count": 8,
                        },
                        "silver": {
                            "path": "/tmp/silver",
                            "exists": True,
                            "parquet_files": 0,
                            "size_bytes": 0,
                            "updated_at": "2026-04-20T00:00:00+00:00",
                            "count": 0,
                        },
                        "quarantine": {
                            "path": "/tmp/quarantine",
                            "exists": True,
                            "parquet_files": 3,
                            "size_bytes": 30,
                            "updated_at": "2026-04-20T00:00:00+00:00",
                            "count": 3,
                        },
                    }
                }
            ),
            "jobs": StaticProbe(
                {
                    "items": control_plane.job_snapshots(),
                    "summary": {
                        "total": 1,
                        "running": 0,
                        "failed": 0,
                        "idle": 1,
                        "by_status": {"idle": 1},
                    },
                }
            ),
            "airflow": StaticProbe(
                {
                    "dag_id": "inventory_gold_pipeline",
                    "dag_path": "/tmp/dag.py",
                    "dag_exists": True,
                    "schedule": "manual",
                    "metadata_available": True,
                    "metadata_path": "/tmp/airflow.db",
                    "last_run": {
                        "state": "failed",
                        "start_date": "2026-04-20T00:00:00+00:00",
                    },
                    "error": None,
                }
            ),
        }

        status = service.get_full_status(force_refresh=True)

        self.assertIn("metrics", status)
        self.assertIn("alerts", status)
        self.assertIn("controls", status)
        self.assertEqual(status["metrics"]["pipeline"]["silver"]["quarantine"], 3)
        self.assertEqual(status["metrics"]["infra"]["service_map"]["kafka"], "stopped")
        self.assertEqual(status["health"]["status"], "critical")
        active_ids = {alert["id"] for alert in status["alerts"]["active"]}
        self.assertIn("infra:kafka", active_ids)
        self.assertIn("airflow:dag_failure", active_ids)
        self.assertIn("pipeline:bronze_silver_gap", active_ids)

    def test_get_pipeline_status_returns_pipeline_subset(self) -> None:
        service = FakeStatusService()
        payload = service.get_pipeline_status(force_refresh=True)

        self.assertIn("pipeline", payload)
        self.assertIn("alerts", payload)
        self.assertEqual(payload["pipeline"]["bronze"]["count"], 4)

    def test_docker_service_status_handles_missing_docker(self) -> None:
        with patch("subprocess.run", side_effect=FileNotFoundError("docker")):
            status = docker_service_status()

        self.assertFalse(status["available"])
        self.assertEqual(status["overall_status"], "unavailable")


class PlatformAppTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_dashboard_page_renders(self) -> None:
        app = create_platform_app(
            control_plane=FakeControlPlane(),
            status_service=FakeStatusService(),
        )
        response = app.test_client().get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Control Plane", response.data)
        self.assertIn(b"Operational Monitoring Dashboard", response.data)

    def test_all_monitoring_pages_render(self) -> None:
        app = create_platform_app(
            control_plane=FakeControlPlane(),
            status_service=FakeStatusService(),
        )
        client = app.test_client()

        for path in (
            "/pipeline",
            "/warehouse",
            "/artifacts",
            "/infrastructure",
            "/jobs",
            "/runbook",
        ):
            response = client.get(path)
            self.assertEqual(response.status_code, 200, path)

    def test_health_endpoint_returns_ok(self) -> None:
        app = create_platform_app(
            control_plane=FakeControlPlane(),
            status_service=FakeStatusService(),
        )
        response = app.test_client().get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"status": "ok"})

    def test_api_status_returns_legacy_snapshot_jobs_and_infra(self) -> None:
        app = create_platform_app(
            control_plane=FakeControlPlane(),
            status_service=FakeStatusService(),
        )
        response = app.test_client().get("/api/status")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("snapshot", payload)
        self.assertIn("jobs", payload)
        self.assertIn("infra", payload)
        self.assertIn("alerts", payload)

    def test_new_status_endpoints_return_structured_payloads(self) -> None:
        app = create_platform_app(
            control_plane=FakeControlPlane(),
            status_service=FakeStatusService(),
        )
        client = app.test_client()

        full = client.get("/api/status/full").get_json()
        pipeline = client.get("/api/status/pipeline").get_json()
        infra = client.get("/api/status/infra").get_json()
        alerts = client.get("/api/status/alerts").get_json()

        self.assertIn("metrics", full)
        self.assertIn("pipeline", pipeline)
        self.assertIn("infra", infra)
        self.assertIn("active", alerts)

    def test_api_job_start_route_calls_control_plane(self) -> None:
        control_plane = FakeControlPlane()
        app = create_platform_app(
            control_plane=control_plane,
            status_service=FakeStatusService(),
        )
        response = app.test_client().post("/api/jobs/inventory_producer/start")

        self.assertEqual(response.status_code, 200)
        self.assertIn(("start_job", "inventory_producer"), control_plane.calls)
        self.assertTrue(response.get_json()["ok"])

    def test_api_job_stop_route_calls_control_plane(self) -> None:
        control_plane = FakeControlPlane()
        app = create_platform_app(
            control_plane=control_plane,
            status_service=FakeStatusService(),
        )
        response = app.test_client().post("/api/jobs/inventory_bronze/stop")

        self.assertEqual(response.status_code, 200)
        self.assertIn(("stop_job", "inventory_bronze"), control_plane.calls)
        self.assertTrue(response.get_json()["ok"])

    def test_api_infra_route_calls_control_plane_and_invalidates_status(self) -> None:
        control_plane = FakeControlPlane()
        status_service = FakeStatusService()
        app = create_platform_app(
            control_plane=control_plane,
            status_service=status_service,
        )
        response = app.test_client().post("/api/infra/start")

        self.assertEqual(response.status_code, 200)
        self.assertIn(("infra", "start"), control_plane.calls)
        self.assertEqual(status_service.invalidate_calls, 1)
        self.assertTrue(response.get_json()["ok"])

    def test_cli_platform_serve_invokes_server(self) -> None:
        with patch("medwarehouse.cli.run_platform_server") as mock_run:
            result = main(["platform", "serve", "--host", "127.0.0.1", "--port", "8787"])

        self.assertEqual(result, 0)
        mock_run.assert_called_once_with(host="127.0.0.1", port=8787, debug=False)


class ControlPlaneServiceTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_start_job_persists_run_and_rejects_duplicate_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            service = ControlPlaneService(store=store, supervisor=supervisor)

            ok, _ = service.start_job("inventory_producer")
            duplicate_ok, duplicate_message = service.start_job("inventory_producer")
            snapshots = service.job_snapshots()
            producer_snapshot = next(
                item for item in snapshots if item["job_id"] == "inventory_producer"
            )

        self.assertTrue(ok)
        self.assertFalse(duplicate_ok)
        self.assertIn("already running", duplicate_message)
        supervisor.launch.assert_called_once()
        self.assertEqual(producer_snapshot["status"], "pending")

    def test_stop_job_rejects_idle_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            service = ControlPlaneService(store=store, supervisor=supervisor)

            ok, message = service.stop_job("inventory_bronze")

        self.assertFalse(ok)
        self.assertIn("not running", message)

    def test_unknown_job_ids_are_rejected_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            service = ControlPlaneService(store=store, supervisor=supervisor)

            start_ok, start_message = service.start_job("missing_job")
            stop_ok, stop_message = service.stop_job("missing_job")

        self.assertFalse(start_ok)
        self.assertFalse(stop_ok)
        self.assertIn("Unknown job", start_message)
        self.assertIn("Unknown job", stop_message)

    def test_infra_actions_reject_unknown_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            service = ControlPlaneService(store=store, supervisor=supervisor)

            ok, message = service.run_infra_action("restart")

        self.assertFalse(ok)
        self.assertIn("Unknown infra action", message)


class PlatformApiEdgeCaseTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_settings.cache_clear()

    def test_duplicate_job_start_is_rejected_through_public_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            control_plane = ControlPlaneService(store=store, supervisor=supervisor)
            app = create_platform_app(
                control_plane=control_plane,
                status_service=FakeStatusService(),
            )
            client = app.test_client()

            first = client.post("/api/jobs/inventory_producer/start").get_json()
            second = client.post("/api/jobs/inventory_producer/start").get_json()

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertIn("already running", second["message"])

    def test_idle_stop_is_rejected_through_public_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            control_plane = ControlPlaneService(store=store, supervisor=supervisor)
            app = create_platform_app(
                control_plane=control_plane,
                status_service=FakeStatusService(),
            )
            response = app.test_client().post("/api/jobs/inventory_bronze/stop")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["ok"])
        self.assertIn("not running", payload["message"])

    def test_unknown_job_routes_return_safe_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            control_plane = ControlPlaneService(store=store, supervisor=supervisor)
            app = create_platform_app(
                control_plane=control_plane,
                status_service=FakeStatusService(),
            )
            client = app.test_client()

            start_payload = client.post("/api/jobs/not_real/start").get_json()
            stop_payload = client.post("/api/jobs/not_real/stop").get_json()

        self.assertFalse(start_payload["ok"])
        self.assertFalse(stop_payload["ok"])
        self.assertIn("Unknown job", start_payload["message"])
        self.assertIn("Unknown job", stop_payload["message"])

    def test_unknown_infra_action_returns_safe_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ControlPlaneStore(Path(tmp_dir) / "control-plane.db")
            supervisor = Mock()
            control_plane = ControlPlaneService(store=store, supervisor=supervisor)
            app = create_platform_app(
                control_plane=control_plane,
                status_service=FakeStatusService(),
            )
            response = app.test_client().post("/api/infra/restart")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["ok"])
        self.assertIn("Unknown infra action", payload["message"])


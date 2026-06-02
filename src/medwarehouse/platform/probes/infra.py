from __future__ import annotations

import subprocess
from typing import Any

from medwarehouse.config import get_settings
from medwarehouse.platform.compose import resolve_compose_command
from medwarehouse.platform.probes.base import Probe


# Services that are always expected when the stack is running.
CORE_SERVICES = ("postgres", "zookeeper", "kafka")

# Services that are only present when --with-airflow was passed to start_services.sh.
# Their absence does not constitute a warning.
OPTIONAL_SERVICES = ("airflow-init", "airflow-webserver", "airflow-scheduler")

# All known services, used only for full-inventory reporting.
ALL_SERVICES = CORE_SERVICES + OPTIONAL_SERVICES

# Kept for backward compatibility with any external code that imported this name.
REQUIRED_SERVICES = ALL_SERVICES


class InfraProbe(Probe):
    name = "infra"

    def _collect(self) -> dict[str, Any]:
        return collect_infra_status()


def collect_infra_status() -> dict[str, Any]:
    resolution = resolve_compose_command()
    if not resolution.available or resolution.command is None:
        return {
            "status": "warning",
            "available": False,
            "overall_status": "unavailable",
            "error": resolution.message,
            "compose_command": None,
            "services": [{"name": s, "status": "unknown"} for s in ALL_SERVICES],
        }

    try:
        result = subprocess.run(
            [*resolution.command, "ps", "--services", "--status", "running"],
            cwd=str(get_settings().paths.project_root),
            check=True,
            capture_output=True,
            text=True,
        )
        running: set[str] = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    except Exception as exc:
        return {
            "status": "warning",
            "available": False,
            "overall_status": "unavailable",
            "error": str(exc),
            "compose_command": " ".join(resolution.command),
            "services": [{"name": s, "status": "unknown"} for s in ALL_SERVICES],
        }

    services = [
        {
            "name": name,
            "status": "running" if name in running else "stopped",
            "required": name in CORE_SERVICES,
        }
        for name in ALL_SERVICES
    ]

    # Overall status is derived from CORE services only.
    core_running = sum(1 for s in services if s["required"] and s["status"] == "running")
    core_total = len(CORE_SERVICES)
    if core_running == core_total:
        overall_status = "running"
    elif core_running == 0:
        overall_status = "stopped"
    else:
        overall_status = "partial"

    # Airflow is only considered "running" if both webserver and scheduler are up.
    airflow_running = all(
        s["status"] == "running"
        for s in services
        if s["name"] in ("airflow-webserver", "airflow-scheduler")
    )

    return {
        "status": "ok",
        "available": True,
        "overall_status": overall_status,
        "error": None,
        "compose_command": " ".join(resolution.command),
        "services": services,
        "kafka_running": any(s["name"] == "kafka" and s["status"] == "running" for s in services),
        "postgres_running": any(s["name"] == "postgres" and s["status"] == "running" for s in services),
        "airflow_running": airflow_running,
    }

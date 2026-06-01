from __future__ import annotations

import subprocess
from typing import Any

from medwarehouse.config import get_settings
from medwarehouse.platform.compose import resolve_compose_command
from medwarehouse.platform.probes.base import Probe


REQUIRED_SERVICES = (
    "postgres",
    "zookeeper",
    "kafka",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
)


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
            "services": [{"name": name, "status": "unknown"} for name in REQUIRED_SERVICES],
        }

    try:
        result = subprocess.run(
            [*resolution.command, "ps", "--services", "--status", "running"],
            cwd=str(get_settings().paths.project_root),
            check=True,
            capture_output=True,
            text=True,
        )
        running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    except Exception as exc:
        return {
            "status": "warning",
            "available": False,
            "overall_status": "unavailable",
            "error": str(exc),
            "compose_command": " ".join(resolution.command),
            "services": [{"name": name, "status": "unknown"} for name in REQUIRED_SERVICES],
        }

    services = [
        {"name": name, "status": "running" if name in running else "stopped"}
        for name in REQUIRED_SERVICES
    ]
    running_count = sum(1 for service in services if service["status"] == "running")
    if running_count == len(REQUIRED_SERVICES):
        overall_status = "running"
    elif running_count == 0:
        overall_status = "stopped"
    else:
        overall_status = "partial"

    return {
        "status": "ok",
        "available": True,
        "overall_status": overall_status,
        "error": None,
        "compose_command": " ".join(resolution.command),
        "services": services,
    }


from __future__ import annotations

from medwarehouse.platform.control import get_control_plane_service
from medwarehouse.platform.probes.infra import REQUIRED_SERVICES, collect_infra_status


class InfraController:
    def run(self, action: str) -> tuple[bool, str]:
        return get_control_plane_service().run_infra_action(action)

    def snapshot(self) -> dict[str, object]:
        return get_control_plane_service().infra_snapshot()


def docker_service_status() -> dict[str, object]:
    return collect_infra_status()


infra_controller = InfraController()

__all__ = ["REQUIRED_SERVICES", "InfraController", "docker_service_status", "infra_controller"]

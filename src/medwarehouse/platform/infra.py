from __future__ import annotations

from medwarehouse.platform.probes.infra import (
    CORE_SERVICES,
    OPTIONAL_SERVICES,
    collect_infra_status,
)


def docker_service_status() -> dict[str, object]:
    """Convenience alias kept for backward compatibility with existing tests."""
    return collect_infra_status()


__all__ = ["CORE_SERVICES", "OPTIONAL_SERVICES", "collect_infra_status", "docker_service_status"]

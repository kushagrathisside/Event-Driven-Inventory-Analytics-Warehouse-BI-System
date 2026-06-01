from __future__ import annotations

from typing import Any

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.filesystem import summarize_artifact_tree


class ArtifactProbe(Probe):
    name = "artifacts"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "artifacts": {
                "bronze": summarize_artifact_tree(self._settings.paths.bronze_inventory_path),
                "silver": summarize_artifact_tree(self._settings.paths.silver_inventory_path),
                "quarantine": summarize_artifact_tree(
                    self._settings.paths.quarantine_inventory_path
                ),
            },
        }


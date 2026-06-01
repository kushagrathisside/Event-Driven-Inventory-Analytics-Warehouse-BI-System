from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from medwarehouse.platform.utils.time import utc_now


ACTIVE_RUN_STATES = frozenset(
    {
        "pending",
        "starting",
        "running",
        "stop_requested",
        "stopping",
        "orphaned",
    }
)

TERMINAL_RUN_STATES = frozenset(
    {
        "succeeded",
        "failed",
        "stopped",
    }
)


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    kind: str
    command_id: str
    name: str
    domain: str
    stage: str
    description: str
    command_display: str
    long_running: bool
    status: str
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    pid: int | None = None
    pgid: int | None = None
    return_code: int | None = None
    message: str | None = None

    @classmethod
    def create(
        cls,
        *,
        kind: str,
        command_id: str,
        name: str,
        domain: str,
        stage: str,
        description: str,
        command_display: str,
        long_running: bool,
        status: str = "pending",
        message: str | None = None,
    ) -> RunRecord:
        timestamp = utc_now()
        return cls(
            run_id=str(uuid4()),
            kind=kind,
            command_id=command_id,
            name=name,
            domain=domain,
            stage=stage,
            description=description,
            command_display=command_display,
            long_running=long_running,
            status=status,
            created_at=timestamp,
            updated_at=timestamp,
            message=message,
        )

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_RUN_STATES

    def to_job_snapshot(self, logs: list[str]) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "job_id": self.command_id,
            "name": self.name,
            "stage": self.stage,
            "domain": self.domain,
            "description": self.description,
            "command": self.command_display,
            "long_running": self.long_running,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "return_code": self.return_code,
            "pid": self.pid,
            "message": self.message,
            "logs": logs,
        }

    def to_infra_snapshot(self, logs: list[str]) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "current_action": self.command_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "return_code": self.return_code,
            "pid": self.pid,
            "message": self.message,
            "logs": logs,
        }


def idle_job_snapshot(
    *,
    job_id: str,
    name: str,
    stage: str,
    domain: str,
    description: str,
    command: str,
    long_running: bool,
) -> dict[str, Any]:
    return {
        "run_id": None,
        "job_id": job_id,
        "name": name,
        "stage": stage,
        "domain": domain,
        "description": description,
        "command": command,
        "long_running": long_running,
        "status": "idle",
        "started_at": None,
        "finished_at": None,
        "return_code": None,
        "pid": None,
        "message": None,
        "logs": [],
    }


def idle_infra_snapshot() -> dict[str, Any]:
    return {
        "run_id": None,
        "status": "idle",
        "current_action": None,
        "started_at": None,
        "finished_at": None,
        "return_code": None,
        "pid": None,
        "message": None,
        "logs": [],
    }


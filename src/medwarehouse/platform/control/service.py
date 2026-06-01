from __future__ import annotations

import os
import sys
import threading
from functools import lru_cache
from pathlib import Path

from medwarehouse.config import get_settings
from medwarehouse.platform.models import RunRecord, idle_infra_snapshot, idle_job_snapshot

from medwarehouse.platform.control.catalog import JobSpec, get_job_spec, job_specs
from medwarehouse.platform.control.runtime import ProcessSupervisor
from medwarehouse.platform.control.store import ControlPlaneStore


class ControlPlaneService:
    def __init__(
        self,
        *,
        store: ControlPlaneStore,
        supervisor: ProcessSupervisor,
    ) -> None:
        self._store = store
        self._supervisor = supervisor
        self._lock = threading.Lock()

    def start_job(self, job_id: str) -> tuple[bool, str]:
        spec = get_job_spec(job_id)
        if spec is None:
            return False, f"Unknown job '{job_id}'."

        command = [sys.executable, "-m", "medwarehouse", *spec.command]
        command_display = "python -m medwarehouse " + " ".join(spec.command)
        return self._start_command(
            kind="job",
            command_id=job_id,
            name=spec.name,
            domain=spec.domain,
            stage=spec.stage,
            description=spec.description,
            long_running=spec.long_running,
            command=command,
            command_display=command_display,
            cwd=self._project_root(),
            env=self._build_env(),
        )

    def stop_job(self, job_id: str) -> tuple[bool, str]:
        spec = get_job_spec(job_id)
        if spec is None:
            return False, f"Unknown job '{job_id}'."

        with self._lock:
            run = self._store.get_active_run("job", job_id)
            if run is None:
                return False, f"{spec.name} is not running."
            return self._supervisor.request_stop(run.run_id)

    def job_snapshots(self) -> list[dict[str, object]]:
        snapshots: list[dict[str, object]] = []
        for spec in job_specs():
            run = self._store.get_latest_run("job", spec.job_id)
            if run is None:
                snapshots.append(self._idle_snapshot_for_job(spec))
                continue
            logs = self._store.get_run_logs(run.run_id, limit=200)
            snapshots.append(run.to_job_snapshot(logs))
        return snapshots

    def run_job_snapshot(self, job_id: str) -> dict[str, object] | None:
        spec = get_job_spec(job_id)
        if spec is None:
            return None
        run = self._store.get_latest_run("job", job_id)
        if run is None:
            return self._idle_snapshot_for_job(spec)
        return run.to_job_snapshot(self._store.get_run_logs(run.run_id, limit=200))

    def run_infra_action(self, action: str) -> tuple[bool, str]:
        if action not in {"start", "stop"}:
            return False, f"Unknown infra action '{action}'."

        settings = get_settings()
        scripts = {
            "start": settings.paths.project_root / "scripts" / "start_services.sh",
            "stop": settings.paths.project_root / "scripts" / "stop_services.sh",
        }
        command = [str(scripts[action])]
        command_display = str(scripts[action])
        return self._start_command(
            kind="infra",
            command_id=action,
            name=f"Infrastructure {action.title()}",
            domain="infrastructure",
            stage="infrastructure",
            description=f"Run the infrastructure {action} script.",
            long_running=False,
            command=command,
            command_display=command_display,
            cwd=settings.paths.project_root,
            env=dict(os.environ),
        )

    def infra_snapshot(self) -> dict[str, object]:
        run = self._store.get_latest_run_for_kind("infra")
        if run is None:
            return idle_infra_snapshot()
        return run.to_infra_snapshot(self._store.get_run_logs(run.run_id, limit=200))

    def _start_command(
        self,
        *,
        kind: str,
        command_id: str,
        name: str,
        domain: str,
        stage: str,
        description: str,
        long_running: bool,
        command: list[str],
        command_display: str,
        cwd: Path,
        env: dict[str, str],
    ) -> tuple[bool, str]:
        with self._lock:
            active = (
                self._store.get_active_run(kind, command_id)
                if kind == "job"
                else self._store.get_active_run("infra")
            )
            if active is not None:
                return False, f"{active.name} is already running."

            run = RunRecord.create(
                kind=kind,
                command_id=command_id,
                name=name,
                domain=domain,
                stage=stage,
                description=description,
                command_display=command_display,
                long_running=long_running,
                message="Accepted for execution.",
            )
            self._store.create_run(run)
            self._store.append_log(run.run_id, f"[{run.created_at}] accepted")
            self._supervisor.launch(run_id=run.run_id, command=command, cwd=cwd, env=env)
            return True, f"Started {name}."

    def _build_env(self) -> dict[str, str]:
        settings = get_settings()
        env = dict(os.environ)
        src_path = str(settings.paths.project_root / "src")
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = src_path if not existing else f"{src_path}{os.pathsep}{existing}"
        return env

    @staticmethod
    def _project_root() -> Path:
        return get_settings().paths.project_root

    @staticmethod
    def _idle_snapshot_for_job(spec: JobSpec) -> dict[str, object]:
        return idle_job_snapshot(
            job_id=spec.job_id,
            name=spec.name,
            stage=spec.stage,
            domain=spec.domain,
            description=spec.description,
            command="python -m medwarehouse " + " ".join(spec.command),
            long_running=spec.long_running,
        )


def _default_store_path() -> Path:
    settings = get_settings()
    override = os.environ.get("MW_CONTROL_PLANE_DB_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return (settings.paths.data_root / "control_plane" / "control_plane.sqlite3").resolve()


@lru_cache(maxsize=1)
def get_control_plane_store() -> ControlPlaneStore:
    return ControlPlaneStore(_default_store_path())


@lru_cache(maxsize=1)
def get_process_supervisor() -> ProcessSupervisor:
    return ProcessSupervisor(get_control_plane_store())


@lru_cache(maxsize=1)
def get_control_plane_service() -> ControlPlaneService:
    return ControlPlaneService(
        store=get_control_plane_store(),
        supervisor=get_process_supervisor(),
    )


def reset_control_plane_services() -> None:
    get_control_plane_service.cache_clear()
    get_process_supervisor.cache_clear()
    get_control_plane_store.cache_clear()


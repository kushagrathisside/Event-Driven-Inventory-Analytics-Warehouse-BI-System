from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from pathlib import Path

from medwarehouse.platform.models import ACTIVE_RUN_STATES, RunRecord, utc_now

from medwarehouse.platform.control.store import ControlPlaneStore


class ProcessSupervisor:
    def __init__(self, store: ControlPlaneStore, *, stop_timeout_seconds: float = 10.0) -> None:
        self._store = store
        self._stop_timeout_seconds = stop_timeout_seconds
        self._lock = threading.Lock()
        self._active_processes: dict[str, subprocess.Popen[str]] = {}
        self.reconcile_runs()

    def launch(
        self,
        *,
        run_id: str,
        command: list[str],
        cwd: Path,
        env: dict[str, str],
    ) -> None:
        worker = threading.Thread(
            target=self._run_process,
            args=(run_id, command, cwd, env),
            daemon=True,
        )
        worker.start()

    def request_stop(self, run_id: str) -> tuple[bool, str]:
        run = self._store.get_run(run_id)
        if run is None:
            return False, f"Unknown run '{run_id}'."
        if run.status not in ACTIVE_RUN_STATES:
            return False, f"{run.name} is not running."

        if run.pid is None and run.status in {"pending", "starting"} and not self._is_managed(run_id):
            self._store.update_run(
                run_id,
                status="stopped",
                finished_at=utc_now(),
                message="Stopped before the process was fully started.",
            )
            self._store.append_log(run_id, f"[{utc_now()}] stopped before pid assignment")
            return True, f"Stopping {run.name}."

        self._store.update_run(run_id, status="stop_requested", message="Stop requested.")
        self._store.append_log(run_id, f"[{utc_now()}] stop requested")

        sent = self._signal_run(run, signal.SIGTERM)
        if not sent:
            return False, f"Unable to signal {run.name}; no pid is available."

        self._store.update_run(run_id, status="stopping", message="Termination signal sent.")
        self._store.append_log(run_id, f"[{utc_now()}] termination signal sent")

        if not self._is_managed(run_id):
            watcher = threading.Thread(
                target=self._wait_for_external_stop,
                args=(run_id, run.pid, run.pgid),
                daemon=True,
            )
            watcher.start()

        escalator = threading.Thread(
            target=self._escalate_stop_if_needed,
            args=(run_id, run.pid, run.pgid),
            daemon=True,
        )
        escalator.start()
        return True, f"Stopping {run.name}."

    def reconcile_runs(self) -> None:
        for run in self._store.list_active_runs():
            if run.pid is not None and self._pid_exists(run.pid):
                self._store.update_run(
                    run.run_id,
                    status="orphaned",
                    message=(
                        "The control plane restarted while this process was active. "
                        "It may still be running and should be stopped or inspected."
                    ),
                )
                self._store.append_log(
                    run.run_id,
                    f"[{utc_now()}] recovered as orphaned pid={run.pid}",
                )
            else:
                self._store.update_run(
                    run.run_id,
                    status="failed",
                    finished_at=utc_now(),
                    message="The control plane restarted before this run completed.",
                )
                self._store.append_log(
                    run.run_id,
                    f"[{utc_now()}] marked failed during recovery",
                )

    def _run_process(
        self,
        run_id: str,
        command: list[str],
        cwd: Path,
        env: dict[str, str],
    ) -> None:
        current = self._store.get_run(run_id)
        if current is None:
            return
        if current.status == "stop_requested":
            self._store.update_run(
                run_id,
                status="stopped",
                finished_at=utc_now(),
                message="Stopped before the process was started.",
            )
            self._store.append_log(run_id, f"[{utc_now()}] cancelled before start")
            return

        self._store.update_run(run_id, status="starting", message="Starting process.")
        self._store.append_log(run_id, f"[{utc_now()}] starting command={' '.join(command)}")

        process: subprocess.Popen[str] | None = None
        try:
            process = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            pgid = None
            try:
                pgid = os.getpgid(process.pid)
            except OSError:
                pgid = None

            with self._lock:
                self._active_processes[run_id] = process

            self._store.update_run(
                run_id,
                status="running",
                started_at=utc_now(),
                pid=process.pid,
                pgid=pgid,
                message="Process is running.",
            )
            self._store.append_log(run_id, f"[{utc_now()}] started pid={process.pid}")

            if process.stdout is not None:
                for line in process.stdout:
                    cleaned = line.rstrip()
                    if cleaned:
                        self._store.append_log(run_id, cleaned)

            return_code = process.wait()
            final = self._store.get_run(run_id)
            status = "succeeded"
            message = "Process completed successfully."
            if final is not None and final.status in {"stop_requested", "stopping"}:
                status = "stopped"
                message = "Process stopped by operator."
            elif return_code != 0:
                status = "failed"
                message = f"Process exited with return code {return_code}."

            self._store.update_run(
                run_id,
                status=status,
                finished_at=utc_now(),
                return_code=return_code,
                message=message,
            )
            self._store.append_log(run_id, f"[{utc_now()}] exited rc={return_code}")
        except Exception as exc:
            self._store.update_run(
                run_id,
                status="failed",
                finished_at=utc_now(),
                message=f"Failed to launch process: {exc}",
            )
            self._store.append_log(run_id, f"[{utc_now()}] launch failed: {exc}")
        finally:
            if process is not None:
                with self._lock:
                    existing = self._active_processes.get(run_id)
                    if existing is process:
                        self._active_processes.pop(run_id, None)

    def _is_managed(self, run_id: str) -> bool:
        with self._lock:
            process = self._active_processes.get(run_id)
        return process is not None and process.poll() is None

    def _signal_run(self, run: RunRecord, sig: signal.Signals) -> bool:
        if run.pgid is not None:
            try:
                os.killpg(run.pgid, sig)
                return True
            except ProcessLookupError:
                return False
            except OSError:
                return False

        if run.pid is not None:
            try:
                os.kill(run.pid, sig)
                return True
            except ProcessLookupError:
                return False
            except OSError:
                return False

        return False

    def _wait_for_external_stop(
        self,
        run_id: str,
        pid: int | None,
        pgid: int | None,
    ) -> None:
        if pid is None:
            return

        deadline = time.monotonic() + self._stop_timeout_seconds
        while time.monotonic() < deadline:
            if not self._pid_exists(pid):
                self._store.update_run(
                    run_id,
                    status="stopped",
                    finished_at=utc_now(),
                    message="Process stopped by operator.",
                )
                self._store.append_log(run_id, f"[{utc_now()}] external process stopped")
                return
            time.sleep(0.5)

        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] sent SIGKILL to pgid={pgid}")
            except OSError:
                pass
        elif pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] sent SIGKILL to pid={pid}")
            except OSError:
                pass

    def _escalate_stop_if_needed(
        self,
        run_id: str,
        pid: int | None,
        pgid: int | None,
    ) -> None:
        if pid is None:
            return

        deadline = time.monotonic() + self._stop_timeout_seconds
        while time.monotonic() < deadline:
            if not self._pid_exists(pid):
                return
            time.sleep(0.5)

        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] sent SIGKILL to pgid={pgid}")
            except OSError:
                return
        else:
            try:
                os.kill(pid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] sent SIGKILL to pid={pid}")
            except OSError:
                return

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True


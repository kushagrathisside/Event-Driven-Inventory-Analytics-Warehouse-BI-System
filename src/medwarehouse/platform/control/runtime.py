from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from pathlib import Path

from medwarehouse.logging import get_logger
from medwarehouse.platform.models import ACTIVE_RUN_STATES, RunRecord, utc_now
from medwarehouse.platform.control.store import ControlPlaneStore


logger = get_logger(__name__)


class ProcessSupervisor:
    """
    Launches, monitors, and stops subprocess-based pipeline jobs.

    Stop sequence per run:
      1. SIGTERM is sent to the process group.
      2. A single watcher thread polls until the process exits naturally.
      3. If stop_timeout_seconds elapse without exit, SIGKILL is sent.
      4. The store is updated exactly once when the process exits.

    Design choices:
    - One watcher thread per stop request (not two).
    - Managed processes (_active_processes) have their watcher inside
      _run_process; for externally-observed processes a separate watcher
      thread is started from request_stop.
    """

    def __init__(self, store: ControlPlaneStore, *, stop_timeout_seconds: float = 10.0) -> None:
        self._store = store
        self._stop_timeout = stop_timeout_seconds
        self._lock = threading.Lock()
        self._active_processes: dict[str, subprocess.Popen[str]] = {}
        self._reconcile_runs()

    # ── Public API ────────────────────────────────────────────────────────────

    def launch(self, *, run_id: str, command: list[str], cwd: Path, env: dict[str, str]) -> None:
        thread = threading.Thread(
            target=self._run_process,
            args=(run_id, command, cwd, env),
            daemon=True,
            name=f"supervisor-run-{run_id[:8]}",
        )
        thread.start()

    def request_stop(self, run_id: str) -> tuple[bool, str]:
        run = self._store.get_run(run_id)
        if run is None:
            return False, f"Unknown run '{run_id}'."
        if run.status not in ACTIVE_RUN_STATES:
            return False, f"{run.name} is not running."

        # Fast path: run never got a PID and we own it — cancel in place.
        if run.pid is None and run.status in {"pending", "starting"} and not self._is_managed(run_id):
            self._store.update_run(
                run_id,
                status="stopped",
                finished_at=utc_now(),
                message="Stopped before the process was fully started.",
            )
            self._store.append_log(run_id, f"[{utc_now()}] stopped before pid assignment")
            logger.info("Run %s cancelled before start (no pid)", run_id[:8])
            return True, f"Stopping {run.name}."

        self._store.update_run(run_id, status="stop_requested", message="Stop requested.")
        self._store.append_log(run_id, f"[{utc_now()}] stop requested")
        logger.info("Stop requested for run %s (pid=%s pgid=%s)", run_id[:8], run.pid, run.pgid)

        sent = self._signal_run(run, signal.SIGTERM)
        if not sent:
            return False, f"Unable to signal {run.name}; no pid available."

        self._store.update_run(run_id, status="stopping", message="Termination signal sent.")
        self._store.append_log(run_id, f"[{utc_now()}] SIGTERM sent")
        logger.debug("SIGTERM sent to run %s", run_id[:8])

        # For externally-managed processes (started before this supervisor
        # instance), launch a single watcher thread that handles both
        # natural exit detection and SIGKILL escalation.
        if not self._is_managed(run_id):
            threading.Thread(
                target=self._watch_until_stopped,
                args=(run_id, run.pid, run.pgid),
                daemon=True,
                name=f"supervisor-stop-{run_id[:8]}",
            ).start()

        # For managed processes, _run_process already owns the wait loop;
        # once it sees stop_requested/stopping status it will exit cleanly.

        return True, f"Stopping {run.name}."

    # ── Reconciliation on restart ─────────────────────────────────────────────

    def _reconcile_runs(self) -> None:
        for run in self._store.list_active_runs():
            if run.pid is not None and self._pid_exists(run.pid):
                self._store.update_run(
                    run.run_id,
                    status="orphaned",
                    message=(
                        "Control plane restarted while this process was active. "
                        "It may still be running and should be stopped or inspected."
                    ),
                )
                self._store.append_log(run.run_id, f"[{utc_now()}] recovered as orphaned pid={run.pid}")
                logger.warning("Orphaned run recovered: %s pid=%s", run.run_id[:8], run.pid)
            else:
                self._store.update_run(
                    run.run_id,
                    status="failed",
                    finished_at=utc_now(),
                    message="Control plane restarted before this run completed.",
                )
                self._store.append_log(run.run_id, f"[{utc_now()}] marked failed during recovery")
                logger.info("Stale run marked failed: %s", run.run_id[:8])

    # ── Process execution ─────────────────────────────────────────────────────

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
            self._store.update_run(run_id, status="stopped", finished_at=utc_now(),
                                   message="Stopped before the process was started.")
            self._store.append_log(run_id, f"[{utc_now()}] cancelled before start")
            return

        self._store.update_run(run_id, status="starting", message="Starting process.")
        self._store.append_log(run_id, f"[{utc_now()}] starting command={' '.join(command)}")
        logger.info("Launching run %s: %s", run_id[:8], " ".join(command))

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
                pass

            with self._lock:
                self._active_processes[run_id] = process

            self._store.update_run(run_id, status="running", started_at=utc_now(),
                                   pid=process.pid, pgid=pgid, message="Process is running.")
            self._store.append_log(run_id, f"[{utc_now()}] started pid={process.pid}")
            logger.info("Run %s started pid=%s pgid=%s", run_id[:8], process.pid, pgid)

            # Stream stdout/stderr into the log store.
            if process.stdout is not None:
                for line in process.stdout:
                    cleaned = line.rstrip()
                    if cleaned:
                        self._store.append_log(run_id, cleaned)

            return_code = process.wait()

            final = self._store.get_run(run_id)
            if final is not None and final.status in {"stop_requested", "stopping"}:
                status, message = "stopped", "Process stopped by operator."
            elif return_code != 0:
                status, message = "failed", f"Process exited with return code {return_code}."
                logger.warning("Run %s failed rc=%d", run_id[:8], return_code)
            else:
                status, message = "succeeded", "Process completed successfully."
                logger.info("Run %s succeeded", run_id[:8])

            self._store.update_run(run_id, status=status, finished_at=utc_now(),
                                   return_code=return_code, message=message)
            self._store.append_log(run_id, f"[{utc_now()}] exited rc={return_code}")

        except Exception as exc:
            logger.exception("Run %s failed to launch: %s", run_id[:8], exc)
            self._store.update_run(run_id, status="failed", finished_at=utc_now(),
                                   message=f"Failed to launch process: {exc}")
            self._store.append_log(run_id, f"[{utc_now()}] launch failed: {exc}")
        finally:
            if process is not None:
                with self._lock:
                    if self._active_processes.get(run_id) is process:
                        self._active_processes.pop(run_id, None)

    # ── Stop helpers ──────────────────────────────────────────────────────────

    def _watch_until_stopped(
        self,
        run_id: str,
        pid: int | None,
        pgid: int | None,
    ) -> None:
        """
        Single watcher thread for externally-managed processes.
        Waits for natural exit; escalates to SIGKILL after timeout.
        Updates the store exactly once.
        """
        if pid is None:
            return

        deadline = time.monotonic() + self._stop_timeout
        while time.monotonic() < deadline:
            if not self._pid_exists(pid):
                self._store.update_run(run_id, status="stopped", finished_at=utc_now(),
                                       message="Process stopped by operator.")
                self._store.append_log(run_id, f"[{utc_now()}] external process exited")
                logger.info("Run %s stopped naturally", run_id[:8])
                return
            time.sleep(0.5)

        # SIGKILL escalation — only if the process is still alive.
        if self._pid_exists(pid):
            logger.warning("Run %s did not stop within %ss — sending SIGKILL", run_id[:8], self._stop_timeout)
            self._send_sigkill(run_id, pid, pgid)

    def _send_sigkill(self, run_id: str, pid: int, pgid: int | None) -> None:
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] SIGKILL sent to pgid={pgid}")
            else:
                os.kill(pid, signal.SIGKILL)
                self._store.append_log(run_id, f"[{utc_now()}] SIGKILL sent to pid={pid}")
        except OSError as exc:
            logger.debug("SIGKILL for run %s failed (process may have exited): %s", run_id[:8], exc)

    def _signal_run(self, run: RunRecord, sig: signal.Signals) -> bool:
        if run.pgid is not None:
            try:
                os.killpg(run.pgid, sig)
                return True
            except (ProcessLookupError, OSError):
                return False
        if run.pid is not None:
            try:
                os.kill(run.pid, sig)
                return True
            except (ProcessLookupError, OSError):
                return False
        return False

    def _is_managed(self, run_id: str) -> bool:
        with self._lock:
            process = self._active_processes.get(run_id)
        return process is not None and process.poll() is None

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

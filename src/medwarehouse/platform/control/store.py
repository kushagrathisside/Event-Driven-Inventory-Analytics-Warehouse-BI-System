from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from medwarehouse.logging import get_logger
from medwarehouse.platform.models import RunRecord, utc_now


logger = get_logger(__name__)

_UPDATABLE_FIELDS = frozenset({
    "status", "message", "started_at", "finished_at",
    "pid", "pgid", "return_code", "updated_at",
})

_DEFAULT_PRUNE_DAYS = 30


class ControlPlaneStore:
    """
    SQLite-backed store for run records and per-run log lines.

    One SQLite connection is created per thread (threading.local) rather than
    per method call.  WAL journal mode allows readers to proceed concurrently
    with the single writer.

    Runs (and their logs, via CASCADE) older than prune_days days are deleted
    at startup to prevent unbounded growth.
    """

    def __init__(self, db_path: Path, *, prune_days: int = _DEFAULT_PRUNE_DAYS) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._initialize()
        if prune_days > 0:
            pruned = self.prune(keep_days=prune_days)
            if pruned:
                logger.debug("Pruned %d old terminal runs at startup (keep_days=%d)", pruned, prune_days)

    @property
    def db_path(self) -> Path:
        return self._db_path

    # ── Connection management ─────────────────────────────────────────────────

    def _get_connection(self) -> sqlite3.Connection:
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            self._local.conn = conn
        return conn

    def _initialize(self) -> None:
        conn = self._get_connection()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id          TEXT PRIMARY KEY,
                kind            TEXT NOT NULL,
                command_id      TEXT NOT NULL,
                name            TEXT NOT NULL,
                domain          TEXT NOT NULL,
                stage           TEXT NOT NULL,
                description     TEXT NOT NULL,
                command_display TEXT NOT NULL,
                long_running    INTEGER NOT NULL,
                status          TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                started_at      TEXT,
                finished_at     TEXT,
                pid             INTEGER,
                pgid            INTEGER,
                return_code     INTEGER,
                message         TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_runs_kind_command_created
                ON runs (kind, command_id, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_runs_kind_status
                ON runs (kind, status);

            CREATE TABLE IF NOT EXISTS run_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                created_at  TEXT NOT NULL,
                line        TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_run_logs_run_id_id
                ON run_logs (run_id, id DESC);
        """)
        conn.commit()

    # ── Writes ────────────────────────────────────────────────────────────────

    def create_run(self, run: RunRecord) -> None:
        conn = self._get_connection()
        conn.execute(
            """
            INSERT INTO runs (
                run_id, kind, command_id, name, domain, stage, description,
                command_display, long_running, status, created_at, updated_at,
                started_at, finished_at, pid, pgid, return_code, message
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run.run_id, run.kind, run.command_id, run.name, run.domain,
                run.stage, run.description, run.command_display,
                int(run.long_running), run.status, run.created_at,
                run.updated_at, run.started_at, run.finished_at,
                run.pid, run.pgid, run.return_code, run.message,
            ),
        )
        conn.commit()

    def update_run(self, run_id: str, **fields: object) -> None:
        if not fields:
            return
        unknown = set(fields) - _UPDATABLE_FIELDS
        if unknown:
            raise ValueError(
                f"update_run: unknown field(s) {sorted(unknown)}. "
                f"Allowed: {sorted(_UPDATABLE_FIELDS)}"
            )
        values = dict(fields)
        values.setdefault("updated_at", utc_now())
        assignments = ", ".join(f"{k} = ?" for k in values)
        conn = self._get_connection()
        conn.execute(
            f"UPDATE runs SET {assignments} WHERE run_id = ?",
            list(values.values()) + [run_id],
        )
        conn.commit()

    def append_log(self, run_id: str, line: str) -> None:
        conn = self._get_connection()
        conn.execute(
            "INSERT INTO run_logs (run_id, created_at, line) VALUES (?, ?, ?)",
            (run_id, utc_now(), line),
        )
        conn.commit()

    def prune(self, *, keep_days: int = _DEFAULT_PRUNE_DAYS) -> int:
        """Delete terminal runs (+ logs via CASCADE) older than keep_days. Returns count."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
        conn = self._get_connection()
        conn.execute(
            """
            DELETE FROM runs
            WHERE status IN ('succeeded', 'failed', 'stopped')
              AND finished_at IS NOT NULL
              AND finished_at < ?
            """,
            (cutoff,),
        )
        deleted: int = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
        return deleted

    # ── Reads ─────────────────────────────────────────────────────────────────

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._get_connection().execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return self._row_to_run(row)

    def get_latest_run(self, kind: str, command_id: str) -> RunRecord | None:
        row = self._get_connection().execute(
            "SELECT * FROM runs WHERE kind = ? AND command_id = ? ORDER BY created_at DESC LIMIT 1",
            (kind, command_id),
        ).fetchone()
        return self._row_to_run(row)

    def get_latest_run_for_kind(self, kind: str) -> RunRecord | None:
        row = self._get_connection().execute(
            "SELECT * FROM runs WHERE kind = ? ORDER BY created_at DESC LIMIT 1",
            (kind,),
        ).fetchone()
        return self._row_to_run(row)

    def get_active_run(self, kind: str, command_id: str | None = None) -> RunRecord | None:
        statuses = ("pending", "starting", "running", "stop_requested", "stopping", "orphaned")
        ph = ",".join("?" * len(statuses))
        query = f"SELECT * FROM runs WHERE kind = ? AND status IN ({ph})"
        params: list[object] = [kind, *statuses]
        if command_id is not None:
            query += " AND command_id = ?"
            params.append(command_id)
        query += " ORDER BY created_at DESC LIMIT 1"
        row = self._get_connection().execute(query, params).fetchone()
        return self._row_to_run(row)

    def list_active_runs(self) -> list[RunRecord]:
        statuses = ("pending", "starting", "running", "stop_requested", "stopping", "orphaned")
        ph = ",".join("?" * len(statuses))
        rows = self._get_connection().execute(
            f"SELECT * FROM runs WHERE status IN ({ph}) ORDER BY created_at DESC",
            statuses,
        ).fetchall()
        return [r for r in (self._row_to_run(row) for row in rows) if r is not None]

    def get_run_logs(self, run_id: str, *, limit: int = 200) -> list[str]:
        rows = self._get_connection().execute(
            "SELECT line FROM run_logs WHERE run_id = ? ORDER BY id DESC LIMIT ?",
            (run_id, limit),
        ).fetchall()
        return [row["line"] for row in reversed(rows)]

    @staticmethod
    def _row_to_run(row: sqlite3.Row | None) -> RunRecord | None:
        if row is None:
            return None
        return RunRecord(
            run_id=row["run_id"],
            kind=row["kind"],
            command_id=row["command_id"],
            name=row["name"],
            domain=row["domain"],
            stage=row["stage"],
            description=row["description"],
            command_display=row["command_display"],
            long_running=bool(row["long_running"]),
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            pid=row["pid"],
            pgid=row["pgid"],
            return_code=row["return_code"],
            message=row["message"],
        )

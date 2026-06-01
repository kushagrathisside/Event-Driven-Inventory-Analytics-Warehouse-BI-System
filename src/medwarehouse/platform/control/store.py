from __future__ import annotations

import sqlite3
from pathlib import Path

from medwarehouse.platform.models import RunRecord, utc_now


class ControlPlaneStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    description TEXT NOT NULL,
                    command_display TEXT NOT NULL,
                    long_running INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    pid INTEGER,
                    pgid INTEGER,
                    return_code INTEGER,
                    message TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_runs_kind_command_created
                    ON runs (kind, command_id, created_at DESC);

                CREATE INDEX IF NOT EXISTS idx_runs_kind_status
                    ON runs (kind, status);

                CREATE TABLE IF NOT EXISTS run_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    created_at TEXT NOT NULL,
                    line TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_run_logs_run_id_id
                    ON run_logs (run_id, id DESC);
                """
            )

    def create_run(self, run: RunRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, kind, command_id, name, domain, stage, description,
                    command_display, long_running, status, created_at, updated_at,
                    started_at, finished_at, pid, pgid, return_code, message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.kind,
                    run.command_id,
                    run.name,
                    run.domain,
                    run.stage,
                    run.description,
                    run.command_display,
                    int(run.long_running),
                    run.status,
                    run.created_at,
                    run.updated_at,
                    run.started_at,
                    run.finished_at,
                    run.pid,
                    run.pgid,
                    run.return_code,
                    run.message,
                ),
            )

    def update_run(self, run_id: str, **fields: object) -> None:
        if not fields:
            return

        values = dict(fields)
        values.setdefault("updated_at", utc_now())
        assignments = ", ".join(f"{key} = ?" for key in values)
        parameters = list(values.values()) + [run_id]

        with self._connect() as conn:
            conn.execute(f"UPDATE runs SET {assignments} WHERE run_id = ?", parameters)

    def append_log(self, run_id: str, line: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_logs (run_id, created_at, line)
                VALUES (?, ?, ?)
                """,
                (run_id, utc_now(), line),
            )

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return self._row_to_run(row)

    def get_latest_run(self, kind: str, command_id: str) -> RunRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE kind = ? AND command_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (kind, command_id),
            ).fetchone()
        return self._row_to_run(row)

    def get_latest_run_for_kind(self, kind: str) -> RunRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE kind = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (kind,),
            ).fetchone()
        return self._row_to_run(row)

    def get_active_run(self, kind: str, command_id: str | None = None) -> RunRecord | None:
        query = """
            SELECT *
            FROM runs
            WHERE kind = ? AND status IN (?, ?, ?, ?, ?, ?)
        """
        params: list[object] = [
            kind,
            "pending",
            "starting",
            "running",
            "stop_requested",
            "stopping",
            "orphaned",
        ]

        if command_id is not None:
            query += " AND command_id = ?"
            params.append(command_id)

        query += " ORDER BY created_at DESC LIMIT 1"

        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_run(row)

    def list_active_runs(self) -> list[RunRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE status IN (?, ?, ?, ?, ?, ?)
                ORDER BY created_at DESC
                """,
                (
                    "pending",
                    "starting",
                    "running",
                    "stop_requested",
                    "stopping",
                    "orphaned",
                ),
            ).fetchall()
        return [self._row_to_run(row) for row in rows if row is not None]

    def get_run_logs(self, run_id: str, *, limit: int = 200) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT line
                FROM run_logs
                WHERE run_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
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


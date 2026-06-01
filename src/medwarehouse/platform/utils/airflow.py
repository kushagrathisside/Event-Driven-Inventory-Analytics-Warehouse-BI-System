from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

from medwarehouse.config import AppSettings, get_settings


def collect_airflow_dag_state(
    *,
    dag_id: str,
    dag_path: Path,
    settings: AppSettings | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    db_candidates = _airflow_db_candidates(active_settings)
    db_path = next((candidate for candidate in db_candidates if candidate.exists()), None)

    result = {
        "dag_id": dag_id,
        "dag_path": str(dag_path),
        "dag_exists": dag_path.exists(),
        "schedule": "manual",
        "metadata_available": False,
        "metadata_path": str(db_path) if db_path else None,
        "last_run": None,
        "error": None,
    }

    if db_path is None:
        return result

    try:
        last_run = _fetch_last_dag_run(db_path=db_path, dag_id=dag_id)
        result["metadata_available"] = True
        result["last_run"] = last_run
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def _airflow_db_candidates(settings: AppSettings) -> list[Path]:
    env_home = os.environ.get("AIRFLOW_HOME")
    candidates = []
    if env_home:
        candidates.append(Path(env_home).expanduser().resolve() / "airflow.db")
    candidates.append((settings.paths.project_root / "airflow" / "airflow.db").resolve())
    candidates.append((Path.home() / "airflow" / "airflow.db").resolve())
    deduped = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate not in seen:
            deduped.append(candidate)
            seen.add(candidate)
    return deduped


def _fetch_last_dag_run(*, db_path: Path, dag_id: str) -> dict[str, Any] | None:
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('dag_run')").fetchall()
        }
        if not columns:
            return None

        date_columns = [
            column_name
            for column_name in ("end_date", "start_date", "logical_date", "execution_date")
            if column_name in columns
        ]
        if not date_columns:
            date_columns = ["run_id"] if "run_id" in columns else []

        order_expression = "COALESCE(" + ", ".join(date_columns) + ")" if date_columns else "rowid"
        selected_columns = [
            column_name
            for column_name in ("run_id", "state", "run_type", "start_date", "end_date", "logical_date")
            if column_name in columns
        ]
        if not selected_columns:
            return None

        query = (
            "SELECT "
            + ", ".join(selected_columns)
            + " FROM dag_run WHERE dag_id = ? "
            + f"ORDER BY {order_expression} DESC LIMIT 1"
        )
        row = conn.execute(query, (dag_id,)).fetchone()
        if row is None:
            return None
        return {
            selected_columns[index]: row[index]
            for index in range(len(selected_columns))
        }


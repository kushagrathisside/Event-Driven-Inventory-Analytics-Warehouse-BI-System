from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def summarize_artifact_tree(path: Path) -> dict[str, Any]:
    parquet_files = sorted(path.rglob("*.parquet")) if path.exists() else []
    total_size = sum(file_path.stat().st_size for file_path in parquet_files)
    newest_mtime = max((file_path.stat().st_mtime for file_path in parquet_files), default=None)
    return {
        "path": str(path),
        "exists": path.exists(),
        "parquet_files": len(parquet_files),
        "size_bytes": total_size,
        "updated_at": _format_timestamp(newest_mtime),
        "count": len(parquet_files),
    }


def _format_timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2

from medwarehouse.config import PostgresConnection


@contextmanager
def connect(connection: PostgresConnection) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(connection.psycopg_dsn)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

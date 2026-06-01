from __future__ import annotations

from pathlib import Path

from medwarehouse.logging import get_logger
from medwarehouse.warehouse.postgres import connect
from medwarehouse.config import PostgresConnection


logger = get_logger(__name__)


def render_sql_template(template: str, context: dict[str, str]) -> str:
    rendered = template
    for key, value in context.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    return rendered


def execute_sql_file(connection: PostgresConnection, sql_path: Path, context: dict[str, str]) -> None:
    sql = render_sql_template(sql_path.read_text(), context)
    logger.info("Applying SQL file %s", sql_path)
    with connect(connection) as conn:
        conn.autocommit = False
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def fetch_rows(connection: PostgresConnection, sql: str) -> list[tuple]:
    with connect(connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


def execute_statement(connection: PostgresConnection, sql: str) -> None:
    with connect(connection) as conn:
        conn.autocommit = False
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()

from medwarehouse.config import get_settings


def connect(connection):
    """Re-export with a lazy psycopg2 import so the module is importable without it installed."""
    from medwarehouse.warehouse.postgres import connect as _connect
    return _connect(connection)


def get_analytics_writer():
    return get_settings().analytics_writer_db


def get_analytics_reader():
    return get_settings().analytics_reader_db


def get_master_db():
    return get_settings().master_db

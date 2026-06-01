from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _get_str(env: dict[str, str], key: str, default: str) -> str:
    value = env.get(key, default).strip()
    return value or default


def _get_int(env: dict[str, str], key: str, default: int) -> int:
    return int(_get_str(env, key, str(default)))


def _get_bool(env: dict[str, str], key: str, default: bool) -> bool:
    value = _get_str(env, key, str(default)).lower()
    return value in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class PostgresConnection:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def psycopg_dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    inventory_topic: str
    procurement_topic: str
    sales_topic: str


@dataclass(frozen=True)
class SparkSettings:
    master: str
    log_level: str
    postgres_driver_jar: Path
    checkpoint_root: Path


@dataclass(frozen=True)
class PathSettings:
    project_root: Path
    data_root: Path
    schemas_root: Path
    sql_root: Path
    bronze_inventory_path: Path
    silver_inventory_path: Path
    quarantine_inventory_path: Path
    inventory_checkpoint_path: Path


@dataclass(frozen=True)
class ProducerSettings:
    interval_seconds: int
    max_events: int
    dry_run: bool
    inventory_seed: str
    inventory_start_time: str
    sample_product_id: str
    sample_warehouse_id: str
    sample_supplier_id: str
    sample_batch_number: str
    sample_expiry_date: str
    sample_currency: str


@dataclass(frozen=True)
class WarehouseRoleSettings:
    fdw_reader_user: str
    fdw_reader_password: str
    spark_writer_user: str
    spark_writer_password: str
    analytics_reader_user: str
    analytics_reader_password: str
    fdw_remote_user: str
    fdw_remote_password: str


@dataclass(frozen=True)
class AppSettings:
    env_name: str
    paths: PathSettings
    kafka: KafkaSettings
    spark: SparkSettings
    producer: ProducerSettings
    master_db: PostgresConnection
    analytics_admin_db: PostgresConnection
    analytics_writer_db: PostgresConnection
    analytics_reader_db: PostgresConnection
    warehouse_roles: WarehouseRoleSettings


def _build_connection(
    env: dict[str, str],
    prefix: str,
    *,
    default_host: str,
    default_port: int,
    default_database: str,
    default_user: str,
    default_password: str,
) -> PostgresConnection:
    return PostgresConnection(
        host=_get_str(env, f"{prefix}_HOST", default_host),
        port=_get_int(env, f"{prefix}_PORT", default_port),
        database=_get_str(env, f"{prefix}_NAME", default_database),
        user=_get_str(env, f"{prefix}_USER", default_user),
        password=_get_str(env, f"{prefix}_PASSWORD", default_password),
    )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    env = dict(os.environ)
    project_root = Path(_get_str(env, "MW_PROJECT_ROOT", str(_repo_root()))).resolve()
    data_root = Path(_get_str(env, "MW_DATA_ROOT", str(project_root / "data"))).resolve()
    schemas_root = Path(
        _get_str(env, "MW_SCHEMAS_ROOT", str(project_root / "schemas" / "kafka"))
    ).resolve()
    sql_root = Path(_get_str(env, "MW_SQL_ROOT", str(project_root / "sql" / "warehouse"))).resolve()
    checkpoint_root = Path(
        _get_str(env, "MW_CHECKPOINT_ROOT", str(data_root / "checkpoints"))
    ).resolve()
    spark_jar = Path(
        _get_str(env, "MW_SPARK_POSTGRES_JAR", str(project_root / "jars" / "postgresql-42.7.3.jar"))
    ).resolve()

    master_db = _build_connection(
        env,
        "MW_MASTER_DB",
        default_host="localhost",
        default_port=5432,
        default_database="medwarehouse_master",
        default_user="postgres",
        default_password="postgres",
    )
    analytics_admin_db = _build_connection(
        env,
        "MW_ANALYTICS_ADMIN_DB",
        default_host="localhost",
        default_port=5432,
        default_database="medwarehouse_analytics",
        default_user="postgres",
        default_password="postgres",
    )
    analytics_writer_db = _build_connection(
        env,
        "MW_ANALYTICS_WRITER_DB",
        default_host=analytics_admin_db.host,
        default_port=analytics_admin_db.port,
        default_database=analytics_admin_db.database,
        default_user="spark_writer",
        default_password="spark_writer_pwd",
    )
    analytics_reader_db = _build_connection(
        env,
        "MW_ANALYTICS_READER_DB",
        default_host=analytics_admin_db.host,
        default_port=analytics_admin_db.port,
        default_database=analytics_admin_db.database,
        default_user="analytics_reader",
        default_password="analytics_reader_pwd",
    )

    warehouse_roles = WarehouseRoleSettings(
        fdw_reader_user=_get_str(env, "MW_ANALYTICS_FDW_READER_USER", "fdw_reader"),
        fdw_reader_password=_get_str(
            env, "MW_ANALYTICS_FDW_READER_PASSWORD", "fdw_reader_pwd"
        ),
        spark_writer_user=analytics_writer_db.user,
        spark_writer_password=analytics_writer_db.password,
        analytics_reader_user=analytics_reader_db.user,
        analytics_reader_password=analytics_reader_db.password,
        fdw_remote_user=_get_str(env, "MW_FDW_REMOTE_USER", master_db.user),
        fdw_remote_password=_get_str(env, "MW_FDW_REMOTE_PASSWORD", master_db.password),
    )

    return AppSettings(
        env_name=_get_str(env, "MW_ENV", "local"),
        paths=PathSettings(
            project_root=project_root,
            data_root=data_root,
            schemas_root=schemas_root,
            sql_root=sql_root,
            bronze_inventory_path=data_root / "bronze" / "inventory_events",
            silver_inventory_path=data_root / "silver" / "inventory_events",
            quarantine_inventory_path=data_root / "quarantine" / "inventory_events",
            inventory_checkpoint_path=checkpoint_root / "inventory_events",
        ),
        kafka=KafkaSettings(
            bootstrap_servers=_get_str(env, "MW_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            inventory_topic=_get_str(env, "MW_KAFKA_INVENTORY_TOPIC", "inventory_events"),
            procurement_topic=_get_str(
                env, "MW_KAFKA_PROCUREMENT_TOPIC", "procurement_events"
            ),
            sales_topic=_get_str(env, "MW_KAFKA_SALES_TOPIC", "sales_events"),
        ),
        spark=SparkSettings(
            master=_get_str(env, "MW_SPARK_MASTER", "local[*]"),
            log_level=_get_str(env, "MW_SPARK_LOG_LEVEL", "WARN"),
            postgres_driver_jar=spark_jar,
            checkpoint_root=checkpoint_root,
        ),
        producer=ProducerSettings(
            interval_seconds=_get_int(env, "MW_PRODUCER_INTERVAL_SECONDS", 5),
            max_events=_get_int(env, "MW_PRODUCER_MAX_EVENTS", 20),
            dry_run=_get_bool(env, "MW_PRODUCER_DRY_RUN", False),
            inventory_seed=_get_str(env, "MW_SAMPLE_SEED", "medical-warehouse-local"),
            inventory_start_time=_get_str(
                env, "MW_SAMPLE_START_TIME", "2026-01-01T08:00:00+00:00"
            ),
            sample_product_id=_get_str(
                env, "MW_SAMPLE_PRODUCT_ID", "P-LOCAL-CALPOL-500"
            ),
            sample_warehouse_id=_get_str(
                env, "MW_SAMPLE_WAREHOUSE_ID", "W-LOCAL-RECEIVING-01"
            ),
            sample_supplier_id=_get_str(
                env, "MW_SAMPLE_SUPPLIER_ID", "SUP-LOCAL-HIMALAYAN"
            ),
            sample_batch_number=_get_str(env, "MW_SAMPLE_BATCH_NUMBER", "BATCH-LOCAL-001"),
            sample_expiry_date=_get_str(env, "MW_SAMPLE_EXPIRY_DATE", "2027-12-31"),
            sample_currency=_get_str(env, "MW_SAMPLE_CURRENCY", "INR"),
        ),
        master_db=master_db,
        analytics_admin_db=analytics_admin_db,
        analytics_writer_db=analytics_writer_db,
        analytics_reader_db=analytics_reader_db,
        warehouse_roles=warehouse_roles,
    )

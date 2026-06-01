from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    name: str
    domain: str
    stage: str
    description: str
    command: tuple[str, ...]
    long_running: bool = False


JOB_SPECS: tuple[JobSpec, ...] = (
    JobSpec(
        job_id="warehouse_bootstrap",
        name="Bootstrap Warehouse",
        domain="inventory",
        stage="warehouse",
        description="Create roles, FDW access, dimensions, facts, and semantic views.",
        command=("warehouse", "bootstrap"),
    ),
    JobSpec(
        job_id="inventory_producer",
        name="Inventory Producer",
        domain="inventory",
        stage="producer",
        description="Emit deterministic inventory events into Kafka.",
        command=("produce", "inventory", "--max-events", "10"),
    ),
    JobSpec(
        job_id="inventory_bronze",
        name="Inventory Bronze Stream",
        domain="inventory",
        stage="bronze",
        description="Continuously ingest Kafka inventory events into immutable parquet.",
        command=("spark", "inventory-bronze"),
        long_running=True,
    ),
    JobSpec(
        job_id="inventory_silver",
        name="Inventory Silver Refresh",
        domain="inventory",
        stage="silver",
        description="Validate, normalize, deduplicate, and quarantine inventory events.",
        command=("spark", "inventory-silver"),
    ),
    JobSpec(
        job_id="inventory_stage",
        name="Stage Inventory Events",
        domain="inventory",
        stage="gold",
        description="Write warehouse-ready inventory events into analytics staging.",
        command=("spark", "stage-inventory-events"),
    ),
    JobSpec(
        job_id="refresh_dimensions",
        name="Refresh Dimensions",
        domain="inventory",
        stage="warehouse",
        description="Refresh SCD Type-2 supplier, product, and warehouse dimensions.",
        command=("warehouse", "refresh-dimensions"),
    ),
    JobSpec(
        job_id="load_inventory_facts",
        name="Load Inventory Facts",
        domain="inventory",
        stage="warehouse",
        description="Load staged inventory events into the append-only fact table.",
        command=("warehouse", "load-facts"),
    ),
    JobSpec(
        job_id="refresh_views",
        name="Refresh Semantic Views",
        domain="inventory",
        stage="semantic",
        description="Refresh BI-facing balance and snapshot views.",
        command=("warehouse", "refresh-views"),
    ),
    JobSpec(
        job_id="quality_checks",
        name="Quality Checks",
        domain="inventory",
        stage="quality",
        description="Run warehouse integrity and reconciliation checks.",
        command=("warehouse", "quality-checks"),
    ),
    JobSpec(
        job_id="build_gold",
        name="Build Gold Pipeline",
        domain="inventory",
        stage="orchestration",
        description="Stage inventory events, refresh dimensions, load facts, refresh views, and validate quality.",
        command=("orchestration", "build-gold"),
    ),
    JobSpec(
        job_id="procurement_producer",
        name="Procurement Producer",
        domain="procurement",
        stage="producer",
        description="Emit procurement sample events. Downstream pipeline not implemented yet.",
        command=("produce", "procurement", "--max-events", "10", "--dry-run"),
    ),
    JobSpec(
        job_id="sales_producer",
        name="Sales Producer",
        domain="sales",
        stage="producer",
        description="Emit sales sample events. Downstream pipeline not implemented yet.",
        command=("produce", "sales", "--max-events", "10", "--dry-run"),
    ),
)


def get_job_spec(job_id: str) -> JobSpec | None:
    for spec in JOB_SPECS:
        if spec.job_id == job_id:
            return spec
    return None


def group_jobs_by_stage() -> dict[str, list[JobSpec]]:
    grouped: dict[str, list[JobSpec]] = {}
    for spec in JOB_SPECS:
        grouped.setdefault(spec.stage, []).append(spec)
    return grouped


def job_specs() -> Sequence[JobSpec]:
    return JOB_SPECS


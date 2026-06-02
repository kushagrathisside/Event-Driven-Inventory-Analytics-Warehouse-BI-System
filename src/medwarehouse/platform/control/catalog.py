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
        job_id="refresh_balance",
        name="Refresh Balance Snapshot",
        domain="inventory",
        stage="warehouse",
        description="Merge v_inventory_balance into fact_inventory_balance for fast BI queries.",
        command=("warehouse", "refresh-balance"),
    ),
    JobSpec(
        job_id="check_reorders",
        name="Check Reorder Thresholds",
        domain="inventory",
        stage="automation",
        description="Compare current stock against reorder policies and create draft purchase orders.",
        command=("warehouse", "check-reorders"),
    ),
    # ── Procurement domain ──────────────────────────────────────────────────
    JobSpec(
        job_id="procurement_producer",
        name="Procurement Producer",
        domain="procurement",
        stage="producer",
        description="Emit sample procurement lifecycle events (PO_CREATED → PO_APPROVED → PO_RECEIVED).",
        command=("produce", "procurement", "--max-events", "10"),
    ),
    JobSpec(
        job_id="procurement_bronze",
        name="Procurement Bronze Stream",
        domain="procurement",
        stage="bronze",
        description="Continuously ingest Kafka procurement events into immutable Parquet.",
        command=("spark", "procurement-bronze"),
        long_running=True,
    ),
    JobSpec(
        job_id="procurement_silver",
        name="Procurement Silver Refresh",
        domain="procurement",
        stage="silver",
        description="Validate, normalize, deduplicate, and quarantine procurement events.",
        command=("spark", "procurement-silver"),
    ),
    JobSpec(
        job_id="build_procurement_gold",
        name="Build Procurement Gold",
        domain="procurement",
        stage="orchestration",
        description="Stage procurement events, load facts, refresh views, and validate quality.",
        command=("orchestration", "build-procurement-gold"),
    ),
    # ── Sales domain ─────────────────────────────────────────────────────────
    JobSpec(
        job_id="sales_producer",
        name="Sales Producer",
        domain="sales",
        stage="producer",
        description="Emit sample sales events (SALE_CREATED with periodic SALE_CANCELLED).",
        command=("produce", "sales", "--max-events", "10"),
    ),
    JobSpec(
        job_id="sales_bronze",
        name="Sales Bronze Stream",
        domain="sales",
        stage="bronze",
        description="Continuously ingest Kafka sales events into immutable Parquet.",
        command=("spark", "sales-bronze"),
        long_running=True,
    ),
    JobSpec(
        job_id="sales_silver",
        name="Sales Silver Refresh",
        domain="sales",
        stage="silver",
        description="Validate, normalize, deduplicate, and quarantine sales events.",
        command=("spark", "sales-silver"),
    ),
    JobSpec(
        job_id="build_sales_gold",
        name="Build Sales Gold",
        domain="sales",
        stage="orchestration",
        description="Stage sales events, refresh dimensions, load facts, refresh views, and validate quality.",
        command=("orchestration", "build-sales-gold"),
    ),
)


# Module-level dict for O(1) lookup — JOB_SPECS is populated before this line.
_JOB_SPEC_MAP: dict[str, JobSpec] = {spec.job_id: spec for spec in JOB_SPECS}


def get_job_spec(job_id: str) -> JobSpec | None:
    return _JOB_SPEC_MAP.get(job_id)


def group_jobs_by_stage() -> dict[str, list[JobSpec]]:
    grouped: dict[str, list[JobSpec]] = {}
    for spec in JOB_SPECS:
        grouped.setdefault(spec.stage, []).append(spec)
    return grouped


def job_specs() -> Sequence[JobSpec]:
    return JOB_SPECS


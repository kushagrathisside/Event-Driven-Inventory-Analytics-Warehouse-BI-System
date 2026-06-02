from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medwarehouse.spark.jobs.procurement_stage import run_procurement_event_staging
from medwarehouse.warehouse.procurement import (
    build_procurement_gold,
    load_procurement_event_facts,
    refresh_procurement_dimensions,
    refresh_procurement_semantic_views,
    run_procurement_quality_checks,
    validate_procurement_silver_ready,
)


with DAG(
    dag_id="procurement_gold_pipeline",
    description="Build procurement facts, PO lifecycle view, and supplier performance view.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["procurement", "warehouse", "gold"],
) as dag:
    validate_silver = PythonOperator(
        task_id="validate_procurement_silver_availability",
        python_callable=validate_procurement_silver_ready,
    )

    refresh_dimensions = PythonOperator(
        task_id="refresh_procurement_dimensions",
        python_callable=refresh_procurement_dimensions,
    )

    stage_procurement_events = PythonOperator(
        task_id="stage_procurement_events",
        python_callable=run_procurement_event_staging,
    )

    load_procurement_facts = PythonOperator(
        task_id="load_procurement_event_facts",
        python_callable=load_procurement_event_facts,
    )

    refresh_semantic_views = PythonOperator(
        task_id="refresh_procurement_semantic_views",
        python_callable=refresh_procurement_semantic_views,
    )

    quality_checks = PythonOperator(
        task_id="procurement_quality_checks",
        python_callable=run_procurement_quality_checks,
    )

    (
        validate_silver
        >> stage_procurement_events
        >> refresh_dimensions
        >> load_procurement_facts
        >> refresh_semantic_views
        >> quality_checks
    )

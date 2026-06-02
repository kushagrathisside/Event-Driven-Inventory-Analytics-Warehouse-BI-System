from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medwarehouse.spark.jobs.inventory_stage import run_inventory_event_staging
from medwarehouse.warehouse.inventory import (
    check_reorder_thresholds,
    load_inventory_event_facts,
    refresh_inventory_balance,
    refresh_inventory_dimensions,
    refresh_inventory_semantic_views,
    run_inventory_quality_checks,
    validate_inventory_silver_ready,
)


with DAG(
    dag_id="inventory_gold_pipeline",
    description="Build inventory dimensions, fact events, and BI-safe views.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["inventory", "warehouse", "gold"],
) as dag:
    validate_silver = PythonOperator(
        task_id="validate_silver_availability",
        python_callable=validate_inventory_silver_ready,
    )

    refresh_dimensions = PythonOperator(
        task_id="refresh_inventory_dimensions",
        python_callable=refresh_inventory_dimensions,
    )

    stage_inventory_events = PythonOperator(
        task_id="stage_inventory_events",
        python_callable=run_inventory_event_staging,
    )

    load_inventory_facts = PythonOperator(
        task_id="load_inventory_event_facts",
        python_callable=load_inventory_event_facts,
    )

    refresh_semantic_views = PythonOperator(
        task_id="refresh_inventory_semantic_views",
        python_callable=refresh_inventory_semantic_views,
    )

    quality_checks = PythonOperator(
        task_id="inventory_quality_checks",
        python_callable=run_inventory_quality_checks,
    )

    refresh_balance = PythonOperator(
        task_id="refresh_inventory_balance",
        python_callable=refresh_inventory_balance,
    )

    check_reorders = PythonOperator(
        task_id="check_reorder_thresholds",
        python_callable=check_reorder_thresholds,
    )

    (
        validate_silver
        >> stage_inventory_events
        >> refresh_dimensions
        >> load_inventory_facts
        >> refresh_semantic_views
        >> quality_checks
        >> refresh_balance
        >> check_reorders
    )

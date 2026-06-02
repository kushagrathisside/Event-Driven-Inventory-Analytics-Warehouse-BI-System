from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medwarehouse.spark.jobs.sales_stage import run_sales_event_staging
from medwarehouse.warehouse.sales import (
    build_sales_gold,
    load_sales_event_facts,
    refresh_sales_dimensions,
    refresh_sales_semantic_views,
    run_sales_quality_checks,
    validate_sales_silver_ready,
)


with DAG(
    dag_id="sales_gold_pipeline",
    description="Build sales facts, revenue summary, and sales velocity views.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sales", "warehouse", "gold"],
) as dag:
    validate_silver = PythonOperator(
        task_id="validate_sales_silver_availability",
        python_callable=validate_sales_silver_ready,
    )

    refresh_dimensions = PythonOperator(
        task_id="refresh_sales_dimensions",
        python_callable=refresh_sales_dimensions,
    )

    stage_sales_events = PythonOperator(
        task_id="stage_sales_events",
        python_callable=run_sales_event_staging,
    )

    load_sales_facts = PythonOperator(
        task_id="load_sales_event_facts",
        python_callable=load_sales_event_facts,
    )

    refresh_semantic_views = PythonOperator(
        task_id="refresh_sales_semantic_views",
        python_callable=refresh_sales_semantic_views,
    )

    quality_checks = PythonOperator(
        task_id="sales_quality_checks",
        python_callable=run_sales_quality_checks,
    )

    (
        validate_silver
        >> stage_sales_events
        >> refresh_dimensions
        >> load_sales_facts
        >> refresh_semantic_views
        >> quality_checks
    )

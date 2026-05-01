from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.getenv("SOURCEIQ_HOME", "/opt/sourceiq")
PYTHONPATH = f"{PROJECT_ROOT}:{os.getenv('PYTHONPATH', '')}"

default_args = {
    "owner": "sourceiq-data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="sourceiq_supplier_intelligence_lakehouse",
    description="Bronze/Silver/Gold supplier intelligence pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["supplier", "lakehouse", "data-quality", "deduplication"],
) as dag:
    ingest_raw_sources = BashOperator(
        task_id="ingest_raw_sources",
        bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH={PYTHONPATH} python notebooks/01_bronze_ingestion.py",
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && test -d data/bronze/conference_csv "
            "&& test -d data/bronze/government_vendor "
            "&& test -d data/bronze/erp_export "
            "&& test -d data/bronze/supplier_activity_api"
        ),
    )

    transform_to_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH={PYTHONPATH} python notebooks/02_silver_cleaning.py",
    )

    run_deduplication = BashOperator(
        task_id="run_deduplication",
        bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH={PYTHONPATH} python notebooks/03_deduplication.py",
    )

    run_quality_checks_and_build_gold = BashOperator(
        task_id="run_quality_checks_and_build_gold",
        bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH={PYTHONPATH} python notebooks/04_gold_modeling.py",
    )

    publish_metrics = BashOperator(
        task_id="publish_metrics",
        bash_command=f"cd {PROJECT_ROOT} && find data/gold -maxdepth 2 -type d | sort",
    )

    ingest_raw_sources >> validate_bronze >> transform_to_silver >> run_deduplication
    run_deduplication >> run_quality_checks_and_build_gold >> publish_metrics

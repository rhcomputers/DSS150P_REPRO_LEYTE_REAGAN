"""
DSS150P - Airflow DAG for the reproducible predictive pipeline.

Orchestration only. Business logic lives in src/. The DAG calls the
same functions a manual run would call, so behavior is identical
whether you trigger via Airflow or run src.cli locally.

Shape:
    ingest -> transform -> validate_staging -> storage -> train -> validate_curated -> evaluate -> quality_report

Retries:
    - Failures on transient tasks (ingest, storage, train) retry twice.
    - Validation tasks are NOT retried: a failed check fails again and
      would duplicate its log rows.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import configure_logging, get_settings
from src.evaluate import evaluate
from src.ingest import ingest
from src.storage import (
    apply_schema,
    benchmark_formats,
    load_curated_to_postgres,
    write_benchmark_report,
    write_partitioned_parquet,
)
from src.train import train
from src.transform import transform_all_raw
from src.validate import run_curated_checks, run_staging_checks, write_report

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "leyte_reagan",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


# ----------------------------------------------------------------------
# Task callables
# ----------------------------------------------------------------------
def _task_ingest(**context) -> None:
    configure_logging(get_settings())
    s = get_settings()
    ingest(s, force=False)


def _task_transform(**context) -> None:
    configure_logging(get_settings())
    s = get_settings()
    staging, curated = transform_all_raw(s)
    context["ti"].xcom_push(key="staging_rows", value=len(staging))
    context["ti"].xcom_push(key="curated_rows", value=len(curated))


def _task_validate_staging(**context) -> None:
    import pandas as pd
    configure_logging(get_settings())
    s = get_settings()
    df = pd.read_parquet(s.staging_dir / "staging.parquet")
    run_id = context["run_id"]
    report = run_staging_checks(df, run_id)
    write_report(report, s)
    report.raise_if_failed()


def _task_validate_curated(**context) -> None:
    import pandas as pd
    configure_logging(get_settings())
    s = get_settings()
    df = pd.read_parquet(s.curated_dir / "curated.parquet")
    run_id = context["run_id"]
    report = run_curated_checks(df, run_id)
    write_report(report, s)
    report.raise_if_failed()


def _task_storage(**context) -> None:
    import pandas as pd
    configure_logging(get_settings())
    s = get_settings()
    curated = pd.read_parquet(s.curated_dir / "curated.parquet")
    results = benchmark_formats(curated, s)
    write_benchmark_report(results, s)
    write_partitioned_parquet(curated, s)

    if s.enable_postgres:
        try:
            apply_schema(s)
            load_curated_to_postgres(curated, s)
        except Exception as exc:
            log.warning("Postgres load skipped: %s", exc)


def _task_train(**context) -> None:
    configure_logging(get_settings())
    s = get_settings()
    train(s)


def _task_evaluate(**context) -> None:
    configure_logging(get_settings())
    s = get_settings()
    result = evaluate(s)
    context["ti"].xcom_push(key="comparison_status", value=result["comparison"]["status"])
    if result["comparison"]["status"] == "fail":
        raise AssertionError("Metrics drifted outside declared tolerance")


def _task_quality_report(**context) -> None:
    """Runs regardless of upstream state; fails itself if anything upstream failed."""
    configure_logging(get_settings())
    ti = context["ti"]
    dag_run = context["dag_run"]

    failed_states = {"failed", "upstream_failed"}
    failed_tasks = [
        t.task_id for t in dag_run.get_task_instances()
        if t.state in failed_states
    ]

    comparison_status = ti.xcom_pull(key="comparison_status", task_ids="evaluate")
    curated_rows = ti.xcom_pull(key="curated_rows", task_ids="transform")

    log.info(
        "QUALITY REPORT | failed_tasks=%s | curated_rows=%s | comparison=%s",
        failed_tasks, curated_rows, comparison_status,
    )

    if failed_tasks:
        raise AssertionError(f"Upstream failures: {failed_tasks}")


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="predictive_repro_pipeline",
    description="DSS150P reproducible predictive pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="0 6 * * 1",          
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dss150p", "repro"],
) as dag:

    t_ingest = PythonOperator(task_id="ingest", python_callable=_task_ingest)

    t_transform = PythonOperator(task_id="transform", python_callable=_task_transform)

    t_validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=_task_validate_staging,
        retries=0,                
    )

    t_storage = PythonOperator(task_id="storage", python_callable=_task_storage)

    t_train = PythonOperator(task_id="train", python_callable=_task_train)

    t_validate_curated = PythonOperator(
        task_id="validate_curated",
        python_callable=_task_validate_curated,
        retries=0,
    )

    t_evaluate = PythonOperator(task_id="evaluate", python_callable=_task_evaluate)

    t_quality_report = PythonOperator(
        task_id="quality_report",
        python_callable=_task_quality_report,
        trigger_rule="all_done",     
        retries=0,
    )

    t_ingest >> t_transform >> t_validate_staging >> t_storage
    t_storage >> t_train >> t_validate_curated >> t_evaluate >> t_quality_report

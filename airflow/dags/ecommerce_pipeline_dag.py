"""
airflow/dags/ecommerce_pipeline_dag.py
--------------------------------------
Daily pipeline DAG:
  1. ingest_orders      — generate & insert new batch orders
  2. dq_check           — run data quality checks (fail-fast on critical issues)
  3. dbt_run            — transform raw → staging → marts
  4. dbt_test           — validate transformed data
  5. notify_success     — log success summary
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner":            "miko",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR     = "/opt/airflow/dbt_project"
INGEST_DIR  = "/opt/airflow/ingestion"
DQ_DIR      = "/opt/airflow/data_quality"

DB_ENV = (
    "DB_HOST=postgres "
    "DB_PORT=5432 "
    "DB_NAME=ecommerce "
    "DB_USER=pipeline "
    "DB_PASSWORD=pipeline"
)


def check_dq_results(**context):
    """Read latest DQ run from DB; fail task if critical failures exist."""
    import os
    import psycopg2

    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="ecommerce", user="pipeline", password="pipeline"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM raw.dq_results
        WHERE status = 'FAIL'
          AND run_at >= NOW() - INTERVAL '10 minutes'
    """)
    fail_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if fail_count > 0:
        raise ValueError(
            f"DQ check found {fail_count} FAIL(s) — "
            "pipeline halted. Fix data issues and re-run."
        )
    return "dq_passed"


with DAG(
    dag_id="ecommerce_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily e-commerce order analytics pipeline",
    schedule_interval="0 6 * * *",   # 06:00 UTC every day
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "data-engineering", "linkedin-project"],
) as dag:

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=f"{DB_ENV} python {INGEST_DIR}/generate_orders.py --mode batch",
        doc_md="Insert yesterday's simulated orders into raw schema.",
    )

    run_dq_checks = BashOperator(
        task_id="run_dq_checks",
        bash_command=f"{DB_ENV} python {DQ_DIR}/dq_check.py",
        doc_md="Run null/duplicate/range checks. Results saved to raw.dq_results.",
    )

    validate_dq = PythonOperator(
        task_id="validate_dq",
        python_callable=check_dq_results,
        doc_md="Fail the DAG if any critical DQ check failed.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --profiles-dir . --target prod"
        ),
        doc_md="Transform raw → staging views → mart tables.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir . --target prod"
        ),
        doc_md="Run dbt schema + data tests on all models.",
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=(
            'echo "Pipeline completed successfully at $(date). '
            'DAG: ecommerce_pipeline"'
        ),
    )

    # Task dependencies
    ingest_orders >> run_dq_checks >> validate_dq >> dbt_run >> dbt_test >> notify_success

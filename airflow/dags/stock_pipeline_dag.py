from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="stock_market_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stock-market", "data-engineering"],
) as dag:

    run_data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=f"cd {PROJECT_DIR} && python src/quality/data_quality_checks.py",
    )

    build_gold_summary = BashOperator(
        task_id="build_gold_summary",
        bash_command=f"cd {PROJECT_DIR} && python src/batch/build_gold_summary.py",
    )

    run_data_quality_checks >> build_gold_summary

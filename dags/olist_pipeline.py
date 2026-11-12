
from datetime import datetime
import os
import subprocess
import sys

from airflow.sdk import DAG
from airflow.models import Connection
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_DIR = "/mnt/c/Users/User/Desktop/data-eng-project"
PYTHON = "/home/user/venvs/olist-spark/bin/python"

sys.path.insert(0, PROJECT_DIR)
from monitoring.airflow_monitor import record_task


def run_data_quality():
    conn = Connection.get_connection_from_secrets("olist_postgres")

    if not conn.password:
        raise RuntimeError("PostgreSQL password is missing.")

    env = os.environ.copy()
    env["PGPASSWORD"] = conn.password

    script = os.path.join(
        PROJECT_DIR, "quality", "check_data_quality.py"
    )

    subprocess.run(
        [sys.executable, script],
        env=env,
        check=True
    )


with DAG(
    dag_id="olist_pipeline",
    description="Pipeline Data Engineering Olist",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Olist", "Data Engineering"],
    default_args={
        "on_success_callback": record_task,
        "on_failure_callback": record_task,
    },
) as dag:

    bronze = BashOperator(
        task_id="build_bronze",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_bronze.py",
    )

    silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_silver.py",
    )

    gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_gold.py",
    )

    export = BashOperator(
        task_id="export_gold_csv",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/export_gold_csv.py",
    )

    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=run_data_quality,
    )

    bronze >> silver >> gold >> export >> data_quality
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/mnt/c/Users/User/Desktop/data-eng-project"
PYTHON = "/home/user/venvs/olist-spark/bin/python"


with DAG(
    dag_id="olist_pipeline",
    description="Pipeline Data Engineering Olist",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Olist", "Data Engineering"],
) as dag:

    bronze = BashOperator(
        task_id="build_bronze",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_bronze.py",
    )

    silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_silver.py",
    )

    gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/build_gold.py",
    )

    export = BashOperator(
        task_id="export_gold_csv",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_jobs/export_gold_csv.py",
    )

    bronze >> silver >> gold >> export

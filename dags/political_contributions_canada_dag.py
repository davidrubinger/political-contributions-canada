from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="political_contributions_canada",
    start_date=datetime(2004, 1, 1),
    schedule_interval=None,
    max_active_runs=1
)

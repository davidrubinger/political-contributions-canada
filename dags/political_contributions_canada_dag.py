from airflow import DAG
from airflow.models import Variable
from airflow.plugins.operators.unzip_url_operator import UnzipURLOperator
from datetime import datetime

project_dir = Variable.get("project_dir")

dag = DAG(
    dag_id="political_contributions_canada",
    start_date=datetime(2004, 1, 1),
    schedule_interval=None,
    max_active_runs=1
)

unzip_contributions = UnzipURLOperator(
    task_id="unzip_contributions",
    url="https://www.elections.ca/fin/oda/od_cntrbtn_audt_e.zip",
    unzip_dir=f"{project_dir}/data",
    dag=dag
)

unzip_population = UnzipURLOperator(
    task_id="unzip_population",
    url="https://www150.statcan.gc.ca/n1/en/tbl/csv/17100009-eng.zip",
    unzip_dir=f"{project_dir}/data",
    dag=dag
)

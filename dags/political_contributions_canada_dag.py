from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.plugins.operators.unzip_url_operator import UnzipURLOperator
from airflow.plugins.helpers import sql_queries
from airflow.hooks.postgres_hook import PostgresHook
from pyspark.sql import SparkSession
import os
from shutil import rmtree
from datetime import datetime
import re

project_dir = Variable.get("project_dir")

dag = DAG(
    dag_id="political_contributions_canada",
    start_date=datetime(2004, 1, 1),
    schedule_interval=None,
    max_active_runs=1
)

def transform_contributions_func(input_csv_file_name, spark_output_dir):
    
    # Load and transform contributions data
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("political-contributions-canada")
        .getOrCreate()
    )
    if os.path.exists(spark_output_dir):
        rmtree(spark_output_dir)
    (
        spark.read.csv(input_csv_file_name, header=True)
        .selectExpr(
            "`Contribution Received date` as received_date",
            "`Contributor first name` as contributor_first_name",
            "`Contributor middle initial` as contributor_middle_initial",
            "`Contributor last name` as contributor_last_name",
            "`Contributor City` as contributor_city",
            "`Contributor Province` as contributor_province_code",
            "`Contributor Postal code` as contributor_postal_code",
            "`Contributor type` as contributor_type",
            "`Recipient first name` as recipient_first_name",
            "`Recipient middle initial` as recipient_middle_initial",
            "`Recipient last name` as recipient_last_name",
            "`Political Entity` as recipient_entity",
            "`Political Party of Recipient` as recipient_party",
            "`Electoral District` as electoral_district",
            "`Electoral event` as electoral_event",
            "`Fiscal/Election date` as fiscal_election_date",
            "`Monetary amount` as monetary_amount",
            "`Non-Monetary amount` as non_monetary_amount",
            "`Form ID` as report_id",
            "`Financial Report` as report_name",
            "`Part Number of Return` as report_part_number",
            "`Financial Report part` as report_part_name"
        )
        .repartition(1)
        .write
        .option("delimiter", "\t")
        .csv(spark_output_dir, mode="overwrite")
    )

def transform_population_func(input_csv_file_name, spark_output_dir):
    
    # Load and transform contributions data
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("political-contributions-canada")
        .getOrCreate()
    )
    if os.path.exists(spark_output_dir):
        rmtree(spark_output_dir)
    (
        spark.read.csv(input_csv_file_name, header=True)
        .selectExpr(
            "REF_DATE as reference_date",
            "GEO as geography",
            "VALUE as population"
        )
        .repartition(1)
        .write
        .option("delimiter", "\t")
        .csv(spark_output_dir, mode="overwrite")
    )

def load_spark_csv_to_postgres(spark_csv_dir,
                               postgres_conn_id,
                               postgres_table_name):
    """
    Load Spark CSV to Postgres.
    
    Keyword arguments:
    spark_csv_dir -- directory where CSV written from Spark is located
    postgres_conn_id -- ID of Airflow connection
    postgres_table_name -- name of target table in Postgres database
    """
    
    # Get Spark CSV file path
    spark_csv_file_name = list(filter(
        re.compile("part-.*csv$").match, os.listdir(spark_csv_dir)
    ))[0]
    spark_csv_file_path = f"{spark_csv_dir}/{spark_csv_file_name}"
    
    print(
        f"""
        Loading Spark CSV {spark_csv_file_path} into Postgres table {postgres_table_name}
        """
    )
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_hook.bulk_load(postgres_table_name, spark_csv_file_path)

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

transform_contributions_task = PythonOperator(
    task_id="transform_contributions",
    python_callable=transform_contributions_func,
    op_kwargs={
        "input_csv_file_name": f"{project_dir}/data/PoliticalFinance/od_cntrbtn_audt_e.csv",
        "spark_output_dir": f"{project_dir}/contributions"
    },
    dag=dag
)

transform_population_task = PythonOperator(
    task_id="transform_population",
    python_callable=transform_population_func,
    op_kwargs={
        "input_csv_file_name": f"{project_dir}/data/17100009.csv",
        "spark_output_dir": f"{project_dir}/population"
    },
    dag=dag
)

create_contributions_in_postgres = PostgresOperator(
    task_id="create_contributions_in_postgres",
    sql=sql_queries.create_contributions,
    postgres_conn_id="postgres",
    dag=dag
)

create_population_in_postgres = PostgresOperator(
    task_id="create_population_in_postgres",
    sql=sql_queries.create_population,
    postgres_conn_id="postgres",
    dag=dag
)

load_contributions_to_postgres = PythonOperator(
    task_id="load_contributions_to_postgres",
    python_callable=load_spark_csv_to_postgres,
    op_kwargs={
        "spark_csv_dir": f"{project_dir}/contributions",
        "postgres_conn_id": "postgres",
        "postgres_table_name": "contributions"
    },
    dag=dag
)

load_population_to_postgres = PythonOperator(
    task_id="load_population_to_postgres",
    python_callable=load_spark_csv_to_postgres,
    op_kwargs={
        "spark_csv_dir": f"{project_dir}/population",
        "postgres_conn_id": "postgres",
        "postgres_table_name": "population"
    },
    dag=dag
)

unzip_contributions >> transform_contributions_task
transform_contributions_task >> load_contributions_to_postgres
create_contributions_in_postgres >> load_contributions_to_postgres

unzip_population >> transform_population_task
transform_population_task >> load_population_to_postgres
create_population_in_postgres >> load_population_to_postgres

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.plugins.operators.unzip_url_operator import UnzipURLOperator
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

def transform_contributions_func(conn_id,
                                 input_csv_file_name,
                                 spark_output_dir):
    
    postgres_hook = PostgresHook(postgres_conn_id = conn_id)
    
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
    
    # Create contributions table
    contributions_table = "contributions"
    print(f"Creating Postgres table {contributions_table}")
    postgres_hook.run(f"drop table if exists {contributions_table};")
    postgres_hook.run(
        f"""
        create table {contributions_table} (
            received_date text,
            contributor_first_name text,
            contributor_middle_initial text,
            contributor_last_name text,
            contributor_city text,
            contributor_province_code text,
            contributor_postal_code text,
            contributor_type text,
            recipient_first_name text,
            recipient_middle_initial text,
            recipient_last_name text,
            recipient_entity text,
            recipient_party text,
            electoral_district text,
            electoral_event text,
            fiscal_election_date text,
            monetary_amount text,
            non_monetary_amount text,
            report_id text,
            report_name text,
            report_part_number text,
            report_part_name text
        );
        """
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

transform_contributions_task = PythonOperator(
    task_id="transform_contributions",
    python_callable=transform_contributions_func,
    op_kwargs={
        "conn_id": "postgres",
        "input_csv_file_name": f"{project_dir}/data/PoliticalFinance/od_cntrbtn_audt_e.csv",
        "spark_output_dir": f"{project_dir}/contributions"
    },
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

unzip_contributions >> transform_contributions_task
transform_contributions_task >> load_contributions_to_postgres

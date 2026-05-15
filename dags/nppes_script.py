import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

# Add parent directory to Python path so Airflow can find npi_automate.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your script
from dags.scripts import npi_automate

PARQUET_DIR = "/opt/airflow/data/parquet"
S3_BUCKET = "reference-data-platform"
S3_PREFIX = "nppes"
AWS_CONN_ID = "aws_s3"
MANIFEST_FILENAME = "_manifest.json"

# Default arguments
default_args = {
    'owner': 'saurav',
    'depends_on_past': False,
    'retries': False,
}


def extract_data_from_source():
    npi_automate.request_url()


def process_extracted_npi_data():
    npi_automate.process_extracted_npi_data_v2()


def write_and_upload_manifest(**context):
    npi_automate.write_nppes_manifest(airflow_run_id=context.get('run_id'))
    upload_manifest_to_s3()


def upload_file_in_s3(file_path, s3_key, bucket_name=S3_BUCKET, aws_conn_id=AWS_CONN_ID):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True,
    )


def upload_parquet_to_s3():
    parquet_files = [f for f in os.listdir(PARQUET_DIR) if f.endswith('.parquet')]
    if not parquet_files:
        raise FileNotFoundError(f"No parquet file found in {PARQUET_DIR}")
    for entry in parquet_files:
        upload_file_in_s3(
            file_path=os.path.join(PARQUET_DIR, entry),
            s3_key=f"{S3_PREFIX}/{entry}",
        )


def upload_manifest_to_s3():
    manifest_path = os.path.join(PARQUET_DIR, MANIFEST_FILENAME)
    upload_file_in_s3(
        file_path=manifest_path,
        s3_key=f"{S3_PREFIX}/{MANIFEST_FILENAME}",
    )


# DAG definition
with DAG(
    dag_id='nppes_npi_data_pipeline',
    default_args=default_args,
    description='Download and process NPI data into parquet',
    schedule='00 1 17 * *',  
    start_date=datetime(2026, 5, 5),
    catchup=False,
    tags=['nppes','npi', 'healthcare', 'etl'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_nppes_csv_npi_data',
        python_callable=extract_data_from_source
    )

    process_and_dump_data = PythonOperator(
        task_id='process_and_dump_extracted_data',
        python_callable=process_extracted_npi_data
    )

    upload_parquet_task = PythonOperator(
        task_id='upload_parquet_to_s3',
        python_callable=upload_parquet_to_s3
    )

    write_and_upload_manifest_task = PythonOperator(
        task_id='write_and_upload_nppes_manifest',
        python_callable=write_and_upload_manifest
    )

    (
        extract_data
        >> process_and_dump_data
        >> upload_parquet_task
        >> write_and_upload_manifest_task
    )
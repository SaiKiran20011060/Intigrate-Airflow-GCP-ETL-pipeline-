from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago


from scripts.extract import generate_employee_data

# --- Configuration Variables ---
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "MyFirstData"
BUCKET_NAME = "bkt-employee-data"
DATASET_ID = "employeedata"
TABLE_ID = "employee_data"
LOCATION = "us-central1"
DATA_FUSION_PIPELINE_NAME = "datafusion-pipeline"
DATA_FUSION_INSTANCE_NAME = "etl-pipeline"
LOCAL_FILE_PATH = "employee_data.csv"
GCS_OBJECT_NAME = "employee_data.csv"


with DAG(
    dag_id="employee_pipeline_from_script",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["gcp", "data-pipeline", "separated-logic"],
    default_args={
        "owner": "airflow",
        "email": ["saikiranravipati2001@gmail.com"],
        "email_on_failure": False,
    },
    description="Runs a Python script to generate data, then uploads to GCS, loads to BigQuery, and starts Data Fusion.",
) as dag:
    # Task 1: Generate the CSV file by calling the function from extract.py
    task_generate_data = PythonOperator(
        task_id="generate_data_from_script",
        python_callable=generate_employee_data,
    )

    # Task 2: Upload the generated file from the local filesystem to GCS
    task_upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=LOCAL_FILE_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 3: Load the data from the GCS file into a BigQuery table
    task_load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 4: Start the Google Cloud Data Fusion pipeline
    task_start_datafusion = CloudDataFusionStartPipelineOperator(
        task_id="start_datafusion_pipeline",
        location=LOCATION,
        pipeline_name=DATA_FUSION_PIPELINE_NAME,
        instance_name=DATA_FUSION_INSTANCE_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Define Task Dependencies ---
    (
        task_generate_data
        >> task_upload_to_gcs
        >> task_load_to_bigquery
        >> task_start_datafusion
    )







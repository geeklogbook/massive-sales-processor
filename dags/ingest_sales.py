import os
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

SOURCE_DIR = "/sales-processing/data/source"
BUCKET_NAME = "raw"

s3_client = boto3.client(
    's3',
    endpoint_url='http://data-lake:9000/',
    aws_access_key_id='minio',
    aws_secret_access_key='minio123',
    region_name='us-east-1'
)

def upload_files_to_raw(**kwargs):
    """Sube archivos CSV desde SOURCE_DIR al bucket raw y los elimina del origen."""
    for filename in os.listdir(SOURCE_DIR):
        if filename.endswith(".csv"):
            file_path = os.path.join(SOURCE_DIR, filename)
            s3_client.upload_file(file_path, BUCKET_NAME, filename)
            os.remove(file_path)  # Eliminar el archivo después de subirlo
            print(f"Subido y eliminado: {filename}")

with DAG('ingest_sales',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    ingest_task = PythonOperator(
        task_id='upload_csv_to_raw',
        python_callable=upload_files_to_raw,
        provide_context=True
    )

ingest_task

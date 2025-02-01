import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def count_files_in_bronze(**kwargs):
    s3_client = boto3.client(
        's3',
        endpoint_url='http://data-lake:9000/',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        region_name='us-east-1'
    )
    
    bucket_name = 'test'
    
    response = s3_client.list_objects(Bucket=bucket_name)

    file_count = sum(1 for obj in response.get('Contents', []) if obj['Key'].endswith('.csv'))
    

    print(f"Hay {file_count} archivos CSV en el bucket {bucket_name}.")

with DAG('count_files_in_bronze',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    count_files_task = PythonOperator(
        task_id='count_files_task',
        python_callable=count_files_in_bronze,
        provide_context=True
    )

count_files_task
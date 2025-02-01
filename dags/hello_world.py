from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

with DAG(
    dag_id="hello_world_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"]
) as dag:
    
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )

    hello_task

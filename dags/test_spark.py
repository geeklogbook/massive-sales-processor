from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def run_spark_job():
    spark = SparkSession.builder.appName("test-spark").master("spark://spark-master:7077").getOrCreate()

    df = spark.createDataFrame([
        (1, 144.5, 5.9, 33, 'M'),
        (2, 167.2, 5.4, 45, 'M'),
        (3, 124.1, 5.2, 23, 'F'),
        (4, 144.5, 5.9, 33, 'M'),
        (5, 133.2, 5.7, 54, 'F'),
        (3, 124.1, 5.2, 23, 'F'),
        (5, 129.2, 5.3, 42, 'M')
    ], ['id', 'weight', 'height', 'age', 'gender'])

    df = df.dropDuplicates()

    df.show()

    spark.stop()

with DAG(
    dag_id="test_spark",
    schedule_interval=None,
    start_date=datetime(2025, 1, 31),
    catchup=False,
    tags=["test"]
) as dag:
    
    spark_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job
    )

    spark_task
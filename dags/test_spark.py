from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

with DAG(
    dag_id="test_spark",
    default_args=default_args,
    description="Simple Spark DataFrame test",
    schedule_interval=None,
    catchup=False,
    tags=["spark", "test"],
) as dag:
    spark_test = SparkSubmitOperator(
        task_id="spark_dataframe_test",
        application="/opt/airflow/dags/spark_job.py",
        conn_id="spark_local",
        conf={
            "spark.app.name": "airflow-spark-test",
            "spark.executor.memory": "512m",
            "spark.driver.memory": "512m",
            "spark.pyspark.driver.python": "/usr/bin/python3",
            "spark.pyspark.python": "/usr/bin/python3"
        },
        verbose=True
    )

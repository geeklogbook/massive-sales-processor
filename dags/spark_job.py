from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("airflow-spark-test").getOrCreate()

    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["name", "age"])

    df.show()

    spark.stop()
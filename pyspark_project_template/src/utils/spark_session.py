from pyspark.sql import SparkSession

def get_spark(app_name="PySparkApp"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

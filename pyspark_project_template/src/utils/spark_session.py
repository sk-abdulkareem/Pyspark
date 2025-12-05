from pyspark.sql import SparkSession

def get_spark(app): return SparkSession.builder.appName(app).getOrCreate()
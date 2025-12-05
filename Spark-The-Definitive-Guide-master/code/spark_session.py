from pyspark.sql import SparkSession
import time

def get_spark(app_name="MyApp"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://localhost:7077")
        .getOrCreate()
    )
    print("\n👉 Spark started. UI: http://localhost:4040")
    return spark

def keep_ui_alive(minutes=10):
    print(f"⏳ Keeping Spark UI alive for {minutes} minutes...")
    time.sleep(minutes * 60)
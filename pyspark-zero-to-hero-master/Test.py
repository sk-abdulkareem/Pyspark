from pyspark.sql import SparkSession
import time

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started. Check Spark UI at http://localhost:4040")

df = spark.range(100)
df.show()  # Action → keeps job active

time.sleep(1000)  # keep UI open for 1000 seconds
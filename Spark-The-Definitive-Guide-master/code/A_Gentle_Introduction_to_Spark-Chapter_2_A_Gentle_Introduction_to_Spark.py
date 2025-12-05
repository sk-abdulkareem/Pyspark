from spark_session import get_spark
from spark_session import keep_ui_alive

spark = get_spark("File 1 Job")

myRange = spark.range(1000).toDF("number")


# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")


# COMMAND ----------

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("E:/Github/Pyspark/Spark-The-Definitive-Guide-master/data/flight-data/csv/2015-summary.csv")

# COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")


# COMMAND ----------

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

sqlWay.explain()
dataFrameWay.explain()


# COMMAND ----------

from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)


# COMMAND ----------

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


# COMMAND ----------

from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()


# COMMAND ----------

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()


# COMMAND ----------

keep_ui_alive(10)

spark.stop()
print("🛑 Spark stopped.")

 from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 from pyspark.sql.functions import col, expr, cast, lit, when, coalesce, desc, asc, count, sum, avg, row_number
 from pyspark.sql.functions import to_date, current_date, current_timestamp, date_format
 from pyspark.sql.window import Window

 # Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

# PySpark Cluster with 2 worker, 1 master
spark = (
    SparkSession
    .builder
    .appName("Cluster Execution")
    .master("spark://17e348267994:7077")
    .config("spark.executor.instances", 4)
    .config("spark.executor.cores", 4)
    .config("spark.cores.max", 6)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

# Dynamic Allocation
spark = (
    SparkSession
    .builder
    .appName("Dynamic Allocation")
    .master("spark://197e20b418a6:7077")
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "512M")
    .config("spark.dynamicAllocation.enabled", True)
    .config("spark.dynamicAllocation.minExecutors", 0)
    .config("spark.dynamicAllocation.maxExecutors", 5)
    .config("spark.dynamicAllocation.initialExecutors", 1)
    .config("spark.dynamicAllocation.shuffleTracking.enabled", True)
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60s")
    .getOrCreate()
)

# Sql Enable
spark = (
    SparkSession
    .builder
    .appName("Spark SQL")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "/data/output/spark-warehouse")
    .getOrCreate()
)

spark

emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

# Create emp Schema
emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

dept_data = [
    ["101", "Sales", "NYC", "US", "1000000"],
    ["102", "Marketing", "LA", "US", "900000"],
    ["103", "Finance", "London", "UK", "1200000"],
    ["104", "Engineering", "Beijing", "China", "1500000"],
    ["105", "Human Resources", "Tokyo", "Japan", "800000"],
    ["106", "Research and Development", "Perth", "Australia", "1100000"],
    ["107", "Customer Service", "Sydney", "Australia", "950000"]
]

# Create Dept Schema
dept_schema = "department_id string, department_name string, city string, country string, budget string"

# Create emp & dept DataFrame
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
dept = spark.createDataFrame(data=dept_data, schema=dept_schema)

# Check number of partitions
emp.rdd.getNumPartitions()

# Write our first Transformation (EMP salary > 50000)
emp_final = emp.where("salary > 50000")

# Write data as CSV output (ACTION)
emp_final.write.format("csv").save("data/output/1/emp.csv")

# Schema for emp
emp.schema

# Print schema
emp.printSchema()

# Count
emp.count()

# Get unique data
# select distinct emp.* from emp
emp_unique = emp.distinct()

# Small Example for Schema
schema_string = "name string, age int"

schema_spark =  StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Columns and expression
emp["salary"]

# SELECT columns
# select employee_id, name, age, salary from emp
emp_filtered = emp.select(col("employee_id"), expr("name"), emp.age, emp.salary)

# Using expr for select
# select employee_id as emp_id, name, cast(age as int) as age, salary from emp_filtered
emp_casted = emp_filtered.select(expr("employee_id as emp_id"), emp.name, expr("cast(age as int) as age"), emp.salary)

# Using selectExpr
emp_casted_1 = emp_filtered.selectExpr("employee_id as emp_id", "name", "cast(age as int) as age", "salary")

# Filter emp based on Age > 30
# select emp_id, name, age, salary from emp_casted where age > 30
emp_final = emp_casted.select("emp_id", "name", "age", "salary").where("age > 30")

# Write the data back as CSV (ACTION)
emp_final.write.format("csv").save("data/output/2/emp.csv")

# Bonus TIP
schema_str = "name string, age int"
from pyspark.sql.types import _parse_datatype_string

schema_spark = _parse_datatype_string(schema_str)

# Casting Column
# select employee_id, name, age, cast(salary as double) as salary from emp
 emp_casted = emp.select("employee_id", "name", "age", col("salary").cast("double"))

 # Adding Columns
# select employee_id, name, age, salary, (salary * 0.2) as tax from emp_casted
emp_taxed = emp_casted.withColumn("tax", col("salary") * 0.2)

# Literals
# select employee_id, name, age, salary, tax, 1 as columnOne, 'two' as columnTwo from emp_taxed
emp_new_cols = emp_taxed.withColumn("columnOne", lit(1)).withColumn("columnTwo", lit('two'))

# Renaming Columns
# select employee_id as emp_id, name, age, salary, tax, columnOne, columnTwo from emp_new_cols
emp_1 = emp_new_cols.withColumnRenamed("employee_id", "emp_id")

# Column names with Spaces
# select employee_id as emp_id, name, age, salary, tax, columnOne, columnTwo as `Column Two` from emp_new_cols
emp_2 = emp_new_cols.withColumnRenamed("columnTwo", "Column Two")

# Remove Column
emp_dropped = emp_new_cols.drop("columnTwo", "columnOne")

# Filter data 
# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_col_dropped where tax > 1000
emp_filtered = emp_dropped.where("tax > 10000")

# LIMIT data
# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_filtered limit 5
emp_limit = emp_filtered.limit(5)

# Bonus TIP
# Add multiple columns
columns = {
    "tax" : col("salary") * 0.2 ,
    "oneNumber" : lit(1), 
    "columnTwo" : lit("two")
}
emp_final = emp.withColumns(columns)

# Case When
# select employee_id, name, age, salary, gender,
# case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end as new_gender, hire_date from emp
emp_gender_fixed = emp.withColumn("new_gender", when(col("gender") == 'Male', 'M')
                                 .when(col("gender") == 'Female', 'F')
                                 .otherwise(None)
                                 )
emp_gender_fixed_1 = emp.withColumn("new_gender", expr("case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end"))

# Replace in Strings
# select employee_id, name, replace(name, 'J', 'Z') as new_name, age, salary, gender, new_gender, hire_date from emp_gender_fixed
from pyspark.sql.functions import regexp_replace

emp_name_fixed = emp_gender_fixed.withColumn("new_name", regexp_replace(col("name"), "J", "Z"))

# Convert Date
# select *,  to_date(hire_date, 'YYYY-MM-DD') as hire_date from emp_name_fixed
emp_date_fix = emp_name_fixed.withColumn("hire_date", to_date(col("hire_date"), 'yyyy-MM-dd'))

# Add Date Columns
# Add current_date, current_timestamp, extract year from hire_date
emp_dated = emp_date_fix.withColumn("date_now", current_date()).withColumn("timestamp_now", current_timestamp())

# Drop Null gender records
emp_1 = emp_dated.na.drop()

# Fix Null values
# select *, nvl('new_gender', 'O') as new_gender from emp_dated
emp_null_df = emp_dated.withColumn("new_gender", coalesce(col("new_gender"), lit("O")))

# Drop old columns and Fix new column names
emp_final = emp_null_df.drop("name", "gender").withColumnRenamed("new_name", "name").withColumnRenamed("new_gender", "gender")

# Bonus TIP
# Convert date into String and extract date information
emp_fixed = emp_final.withColumn("date_year", date_format(col("timestamp_now"), "z"))

emp_data_1 = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"]
]

emp_data_2 = [
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# Create emp DataFrame
emp_data_1 = spark.createDataFrame(data=emp_data_1, schema=emp_schema)
emp_data_2 = spark.createDataFrame(data=emp_data_2, schema=emp_schema)

# UNION and UNION ALL
# select * from emp_data_1 UNION select * from emp_data_2
emp = emp_data_1.unionAll(emp_data_2)

# Sort the emp data based on desc Salary
# select * from emp order by salary desc
emp_sorted = emp.orderBy(col("salary").asc())

# Aggregation
# select dept_id, count(employee_id) as total_dept_count from emp_sorted group by dept_id 
emp_count = emp_sorted.groupBy("department_id").agg(count("employee_id").alias("total_dept_count"))

# Aggregation
# select dept_id, sum(salary) as total_dept_salary from emp_sorted group by dept_id 
emp_sum = emp_sorted.groupBy("department_id").agg(sum("salary").alias("total_dept_salary"))

# Bonus TIP - unionByName
# In case the column sequence is different
emp_data_2_other = emp_data_2.select("employee_id", "salary", "department_id", "name", "hire_date", "gender", "age")
emp_fixed = emp_data_1.unionByName(emp_data_2_other)

# Get unique data
# select distinct emp.* from emp
emp_unique = emp.distinct()

# Window Functions
# select *, max(salary) over(partition by department_id order by salary desc) as max_salary from emp_unique
window_spec = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())
max_func = max(col("salary")).over(window_spec)
emp_1 = emp.withColumn("max_salary", max_func)

# Window Functions - 2nd highest salary of each department
# select *, row_number() over(partition by department_id order by salary desc) as rn from emp_unique where rn = 2
window_spec = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())
rn = row_number().over(window_spec)
emp_2 = emp.withColumn("rn", rn).where("rn = 2")

# Window function using expr
# select *, row_number() over(partition by department_id order by salary desc) as rn from emp_unique where rn = 2
emp_3 = emp.withColumn("rn", expr("row_number() over(partition by department_id order by salary desc)")).where("rn = 2")

# Repartition of data using repartition & coalesce
emp_partitioned = emp.repartition(4, "department_id")

# Find the partition info for partitions and reparition
from pyspark.sql.functions import spark_partition_id
emp_1 = emp.repartition(4, "department_id").withColumn("partition_num", spark_partition_id())

# INNER JOIN datasets
# select e.emp_name, d.department_name, d.department_id, e.salary 
# from emp e inner join dept d on emp.department_id = dept.department_id
df_joined = emp.alias("e").join(dept.alias("d"), how="inner", on=emp.department_id==dept.department_id)

# LEFT OUTER JOIN datasets
# select e.emp_name, d.department_name, d.department_id, e.salary 
# from emp e left outer join dept d on emp.department_id = dept.department_id
df_joined = emp.alias("e").join(dept.alias("d"), how="left_outer", on=emp.department_id==dept.department_id)

# Bonus TIP
# Joins with cascading conditions
# Join with Department_id and only for departments 101 or 102
# Join with not null/null conditions

df_final = emp.join(dept, how="left_outer", 
                   on=(emp.department_id==dept.department_id) & ((emp.department_id == "101") | (emp.department_id == "102")) 
                    & (emp.salary.isNull())
                   )

# Read a csv file into dataframe
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("data/input/emp.csv")

# Reading with Schema
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_schema = spark.read.format("csv").option("header",True).schema(_schema).load("data/input/emp.csv")

# Handle BAD records - PERMISSIVE (Default mode)
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date, bad_record string"
df_p = spark.read.format("csv").schema(_schema).option("columnNameOfCorruptRecord", "bad_record").option("header", True).load("data/input/emp_new.csv")

# Handle BAD records - DROPMALFORMED
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_m = spark.read.format("csv").option("header", True).option("mode", "DROPMALFORMED").schema(_schema).load("data/input/emp_new.csv")

# Handle BAD records - FAILFAST
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_m = spark.read.format("csv").option("header", True).option("mode", "FAILFAST").schema(_schema).load("data/input/emp_new.csv")

# BONUS TIP
# Multiple options
_options = {
    "header" : "true",
    "inferSchema" : "true",
    "mode" : "PERMISSIVE"
}
df = (spark.read.format("csv").options(**_options).load("data/input/emp.csv"))

# Read Parquet Sales data
df_parquet = spark.read.format("parquet").load("data/input/sales_total_parquet/*.parquet")

# Read ORC Sales data
df_orc = spark.read.format("orc").load("data/input/sales_total_orc/*.orc")

# Benefits of Columnar Storage
# Lets create a simple Python decorator - {get_time} to get the execution timings
# If you dont know about Python decorators - check out : https://www.geeksforgeeks.org/decorators-in-python/
import time

def get_time(func):
    def inner_get_time() -> str:
        start_time = time.time()
        func()
        end_time = time.time()
        return (f"Execution time: {(end_time - start_time)*1000} ms")
    print(inner_get_time())

@get_time
def x():
    df = spark.read.format("parquet").load("data/input/sales_data.parquet")
    df.count()

@get_time
def x():
    df = spark.read.format("parquet").load("data/input/sales_data.parquet")
    df.select("trx_id").count()

# BONUS TIP
# RECURSIVE READ
sales_recursive
|__ sales_1\1.parquet
|__ sales_1\sales_2\2.parquet
df_1 = spark.read.format("parquet").option("recursiveFileLookup", True).load("data/input/sales_recursive/")

# Read Single line JSON file
df_single = spark.read.format("json").load("data/input/order_singleline.json")

# For single value output
df = spark.read.format("text").load("data/input/order_singleline.json")

# Read Multiline JSON file
df_multi = spark.read.format("json").option("multiLine", True).load("data/input/order_multiline.json")

# With Schema
_schema = "customer_id string, order_id string, contact array<long>"
df_schema = spark.read.format("json").schema(_schema).load("data/input/order_singleline.json")

# 
_schema = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"
df_schema_new = spark.read.format("json").schema(_schema).load("data/input/order_singleline.json")

# Function from_json to read from a column
_schema = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"
from pyspark.sql.functions import from_json, to_json, explode
df_expanded = df.withColumn("parsed", from_json(df.value, _schema))

# Function to_json to parse a JSON string
df_unparsed = df_expanded.withColumn("unparsed", to_json(df_expanded.parsed))

# Get values from Parsed JSON
df_1 = df_expanded.select("parsed.*")
df_2 = df_1.withColumn("expanded_line_items", explode("order_line_items"))
df_3 = df_2.select("contact", "customer_id", "order_id", "expanded_line_items.*")

# Explode Array fields
df_final = df_3.withColumn("contact_expanded", explode("contact"))

# Drop column
df_final.drop("contact").show()

# Write the data with Partition to output location
emp.write.format("csv").partitionBy("department_id").option("header", True).save("data/output/11/4/emp.csv")

# Write Modes - append, overwrite, ignore and error
emp.write.format("csv").mode("error").option("header", True).save("data/output/11/3/emp.csv")

# Bonus TIP
# What if we need to write only 1 output file to share with DownStream?
emp.repartition(1).write.format("csv").option("header", True).save("data/output/11/5/emp.csv")

# Create a function to generate 10% of Salary as Bonus
def bonus(salary):
    return int(salary) * 0.1

# Register as UDF
from pyspark.sql.functions import udf
bonus_udf = udf(bonus)
spark.udf.register("bonus_sql_udf", bonus, "double")

# Create new column as bonus using UDF
emp.withColumn("bonus", expr("bonus_sql_udf(salary)")).show()

# Create new column as bonus without UDF - To check performance
emp.withColumn("bonus", expr("salary * 0.1")).show()

# Disable AQE and Broadcast join
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Check default Parallism
spark.sparkContext.defaultParallelism

# Read EMP CSV file with 10M records
_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"
emp = spark.read.format("csv").schema(_schema).option("header", True).load("/data/input/datasets/employee_records.csv")

# Find out avg salary as per dept
emp_avg = emp.groupBy("department_id").agg(avg("salary").alias("avg_sal"))

# Write data for performance Benchmarking
emp_avg.write.format("noop").mode("overwrite").save()

# Check Spark Shuffle Partition setting
spark.conf.get("spark.sql.shuffle.partitions")

# Set Spark Shuffle Partition setting
spark.conf.set("spark.sql.shuffle.partitions", 16)

# Cache DataFrame (cache or persist)
df_cache = df.where("amount > 100").cache()

# MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK_2
import pyspark
df_persist = df.persist(pyspark.StorageLevel.MEMORY_ONLY_2)

# Remove Cache
spark.catalog.clearCache()

# Variable (Lookup)
dept_names = {1 : 'Department 1', 
              2 : 'Department 2', 
              3 : 'Department 3', 
              4 : 'Department 4',
              5 : 'Department 5', 
              6 : 'Department 6', 
              7 : 'Department 7', 
              8 : 'Department 8', 
              9 : 'Department 9', 
              10 : 'Department 10'}

# Broadcast the variable
broadcast_dept_names = spark.sparkContext.broadcast(dept_names)

# Check the value of the variable
broadcast_dept_names.value

# Create UDF to return Department name
@udf
def get_dept_names(dept_id):
    return broadcast_dept_names.value.get(dept_id)

emp_final = emp.withColumn("dept_name", get_dept_names(col("department_id")))

# Calculate total salary of Department 6
emp.where("department_id = 6").groupBy("department_id").agg(sum("salary").cast("long")).show()

# Accumulators
dept_sal = spark.sparkContext.accumulator(0)

# Use foreach
def calculate_salary(department_id, salary):
    if department_id == 6:
        dept_sal.add(salary)

emp.foreach(lambda row : calculate_salary(row.department_id, row.salary))

# View total value
dept_sal.value

#
# Join Datasets - Big and Small Table 
from pyspark.sql.functions import broadcast
df_joined = emp.join(broadcast(dept), on=emp.department_id==dept.department_id, how="left_outer")

# Join Big and Big table - SortMerge without Buckets
df_sales_joined = sales.join(city, on=sales.city_id==city.city_id, how="left_outer")

# Write Sales data in Buckets
sales.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/sales_bucket.csv").saveAsTable("sales_bucket")

# Write City data in Buckets
city.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/city_bucket.csv").saveAsTable("city_bucket")

# Join Sales and City data - SortMerge with Bucket
df_joined_bucket = sales_bucket.join(city_bucket, on=sales_bucket.city_id==city_bucket.city_id, how="left_outer")

# Join Datasets
df_joined = emp.join(dept, on=emp.department_id==dept.department_id, how="left_outer")

# Check the partition details to understand distribution
part_df = df_joined.withColumn("partition_num", spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count"))

# Verify Employee data based on department_id
emp.groupBy("department_id").agg(count(lit(1))).show()

# Set shuffle partitions to a lesser number - 16
spark.conf.set("spark.sql.shuffle.partitions", 32)

# Let prepare the salt
import random
# UDF to return a random number every time and add to Employee as salt
@udf
def salt_udf():
    return random.randint(0, 32)

# Salt Data Frame to add to department
salt_df = spark.range(0, 32)

# Salted Employee
salted_emp = emp.withColumn("salted_dept_id", concat("department_id", lit("_"), salt_udf()))

# Salted Department
salted_dept = dept.join(salt_df, how="cross").withColumn("salted_dept_id", concat("department_id", lit("_"), "id"))

# Lets make the salted join now
salted_joined_df = salted_emp.join(salted_dept, on=salted_emp.salted_dept_id==salted_dept.salted_dept_id, how="left_outer")

# Check the partition details to understand distribution
part_df = salted_joined_df.withColumn("partition_num", spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count"))

# Disable AQE - Its enabled by default
# Coalescing post-shuffle partitions - remove un-necessary shuffle partitions
# Skewed join optimization (balance partitions size) - join smaller partitions and split bigger partition
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)

# Fix partition sizes to avoid Skew
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB") #Default value: 64MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "10MB") #Default value: 256MB

# Converting sort-merge join to broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Join Datasets - without specifying specific broadcast table
df_joined = emp.join(dept, on=emp.department_id==dept.department_id, how="left_outer")

# Spark Catalog (Metadata) - in-memory/hive
spark.conf.get("spark.sql.catalogImplementation")

spark.sql("show databases")
spark.sql("show tables in default")
emp.createOrReplaceTempView("emp_view")
emp_filtered = spark.sql("""
    select * from emp_view
    where department_id = 1
""")
spark.sql("""
    select e.*, date_format(dob, 'yyyy') as dob_year from emp_view e
""")
emp_final = spark.sql("""
    select /*+ BROADCAST(d) */
    e.* , d.department_name
    from emp_view e left outer join dept_view d
    on e.department_id = d.department_id
""")
emp_final.write.format("parquet").saveAsTable("emp_final")
spark.sql("select * from emp_final")
spark.sql("describe extended emp_final").show() 
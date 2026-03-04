from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("Build Staging Layer") \
    .getOrCreate()

# PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5432/loan_dw"
properties = {
    "user": "loan_user",
    "password": "loan_pass",
    "driver": "org.postgresql.Driver"
}

# STEP 1: Read raw table with parallel JDBC partitioning
df = spark.read.jdbc(
    url=url,
    table="raw.application_train",
    column="sk_id_curr",
    lowerBound=100000,
    upperBound=200000,
    numPartitions=4,
    properties=properties
)

print("Initial row count:", df.count())

# STEP 2: Standardize column names
for column in df.columns:
    new_col = column.lower().replace(" ", "_").replace("-", "_")
    df = df.withColumnRenamed(column, new_col)

# STEP 3: Cast numeric columns
df = df.withColumn("amt_income_total", col("amt_income_total").cast(DoubleType()))
df = df.withColumn("cnt_children", col("cnt_children").cast(IntegerType()))

# STEP 4: Handle nulls
df = df.fillna({
    "amt_income_total": 0,
    "cnt_children": 0
})

# STEP 5: Remove duplicates
df = df.dropDuplicates()

print("Final row count:", df.count())

# STEP 6: Write to staging schema
df.write.jdbc(
    url=url,
    table="staging.application_train",
    mode="overwrite",
    properties=properties
)

print("Successfully written to staging.application_train")

spark.stop()
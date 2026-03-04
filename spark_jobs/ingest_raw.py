from pyspark.sql import SparkSession
import os

# Spark Session
spark = SparkSession.builder \
    .appName("LoanDW-Raw-Ingestion") \
    .config("spark.jars", "jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Base folder
base_path = "/Users/revanth/Downloads/home-credit-default-risk/"

# List of files
files = [
    "application_train.csv",
    "bureau.csv",
    "bureau_balance.csv",
    "previous_application.csv",
    "POS_CASH_balance.csv",
    "credit_card_balance.csv",
    "installments_payments.csv"
]

# Loop through files
for file in files:
    
    table_name = file.replace(".csv", "").lower()
    full_path = os.path.join(base_path, file)

    print(f"Ingesting {file} into raw.{table_name}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(full_path)

    print(f"Row count for {file}: {df.count()}")

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/loan_dw") \
        .option("dbtable", f"raw.{table_name}") \
        .option("user", "loan_user") \
        .option("password", "loan_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print(f"Finished loading raw.{table_name}\n")

print("All files successfully ingested into raw schema.")
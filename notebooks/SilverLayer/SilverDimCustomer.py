# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SILVER LAYER - CREATING DIM_CUSTOMER

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("\n Connection configured")

# Reading Bronze Delta table

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/supply_chain_bronze_delta/"
df_bronze = spark.read.format("delta").load(bronze_path)

print(f"   Total records in Bronze: {df_bronze.count():,}")

# Selecting customer columns 

df_customer = df_bronze.select(
    col("customer_id"),
    col("customer_fname"),
    col("customer_lname"),
    col("customer_email"),
    col("customer_segment"),
    col("customer_city"),
    col("customer_state"),
    col("customer_country"),
    col("customer_zipcode"),
    col("customer_street")
).distinct()  # Remove duplicates - each customer should appear only once

print(f"   Unique customers: {df_customer.count():,}")

# Running data quality checks

# Check for null customer_id
null_ids = df_customer.filter(col("customer_id").isNull()).count()
print(f"   Null customer_id count: {null_ids}")

# Check for duplicate customer_id
duplicate_ids = df_customer.groupBy("customer_id").count().filter(col("count") > 1).count()
print(f"   Duplicate customer_id count: {duplicate_ids}")

# Show null counts per column
print("\n   Null counts per column:")
null_counts = df_customer.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_customer.columns
])
display(null_counts)

# Adding metadata columns

df_customer_final = df_customer \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_customer_final.columns)}")

# Writing to Silver Delta table...")

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
dim_customer_path = f"{silver_path}dim_customer/"

df_customer_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_customer_path)

print(f" Data written to: {dim_customer_path}")

# Verifying Silver dim_customer table

df_verify = spark.read.format("delta").load(dim_customer_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.limit(10))

# Show schema
print("\n Table Schema:")
df_verify.printSchema()

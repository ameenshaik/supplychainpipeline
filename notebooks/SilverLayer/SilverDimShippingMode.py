# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Reading Bronze Delta table

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/supply_chain_bronze_delta/"
df_bronze = spark.read.format("delta").load(bronze_path)

print(f"   Total records in Bronze: {df_bronze.count():,}")

# Selecting shipping mode columns

df_shipping = df_bronze.select(
    col("shipping_mode"),
    col("days_for_shipment")
).distinct()  # Remove duplicates

print(f"   Unique shipping modes: {df_shipping.count()}")

# Running data quality checks

# Show all shipping modes
print("\n   Shipping Modes Distribution:")
display(df_shipping.groupBy("shipping_mode", "days_for_shipment") \
        .count() \
        .orderBy("shipping_mode"))

# Check for nulls
print("\n   Null counts per column:")
null_counts = df_shipping.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_shipping.columns
])
display(null_counts)

# Creating surrogate key (shipping_mode_id)

# Create a unique ID using row_number()
window_spec = Window.orderBy("shipping_mode")

df_shipping_with_id = df_shipping \
    .withColumn("shipping_mode_id", row_number().over(window_spec))

print(f"   shipping_mode_id created")

# Reordering columns

df_shipping_ordered = df_shipping_with_id.select(
    "shipping_mode_id",
    "shipping_mode",
    "days_for_shipment"
)

# Adding metadata columns

df_shipping_final = df_shipping_ordered \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_shipping_final.columns)}")

# Writing to Silver Delta table

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
dim_shipping_mode_path = f"{silver_path}dim_shipping_mode/"

df_shipping_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_shipping_mode_path)

print(f" Data written to: {dim_shipping_mode_path}")

# Verifying Silver dim_shipping_mode table

df_verify = spark.read.format("delta").load(dim_shipping_mode_path)

print(f"   Total records: {df_verify.count()}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show all records (should be small)
print("\n All Shipping Modes:")
display(df_verify.orderBy("shipping_mode_id"))

# Show schema
print("\n Table Schema:")
df_verify.printSchema()
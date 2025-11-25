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

# Selecting geography columns

df_geography = df_bronze.select(
    col("order_city").alias("city"),
    col("order_state").alias("state"),
    col("order_country").alias("country"),
    col("order_region").alias("region"),
    col("market"),
    col("latitude"),
    col("longitude"),
    col("order_zipcode").alias("zipcode")
).distinct()  # Remove duplicates

print(f"   Unique geography combinations: {df_geography.count():,}")

# Running data quality checks

# Show null counts per column
print("\n   Null counts per column:")
null_counts = df_geography.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_geography.columns
])
display(null_counts)

# Show top countries
print("\n   Top 10 Countries:")
display(df_geography.groupBy("country").count().orderBy(col("count").desc()).limit(10))

# Show markets
print("\n   Markets:")
display(df_geography.groupBy("market").count().orderBy(col("count").desc()))

# Show regions
print("\n   Regions:")
display(df_geography.groupBy("region").count().orderBy(col("count").desc()))

# Creating surrogate key (geography_id)

# Create a unique ID using row_number()
window_spec = Window.orderBy("country", "state", "city", "zipcode")

df_geography_with_id = df_geography \
    .withColumn("geography_id", row_number().over(window_spec))

print(f"    geography_id created (1 to {df_geography_with_id.count()})")

# Reordering columns

df_geography_ordered = df_geography_with_id.select(
    "geography_id",
    "city",
    "state", 
    "country",
    "region",
    "market",
    "latitude",
    "longitude",
    "zipcode"
)

# dding metadata columns

df_geography_final = df_geography_ordered \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_geography_final.columns)}")

# Writing to Silver Delta table

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
dim_geography_path = f"{silver_path}dim_geography/"

df_geography_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_geography_path)

print(f" Data written to: {dim_geography_path}")

# Verifying Silver dim_geography table

df_verify = spark.read.format("delta").load(dim_geography_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.orderBy("geography_id").limit(10))

# Show schema
print("\n Table Schema:")
df_verify.printSchema()

# Show geography_id range
print("\n Geography ID Range:")
df_verify.agg(
    min("geography_id").alias("min_geography_id"),
    max("geography_id").alias("max_geography_id")
).show()

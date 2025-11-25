# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


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

# Selecting product columns

df_product = df_bronze.select(
    col("product_card_id"),
    col("product_name"),
    col("product_description"),
    col("product_price"),
    col("product_status"),
    col("product_image"),
    col("category_id"),
    col("category_name"),
    col("product_category_id"),
    col("department_id"),
    col("department_name")
).distinct()  # Remove duplicates - each product should appear only once

print(f"   Unique products: {df_product.count():,}")

# Running data quality checks

# Check for null product_card_id
null_ids = df_product.filter(col("product_card_id").isNull()).count()
print(f"   Null product_card_id count: {null_ids}")

# Check for duplicate product_card_id
duplicate_ids = df_product.groupBy("product_card_id").count().filter(col("count") > 1).count()
print(f"   Duplicate product_card_id count: {duplicate_ids}")

# Show null counts per column
print("\n   Null counts per column:")
null_counts = df_product.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_product.columns
])
display(null_counts)

# Show category distribution
print("\n   Products per Category:")
display(df_product.groupBy("category_name").count().orderBy(col("count").desc()).limit(10))

# Show department distribution
print("\n   Products per Department:")
display(df_product.groupBy("department_name").count().orderBy(col("count").desc()))

# Applying transformations

df_product_transformed = df_product \
    .withColumn("product_price", col("product_price").cast("decimal(10,2)")) \
    .withColumn("product_status_desc", 
                when(col("product_status") == 0, "Inactive")
                .when(col("product_status") == 1, "Active")
                .otherwise("Unknown"))

print("   Price converted to decimal(10,2)")
print("   Product status decoded")

# Adding metadata columns

df_product_final = df_product_transformed \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_product_final.columns)}")

# Writing to Silver Delta table

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
dim_product_path = f"{silver_path}dim_product/"

df_product_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_product_path)

print(f" Data written to: {dim_product_path}")

# Verifying Silver dim_product table

df_verify = spark.read.format("delta").load(dim_product_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.select(
    "product_card_id",
    "product_name",
    "category_name",
    "product_price",
    "product_status_desc",
    "department_name"
).limit(10))

# Show schema
print("\n📋 Table Schema:")
df_verify.printSchema()
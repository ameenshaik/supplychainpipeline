# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

print("BRONZE LAYER - INGESTING RAW DATA")

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"


spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("\nConnection configured")

# Read raw CSV data

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"

df_raw = spark.read.csv(
    f"{bronze_path}DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True,
    encoding='ISO-8859-1'
)

print(f"   Records read: {df_raw.count():,}")
print(f"   Columns: {len(df_raw.columns)}")

print("\n Cleaning column names...")

def clean_column_name(col_name):
    """
    Clean column names by:
    - Converting to lowercase
    - Replacing spaces with underscores
    - Removing special characters
    """
    # Convert to lowercase
    col_name = col_name.lower()
    # Replace spaces with underscores
    col_name = col_name.replace(" ", "_")
    # Remove parentheses and their contents
    col_name = re.sub(r'\([^)]*\)', '', col_name)
    # Remove any remaining special characters except underscores
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)
    # Remove trailing underscores
    col_name = col_name.strip('_')
    # Replace multiple underscores with single underscore
    col_name = re.sub(r'_+', '_', col_name)
    
    return col_name

# Apply cleaning to all columns
old_columns = df_raw.columns
new_columns = [clean_column_name(col) for col in old_columns]

# Show some examples of column name changes
print("\n   Example column name transformations:")
# for i in range(min(5, len(old_columns))):
for i in range(5 if len(old_columns) > 5 else len(old_columns)):
    print(f"   '{old_columns[i]}' → '{new_columns[i]}'")

# Rename columns
df_cleaned = df_raw.toDF(*new_columns)

print(f"\n Column names cleaned!")

# Adding metadata columns

df_bronze = df_cleaned \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("ingestion_date", current_date()) \
    .withColumn("source_file", lit("DataCoSupplyChainDataset.csv")) \
    .withColumn("data_source", lit("Kaggle")) \
    .withColumn("pipeline_run_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))

print(f"   Metadata columns added")
print(f"   Total columns now: {len(df_bronze.columns)}")

# Writing to Bronze Delta table

bronze_delta_path = f"{bronze_path}supply_chain_bronze_delta/"

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_delta_path)

print(f" Data written to: {bronze_delta_path}")

# Verifying Bronze Delta table

df_verify = spark.read.format("delta").load(bronze_delta_path)

print(f"   Total records in Delta table: {df_verify.count():,}")
print(f"   Total columns in Delta table: {len(df_verify.columns)}")

# Show schema with cleaned names
print("\n Sample of cleaned column names:")
for col in df_verify.columns[:10]:
    print(f"   - {col}")

# Show sample with metadata
print("\n Sample data:")
display(df_verify.select(
    "order_id",
    "order_status",
    "customer_city",
    "sales",
    "ingestion_timestamp",
    "ingestion_date"
).limit(5))
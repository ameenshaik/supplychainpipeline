# Databricks notebook source
from pyspark.sql.functions import *


# Setup connection
storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"


spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
parquet_export_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/powerbi_parquet/"

print(f" Export location: {parquet_export_path}")
#Exporting tables

# Table 1: GoldFactSales
print("\n Exporting GoldFactSales...")
df1 = spark.read.format("delta").load(f"{gold_path}GoldFactSales/")
df1.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{parquet_export_path}GoldFactSales/")
print(f"    Exported {df1.count():,} records")

# Table 2: GoldFactDeliveryPerformance
print("\n Exporting GoldFactDeliveryPerformance...")
df2 = spark.read.format("delta").load(f"{gold_path}GoldFactDeliveryPerformance/")
df2.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{parquet_export_path}GoldFactDeliveryPerformance/")
print(f"    Exported {df2.count():,} records")

# Table 3: GoldDimProductPerformance
print("\n Exporting GoldDimProductPerformance...")
df3 = spark.read.format("delta").load(f"{gold_path}GoldDimProductPerformance/")
df3.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{parquet_export_path}GoldDimProductPerformance/")
print(f"    Exported {df3.count():,} records")

# Table 4: GoldDimCustomerInsights
print("\n Exporting GoldDimCustomerInsights...")
df4 = spark.read.format("delta").load(f"{gold_path}GoldDimCustomerInsights/")
df4.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{parquet_export_path}GoldDimCustomerInsights/")
print(f"    Exported {df4.count():,} records")


# Databricks notebook source

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Read the data
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"
df = spark.read.csv(
    f"{bronze_path}DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True,
    encoding='ISO-8859-1'
)


print(" DATASET OVERVIEW")

# Basic info
print(f"\n Total Records: {df.count():,}")
print(f" Total Columns: {len(df.columns)}")

# Show schema
print("\n Schema:")
df.printSchema()

# Show column names
print("\n Column Names:")
for i, col in enumerate(df.columns, 1):
    print(f"{i}. {col}")

# Display first few rows
print("\n First 10 Rows:")
display(df.limit(10))
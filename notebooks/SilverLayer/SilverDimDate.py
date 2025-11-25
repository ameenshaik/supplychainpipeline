# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Reading Bronze Delta table to determine date range

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/supply_chain_bronze_delta/"
df_bronze = spark.read.format("delta").load(bronze_path)

# Get min and max dates from order_date column
# Convert string dates to proper date format in PySpark
df_with_dates = df_bronze \
    .withColumn("order_date_parsed", to_timestamp(col("order_date"), "M/d/yyyy H:mm"))

date_range = df_with_dates.select(
    min("order_date_parsed").alias("min_date"),
    max("order_date_parsed").alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

print(f"   Date range in data: {min_date} to {max_date}")

# Extend the range a bit (add buffer)
min_date = min_date - timedelta(days=365)  # 1 year before
max_date = max_date + timedelta(days=365)  # 1 year after

print(f"   Extended date range: {min_date.date()} to {max_date.date()}")

# Generating date dimension

# Calculate number of days
num_days = (max_date - min_date).days + 1
print(f"   Total days to generate: {num_days:,}")

# Create list of dates
date_list = [min_date + timedelta(days=x) for x in range(num_days)]

# Create DataFrame from date list
df_dates = spark.createDataFrame([(d,) for d in date_list], ["date"])

# Adding date attributes

df_dim_date = df_dates \
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("integer")) \
    .withColumn("date", col("date").cast("date")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("day", dayofmonth(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("day_of_week_name", date_format(col("date"), "EEEE")) \
    .withColumn("week_of_year", weekofyear(col("date"))) \
    .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), 1).otherwise(0)) \
    .withColumn("fiscal_year", when(month(col("date")) >= 4, year(col("date")) + 1).otherwise(year(col("date")))) \
    .withColumn("fiscal_quarter", 
                when(month(col("date")).between(4, 6), 1)
                .when(month(col("date")).between(7, 9), 2)
                .when(month(col("date")).between(10, 12), 3)
                .otherwise(4))

print("    Date attributes added:")
print("      - date_id (primary key in format: YYYYMMDD)")
print("      - year, quarter, month, day")
print("      - month_name, day_of_week_name")
print("      - week_of_year, is_weekend")
print("      - fiscal_year, fiscal_quarter")

# Reordering columns

df_dim_date_ordered = df_dim_date.select(
    "date_id",
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "day",
    "day_of_week",
    "day_of_week_name",
    "week_of_year",
    "is_weekend",
    "fiscal_year",
    "fiscal_quarter"
)

# Adding metadata columns

df_dim_date_final = df_dim_date_ordered \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_dim_date_final.columns)}")

# Writing to Silver Delta table

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
dim_date_path = f"{silver_path}dim_date/"

df_dim_date_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_date_path)

print(f" Data written to: {dim_date_path}")

# Verifying Silver dim_date table

df_verify = spark.read.format("delta").load(dim_date_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show date range
print("\n Date Range:")
df_verify.agg(
    min("date").alias("min_date"),
    max("date").alias("max_date")
).show()

# Show sample
print("\n Sample dates:")
display(df_verify.orderBy("date_id").limit(10))

# Show schema
print("\n Table Schema:")
df_verify.printSchema()

# Show year distribution
print("\n Records per Year:")
display(df_verify.groupBy("year").count().orderBy("year"))
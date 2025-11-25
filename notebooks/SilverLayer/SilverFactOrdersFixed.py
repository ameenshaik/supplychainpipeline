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

# Reading dimension tables

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"

dim_customer = spark.read.format("delta").load(f"{silver_path}dim_customer/")
dim_product = spark.read.format("delta").load(f"{silver_path}dim_product/")
dim_geography = spark.read.format("delta").load(f"{silver_path}dim_geography/")
dim_shipping_mode = spark.read.format("delta").load(f"{silver_path}dim_shipping_mode/")
dim_date = spark.read.format("delta").load(f"{silver_path}dim_date/")

print(f"    All dimension tables loaded")

# Preparing fact table columns

df_fact = df_bronze.select(
    # Primary Keys
    col("order_id"),
    col("order_item_id"),
    
    # Foreign Keys
    col("customer_id"),
    col("product_card_id"),
    
    # Geography columns for joining (EXCLUDING zipcode for better matching)
    col("order_city").alias("city"),
    col("order_state").alias("state"),
    col("order_country").alias("country"),
    col("order_region").alias("region"),
    col("market"),
    
    # Shipping mode for joining
    col("shipping_mode"),
    
    # Dates
    col("order_date"),
    col("shipping_date"),
    
    # Order Details
    col("type"),
    col("order_status"),
    col("delivery_status"),
    col("late_delivery_risk"),
    
    # Measures
    col("order_item_quantity"),
    col("order_item_product_price"),
    col("order_item_discount"),
    col("order_item_discount_rate"),
    col("order_item_profit_ratio"),
    col("order_item_total"),
    col("sales"),
    col("sales_per_customer"),
    col("benefit_per_order"),
    col("order_profit_per_order"),
    col("days_for_shipping"),
    col("days_for_shipment")
)

print(f"   Columns selected: {len(df_fact.columns)}")

# Parsing dates and creating date keys

df_fact = df_fact \
    .withColumn("order_date_parsed", to_timestamp(col("order_date"), "M/d/yyyy H:mm")) \
    .withColumn("shipping_date_parsed", to_timestamp(col("shipping_date"), "M/d/yyyy H:mm")) \
    .withColumn("order_date_id", date_format(col("order_date_parsed"), "yyyyMMdd").cast("integer")) \
    .withColumn("shipping_date_id", date_format(col("shipping_date_parsed"), "yyyyMMdd").cast("integer"))

print("    Dates parsed and date_ids created")

# Joining with dim_geography (using improved join strategy)

# Create a unique geography lookup without duplicates
# Use window function to pick one geography_id per combination
from pyspark.sql.window import Window

window_spec = Window.partitionBy("city", "state", "country", "region", "market").orderBy("geography_id")

dim_geography_deduped = dim_geography \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .select("geography_id", "city", "state", "country", "region", "market")

print(f"   Unique geography combinations for join: {dim_geography_deduped.count():,}")

# Join on city, state, country, region, market (NO zipcode)
df_fact = df_fact.join(
    dim_geography_deduped,
    on=["city", "state", "country", "region", "market"],
    how="left"
)

print("    geography_id added")

# Joining with dim_shipping_mode

df_fact = df_fact.join(
    dim_shipping_mode.select("shipping_mode_id", "shipping_mode"),
    on=["shipping_mode"],
    how="left"
)

print("   shipping_mode_id added")

# Selecting final fact table columns

df_fact_final = df_fact.select(
    # Primary Keys
    "order_id",
    "order_item_id",
    
    # Foreign Keys
    "customer_id",
    "product_card_id",
    "geography_id",
    "shipping_mode_id",
    "order_date_id",
    "shipping_date_id",
    
    # Order Details
    "type",
    "order_status",
    "delivery_status",
    "late_delivery_risk",
    
    # Measures
    "order_item_quantity",
    "order_item_product_price",
    "order_item_discount",
    "order_item_discount_rate",
    "order_item_profit_ratio",
    "order_item_total",
    "sales",
    "sales_per_customer",
    "benefit_per_order",
    "order_profit_per_order",
    "days_for_shipping",
    "days_for_shipment",
    
    # Original dates
    col("order_date_parsed").alias("order_datetime"),
    col("shipping_date_parsed").alias("shipping_datetime")
)

print(f"   Total columns in fact table: {len(df_fact_final.columns)}")

# Running data quality checks

null_checks = df_fact_final.select(
    count(when(col("customer_id").isNull(), 1)).alias("null_customer_id"),
    count(when(col("product_card_id").isNull(), 1)).alias("null_product_card_id"),
    count(when(col("geography_id").isNull(), 1)).alias("null_geography_id"),
    count(when(col("shipping_mode_id").isNull(), 1)).alias("null_shipping_mode_id"),
    count(when(col("order_date_id").isNull(), 1)).alias("null_order_date_id")
).collect()[0]

print(f"\n    Null Foreign Keys Check:")
print(f"      customer_id: {null_checks['null_customer_id']}")
print(f"      product_card_id: {null_checks['null_product_card_id']}")
print(f"      geography_id: {null_checks['null_geography_id']}")
print(f"      shipping_mode_id: {null_checks['null_shipping_mode_id']}")
print(f"      order_date_id: {null_checks['null_order_date_id']}")

# Adding metadata columns

df_fact_final = df_fact_final \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_fact_final.columns)}")

# Writing to Silver Delta table

fact_orders_path = f"{silver_path}fact_orders/"

df_fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(fact_orders_path)

print(f" Data written to: {fact_orders_path}")

# Verifying Silver fact_orders table

df_verify = spark.read.format("delta").load(fact_orders_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.select(
    "order_id",
    "order_item_id",
    "customer_id",
    "product_card_id",
    "geography_id",
    "order_date_id",
    "sales",
    "order_profit_per_order"
).limit(10))

# Show aggregated stats
print("\n Fact Table Summary Statistics:")
df_verify.select(
    sum("sales").alias("total_sales"),
    sum("order_profit_per_order").alias("total_profit"),
    sum("order_item_quantity").alias("total_quantity"),
    count("*").alias("total_orders")
).show()
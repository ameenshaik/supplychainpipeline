# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

storage_account_name = "adlssupplychainproject"
storage_account_key = "<YOUR_STORAGE_ACCOUNT_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Reading Silver layer tables

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"

# Read all necessary tables
fact_orders = spark.read.format("delta").load(f"{silver_path}fact_orders/")
dim_geography = spark.read.format("delta").load(f"{silver_path}dim_geography/")
dim_shipping_mode = spark.read.format("delta").load(f"{silver_path}dim_shipping_mode/")
dim_date = spark.read.format("delta").load(f"{silver_path}dim_date/")
dim_customer = spark.read.format("delta").load(f"{silver_path}dim_customer/")

print(f"    All tables loaded")

# Joining fact table with dimensions

df_delivery = fact_orders

# Join with dim_date (order date)
df_delivery = df_delivery.join(
    dim_date.select(
        col("date_id").alias("order_date_id"),
        col("date").alias("order_date"),
        col("year").alias("order_year"),
        col("quarter").alias("order_quarter"),
        col("month").alias("order_month"),
        col("month_name").alias("order_month_name")
    ),
    on="order_date_id",
    how="left"
)

# Join with dim_date (shipping date)
df_delivery = df_delivery.join(
    dim_date.select(
        col("date_id").alias("shipping_date_id"),
        col("date").alias("shipping_date"),
        col("year").alias("shipping_year"),
        col("month").alias("shipping_month")
    ),
    on="shipping_date_id",
    how="left"
)

# Join with dim_geography
df_delivery = df_delivery.join(
    dim_geography.select(
        "geography_id",
        col("city").alias("order_city"),
        col("state").alias("order_state"),
        col("country").alias("order_country"),
        col("region").alias("order_region"),
        "market"
    ),
    on="geography_id",
    how="left"
)

# Join with dim_shipping_mode
df_delivery = df_delivery.join(
    dim_shipping_mode.select(
        "shipping_mode_id",
        "shipping_mode",
        col("days_for_shipment").alias("scheduled_shipping_days")
    ),
    on="shipping_mode_id",
    how="left"
)

# Join with dim_customer
df_delivery = df_delivery.join(
    dim_customer.select(
        "customer_id",
        "customer_segment"
    ),
    on="customer_id",
    how="left"
)

print("    All dimensions joined")

# Organizing delivery columns

df_gold_delivery = df_delivery.select(
    # Order Identification
    "order_id",
    "order_item_id",
    
    # Date Dimensions
    "order_date_id",
    "order_date",
    "order_year",
    "order_quarter",
    "order_month",
    "order_month_name",
    "shipping_date_id",
    "shipping_date",
    
    # Geography Dimensions
    "order_city",
    "order_state",
    "order_country",
    "order_region",
    "market",
    
    # Shipping Dimensions
    "shipping_mode",
    "scheduled_shipping_days",
    
    # Customer Dimension
    "customer_segment",
    
    # Delivery Status
    "order_status",
    "delivery_status",
    "late_delivery_risk",
    
    # Shipping Metrics
    "days_for_shipping",
    "days_for_shipment",
    
    # Financial Impact
    "sales",
    "order_profit_per_order"
)

# Adding calculated delivery metrics

df_gold_delivery = df_gold_delivery \
    .withColumn("is_late_delivery", col("late_delivery_risk").cast("integer")) \
    .withColumn("is_on_time", when(col("late_delivery_risk") == 0, 1).otherwise(0)) \
    .withColumn("shipping_delay_days", col("days_for_shipping") - col("scheduled_shipping_days")) \
    .withColumn("is_delayed", when(col("days_for_shipping") > col("scheduled_shipping_days"), 1).otherwise(0)) \
    .withColumn("delay_severity", 
                when(col("shipping_delay_days") <= 0, "On Time or Early")
                .when(col("shipping_delay_days").between(1, 2), "Slight Delay (1-2 days)")
                .when(col("shipping_delay_days").between(3, 5), "Moderate Delay (3-5 days)")
                .otherwise("Severe Delay (5+ days)")) \
    .withColumn("shipping_performance_score",
                when(col("is_late_delivery") == 0, 100)
                .otherwise(100 - (col("shipping_delay_days") * 10))) \
    .withColumn("order_to_ship_days", 
                datediff(col("shipping_date"), col("order_date"))) \
    .withColumn("is_same_day_ship", 
                when(col("order_to_ship_days") == 0, 1).otherwise(0))

print("    Calculated metrics added:")
print("      - is_late_delivery")
print("      - is_on_time")
print("      - shipping_delay_days")
print("      - is_delayed")
print("      - delay_severity")
print("      - shipping_performance_score")
print("      - order_to_ship_days")
print("      - is_same_day_ship")

# Adding metadata columns

df_gold_delivery_final = df_gold_delivery \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_gold_delivery_final.columns)}")

# Writing to Gold Delta table

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
gold_delivery_path = f"{gold_path}GoldFactDeliveryPerformance/"

df_gold_delivery_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_delivery_path)

print(f" Data written to: {gold_delivery_path}")

# Verifying GoldFactDeliveryPerformance table

df_verify = spark.read.format("delta").load(gold_delivery_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.select(
    "order_date",
    "shipping_mode",
    "market",
    "delivery_status",
    "days_for_shipping",
    "shipping_delay_days",
    "delay_severity"
).limit(10))

# DELIVERY PERFORMANCE INSIGHTS:

# Overall delivery performance
print("\n Overall Delivery Performance:")
df_verify.select(
    count("*").alias("total_orders"),
    sum("is_late_delivery").alias("late_deliveries"),
    sum("is_on_time").alias("on_time_deliveries"),
    avg("days_for_shipping").alias("avg_shipping_days"),
    avg("shipping_delay_days").alias("avg_delay_days"),
    avg("shipping_performance_score").alias("avg_performance_score")
).show()

# On-time delivery rate
print("\n On-Time Delivery Rate:")
on_time_rate = df_verify.agg(
    (sum("is_on_time") / count("*") * 100).alias("on_time_percentage")
).collect()[0]["on_time_percentage"]
print(f"   On-Time Delivery Rate: {on_time_rate:.2f}%")
print(f"   Late Delivery Rate: {100 - on_time_rate:.2f}%")

# Delay severity distribution
print("\n Delay Severity Distribution:")
display(df_verify.groupBy("delay_severity") \
    .agg(
        count("*").alias("order_count"),
        avg("shipping_delay_days").alias("avg_delay_days")
    ) \
    .orderBy("order_count", ascending=False))

# Performance by Shipping Mode
print("\n Performance by Shipping Mode:")
display(df_verify.groupBy("shipping_mode") \
    .agg(
        count("*").alias("total_orders"),
        sum("is_late_delivery").alias("late_deliveries"),
        (sum("is_on_time") / count("*") * 100).alias("on_time_rate_pct"),
        avg("days_for_shipping").alias("avg_shipping_days"),
        avg("shipping_delay_days").alias("avg_delay_days")
    ) \
    .orderBy("total_orders", ascending=False))

# Performance by Market
print("\n Performance by Market:")
display(df_verify.groupBy("market") \
    .agg(
        count("*").alias("total_orders"),
        (sum("is_on_time") / count("*") * 100).alias("on_time_rate_pct"),
        avg("days_for_shipping").alias("avg_shipping_days"),
        avg("shipping_delay_days").alias("avg_delay_days")
    ) \
    .orderBy("on_time_rate_pct", ascending=False))

# Performance by Customer Segment
print("\n Performance by Customer Segment:")
display(df_verify.groupBy("customer_segment") \
    .agg(
        count("*").alias("total_orders"),
        (sum("is_on_time") / count("*") * 100).alias("on_time_rate_pct"),
        avg("days_for_shipping").alias("avg_shipping_days")
    ) \
    .orderBy("total_orders", ascending=False))

# Monthly trend
print("\n Monthly Delivery Performance Trend:")
display(df_verify.groupBy("order_year", "order_month", "order_month_name") \
    .agg(
        count("*").alias("total_orders"),
        (sum("is_on_time") / count("*") * 100).alias("on_time_rate_pct"),
        avg("days_for_shipping").alias("avg_shipping_days")
    ) \
    .orderBy("order_year", "order_month"))

# Worst performing regions
print("\n Top 10 Worst Performing Regions:")
display(df_verify.groupBy("order_country", "order_region") \
    .agg(
        count("*").alias("total_orders"),
        sum("is_late_delivery").alias("late_deliveries"),
        (sum("is_on_time") / count("*") * 100).alias("on_time_rate_pct")
    ) \
    .filter(col("total_orders") >= 100) \
    .orderBy("on_time_rate_pct", ascending=True) \
    .limit(10))
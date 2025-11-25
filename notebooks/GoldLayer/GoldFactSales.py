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

# Read all tables
fact_orders = spark.read.format("delta").load(f"{silver_path}fact_orders/")
dim_customer = spark.read.format("delta").load(f"{silver_path}dim_customer/")
dim_product = spark.read.format("delta").load(f"{silver_path}dim_product/")
dim_geography = spark.read.format("delta").load(f"{silver_path}dim_geography/")
dim_shipping_mode = spark.read.format("delta").load(f"{silver_path}dim_shipping_mode/")
dim_date = spark.read.format("delta").load(f"{silver_path}dim_date/")

print(f"    fact_orders: {fact_orders.count():,} records")
print(f"    dim_customer: {dim_customer.count():,} records")
print(f"    dim_product: {dim_product.count():,} records")
print(f"    dim_geography: {dim_geography.count():,} records")
print(f"    dim_shipping_mode: {dim_shipping_mode.count():,} records")
print(f"    dim_date: {dim_date.count():,} records")

# Joining fact table with all dimensions

# Start with fact table
df_sales = fact_orders

# Join with dim_date (order date)
df_sales = df_sales.join(
    dim_date.select(
        col("date_id").alias("order_date_id"),
        col("date").alias("order_date"),
        col("year").alias("order_year"),
        col("quarter").alias("order_quarter"),
        col("month").alias("order_month"),
        col("month_name").alias("order_month_name"),
        col("day_of_week_name").alias("order_day_of_week")
    ),
    on="order_date_id",
    how="left"
)

# Join with dim_customer
df_sales = df_sales.join(
    dim_customer.select(
        "customer_id",
        "customer_segment",
        col("customer_city").alias("customer_city"),
        col("customer_state").alias("customer_state"),
        col("customer_country").alias("customer_country")
    ),
    on="customer_id",
    how="left"
)

# Join with dim_product
df_sales = df_sales.join(
    dim_product.select(
        "product_card_id",
        "product_name",
        "category_name",
        "department_name",
        "product_price"
    ),
    on="product_card_id",
    how="left"
)

# Join with dim_geography
df_sales = df_sales.join(
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
df_sales = df_sales.join(
    dim_shipping_mode.select(
        "shipping_mode_id",
        "shipping_mode"
    ),
    on="shipping_mode_id",
    how="left"
)

print("    All dimensions joined")
print(f"   Total records after joins: {df_sales.count():,}")

# Organizing columns for Gold layer

df_gold_sales = df_sales.select(
    # Date Dimensions
    "order_date_id",
    "order_date",
    "order_year",
    "order_quarter", 
    "order_month",
    "order_month_name",
    "order_day_of_week",
    
    # Product Dimensions
    "product_card_id",
    "product_name",
    "category_name",
    "department_name",
    
    # Customer Dimensions
    "customer_id",
    "customer_segment",
    "customer_city",
    "customer_state",
    "customer_country",
    
    # Geography Dimensions
    "order_city",
    "order_state",
    "order_country",
    "order_region",
    "market",
    
    # Shipping Dimensions
    "shipping_mode",
    
    # Order Details
    "order_id",
    "order_status",
    "delivery_status",
    "late_delivery_risk",
    "type",
    
    # Metrics/Measures
    "sales",
    "order_profit_per_order",
    "benefit_per_order",
    "order_item_quantity",
    "order_item_discount",
    "order_item_discount_rate",
    "days_for_shipping",
    "days_for_shipment"
)

# Adding calculated metrics

df_gold_sales = df_gold_sales \
    .withColumn("profit_margin_pct", 
                when(col("sales") > 0, (col("order_profit_per_order") / col("sales")) * 100)
                .otherwise(0)) \
    .withColumn("discount_amount", col("sales") * col("order_item_discount_rate")) \
    .withColumn("revenue_after_discount", col("sales") - col("discount_amount")) \
    .withColumn("shipping_delay_days", col("days_for_shipping") - col("days_for_shipment")) \
    .withColumn("is_late_delivery", col("late_delivery_risk").cast("integer")) \
    .withColumn("is_profitable", when(col("order_profit_per_order") > 0, 1).otherwise(0))

print("    Calculated metrics added:")
print("      - profit_margin_pct")
print("      - discount_amount")
print("      - revenue_after_discount")
print("      - shipping_delay_days")
print("      - is_late_delivery")
print("      - is_profitable")

# Adding metadata columns

df_gold_sales_final = df_gold_sales \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_gold_sales_final.columns)}")

# Writing to Gold Delta table

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
gold_fact_sales_path = f"{gold_path}GoldFactSales/"

df_gold_sales_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_fact_sales_path)

print(f" Data written to: {gold_fact_sales_path}")

# Verifying GoldFactSales table

df_verify = spark.read.format("delta").load(gold_fact_sales_path)

print(f"   Total records: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.select(
    "order_date",
    "category_name",
    "customer_segment",
    "market",
    "sales",
    "order_profit_per_order",
    "profit_margin_pct"
).limit(10))

# BUSINESS INSIGHTS:

# Total metrics
print("\n Overall Performance:")
df_verify.select(
    sum("sales").alias("total_sales"),
    sum("order_profit_per_order").alias("total_profit"),
    avg("profit_margin_pct").alias("avg_profit_margin_pct"),
    sum("order_item_quantity").alias("total_quantity"),
    count("*").alias("total_orders")
).show()

# Sales by Category
print("\n Sales by Category:")
display(df_verify.groupBy("category_name") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("order_profit_per_order").alias("total_profit"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_sales").desc()) \
    .limit(10))

# Sales by Market
print("\n Sales by Market:")
display(df_verify.groupBy("market") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("order_profit_per_order").alias("total_profit"),
        avg("profit_margin_pct").alias("avg_profit_margin")
    ) \
    .orderBy(col("total_sales").desc()))

# Sales by Year and Quarter
print("\n Sales Trend by Year-Quarter:")
display(df_verify.groupBy("order_year", "order_quarter") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("order_profit_per_order").alias("total_profit"),
        count("*").alias("order_count")
    ) \
    .orderBy("order_year", "order_quarter"))

# Delivery Performance
print("\n Delivery Performance:")
df_verify.groupBy("is_late_delivery") \
    .agg(
        count("*").alias("order_count"),
        avg("days_for_shipping").alias("avg_shipping_days")
    ) \
    .withColumn("delivery_status", when(col("is_late_delivery") == 1, "Late").otherwise("On Time")) \
    .select("delivery_status", "order_count", "avg_shipping_days") \
    .show()
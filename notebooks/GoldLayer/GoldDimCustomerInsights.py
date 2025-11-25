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

fact_orders = spark.read.format("delta").load(f"{silver_path}fact_orders/")
dim_customer = spark.read.format("delta").load(f"{silver_path}dim_customer/")
dim_date = spark.read.format("delta").load(f"{silver_path}dim_date/")
dim_geography = spark.read.format("delta").load(f"{silver_path}dim_geography/")

print(f"    All tables loaded")

# Joining fact table with dimensions

df_customer = fact_orders.join(
    dim_customer.select(
        "customer_id",
        "customer_fname",
        "customer_lname",
        "customer_email",
        "customer_segment",
        "customer_city",
        "customer_state",
        "customer_country"
    ),
    on="customer_id",
    how="left"
)

# Join with date
df_customer = df_customer.join(
    dim_date.select(
        col("date_id").alias("order_date_id"),
        col("date").alias("order_date")
    ),
    on="order_date_id",
    how="left"
)

# Join with geography for market info
df_customer = df_customer.join(
    dim_geography.select(
        "geography_id",
        "market"
    ),
    on="geography_id",
    how="left"
)

print("    All dimensions joined")

# Calculating customer metrics

df_customer_agg = df_customer.groupBy(
    "customer_id",
    "customer_fname",
    "customer_lname",
    "customer_email",
    "customer_segment",
    "customer_city",
    "customer_state",
    "customer_country"
).agg(
    # Order Metrics
    count("order_id").alias("total_orders"),
    countDistinct("order_id").alias("unique_orders"),
    
    # Financial Metrics
    sum("sales").alias("total_sales"),
    sum("order_profit_per_order").alias("total_profit"),
    avg("sales").alias("avg_order_value"),
    avg("order_profit_per_order").alias("avg_profit_per_order"),
    
    # Product Metrics
    sum("order_item_quantity").alias("total_items_purchased"),
    avg("order_item_quantity").alias("avg_items_per_order"),
    countDistinct("product_card_id").alias("unique_products_purchased"),
    
    # Discount Metrics
    avg("order_item_discount_rate").alias("avg_discount_rate"),
    sum("order_item_discount").alias("total_discount_received"),
    
    # Delivery Metrics
    sum("late_delivery_risk").alias("late_deliveries"),
    avg("days_for_shipping").alias("avg_shipping_days"),
    
    # Date Metrics (RFM - Recency, Frequency, Monetary)
    min("order_date").alias("first_order_date"),
    max("order_date").alias("last_order_date"),
    
    # Market
    first("market").alias("primary_market")
)

print("    Aggregations completed")

# Adding calculated customer insights

# Get current date for recency calculation
from datetime import datetime
current_date = datetime.now()

df_gold_customer = df_customer_agg \
    .withColumn("customer_lifetime_value", col("total_sales")) \
    .withColumn("profit_margin_pct",
                when(col("total_sales") > 0, (col("total_profit") / col("total_sales")) * 100)
                .otherwise(0)) \
    .withColumn("customer_tenure_days",
                datediff(col("last_order_date"), col("first_order_date"))) \
    .withColumn("days_since_last_order",
                datediff(lit(current_date), col("last_order_date"))) \
    .withColumn("avg_days_between_orders",
                when(col("unique_orders") > 1, 
                     col("customer_tenure_days") / (col("unique_orders") - 1))
                .otherwise(0)) \
    .withColumn("is_repeat_customer",
                when(col("unique_orders") > 1, 1).otherwise(0)) \
    .withColumn("is_profitable_customer",
                when(col("total_profit") > 0, 1).otherwise(0)) \
    .withColumn("late_delivery_rate_pct",
                when(col("total_orders") > 0, (col("late_deliveries") / col("total_orders")) * 100)
                .otherwise(0)) \
    .withColumn("discount_dependency_pct",
                when(col("total_sales") > 0, (col("total_discount_received") / col("total_sales")) * 100)
                .otherwise(0))

print("    Basic metrics calculated")

# Adding RFM Analysis

from pyspark.sql.window import Window

# Calculate quartiles for RFM scoring
recency_window = Window.orderBy(col("days_since_last_order"))
frequency_window = Window.orderBy(col("total_orders").desc())
monetary_window = Window.orderBy(col("total_sales").desc())

df_gold_customer = df_gold_customer \
    .withColumn("recency_score", 
                ntile(4).over(recency_window)) \
    .withColumn("frequency_score",
                ntile(4).over(frequency_window)) \
    .withColumn("monetary_score",
                ntile(4).over(monetary_window)) \
    .withColumn("rfm_score",
                col("recency_score") + col("frequency_score") + col("monetary_score")) \
    .withColumn("rfm_segment",
                when(col("rfm_score") >= 10, "Champions")
                .when(col("rfm_score").between(8, 9), "Loyal Customers")
                .when(col("rfm_score").between(6, 7), "Potential Loyalists")
                .when(col("rfm_score").between(5, 5), "Recent Customers")
                .when(col("rfm_score").between(4, 4), "Promising")
                .when(col("rfm_score").between(3, 3), "Customers Needing Attention")
                .otherwise("At Risk"))

print("    RFM scores calculated")

# Adding customer value segmentation

df_gold_customer = df_gold_customer \
    .withColumn("value_segment",
                when(col("customer_lifetime_value") >= 1000, "High Value")
                .when(col("customer_lifetime_value").between(500, 999), "Medium Value")
                .when(col("customer_lifetime_value").between(100, 499), "Low Value")
                .otherwise("Very Low Value")) \
    .withColumn("purchase_frequency_category",
                when(col("total_orders") >= 20, "Very Frequent")
                .when(col("total_orders").between(10, 19), "Frequent")
                .when(col("total_orders").between(5, 9), "Occasional")
                .otherwise("Rare")) \
    .withColumn("customer_health_status",
                when((col("days_since_last_order") <= 90) & (col("total_orders") >= 5), "Healthy")
                .when((col("days_since_last_order") <= 180) & (col("total_orders") >= 3), "Active")
                .when(col("days_since_last_order") <= 365, "At Risk")
                .otherwise("Churned"))

print("    Value segmentation completed")

# Adding customer rankings

# Overall rankings
window_sales = Window.orderBy(col("total_sales").desc())
window_orders = Window.orderBy(col("total_orders").desc())
window_profit = Window.orderBy(col("total_profit").desc())

df_gold_customer = df_gold_customer \
    .withColumn("sales_rank", row_number().over(window_sales)) \
    .withColumn("orders_rank", row_number().over(window_orders)) \
    .withColumn("profit_rank", row_number().over(window_profit))

# Rankings by segment
window_segment = Window.partitionBy("customer_segment").orderBy(col("total_sales").desc())
df_gold_customer = df_gold_customer \
    .withColumn("rank_in_segment", row_number().over(window_segment))

print("    Rankings added")

# Adding metadata columns

df_gold_customer_final = df_gold_customer \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_gold_customer_final.columns)}")

# Writing to Gold Delta table

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
gold_customer_path = f"{gold_path}GoldDimCustomerInsights/"

df_gold_customer_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_customer_path)

print(f" Data written to: {gold_customer_path}")

# Verifying GoldDimCustomerInsights table

df_verify = spark.read.format("delta").load(gold_customer_path)

print(f"   Total customers: {df_verify.count():,}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n Sample data:")
display(df_verify.select(
    "customer_id",
    "customer_fname",
    "customer_lname",
    "customer_segment",
    "total_orders",
    "customer_lifetime_value",
    "rfm_segment",
    "value_segment",
    "customer_health_status"
).limit(10))

# CUSTOMER INSIGHTS ANALYSIS:

# Overall customer metrics
print("\n Overall Customer Metrics:")
df_verify.select(
    count("*").alias("total_customers"),
    sum("customer_lifetime_value").alias("total_clv"),
    avg("customer_lifetime_value").alias("avg_clv"),
    avg("total_orders").alias("avg_orders_per_customer"),
    sum("is_repeat_customer").alias("repeat_customers")
).show()

# RFM Segment Distribution
print("\n RFM Segment Distribution:")
display(df_verify.groupBy("rfm_segment") \
    .agg(
        count("*").alias("customer_count"),
        sum("customer_lifetime_value").alias("total_clv"),
        avg("total_orders").alias("avg_orders")
    ) \
    .orderBy("total_clv", ascending=False))

# Value Segment Distribution
print("\n Value Segment Distribution:")
display(df_verify.groupBy("value_segment") \
    .agg(
        count("*").alias("customer_count"),
        sum("customer_lifetime_value").alias("total_clv"),
        avg("customer_lifetime_value").alias("avg_clv")
    ) \
    .orderBy("total_clv", ascending=False))

# Customer Health Status
print("\n Customer Health Status:")
display(df_verify.groupBy("customer_health_status") \
    .agg(
        count("*").alias("customer_count"),
        avg("days_since_last_order").alias("avg_days_since_last_order"),
        sum("customer_lifetime_value").alias("total_clv")
    ) \
    .orderBy("customer_count", ascending=False))

# Top 10 Customers by CLV
print("\n Top 10 Customers by Lifetime Value:")
display(df_verify.select(
    "sales_rank",
    "customer_fname",
    "customer_lname",
    "customer_segment",
    "customer_lifetime_value",
    "total_orders",
    "rfm_segment",
    "customer_health_status"
).orderBy("sales_rank").limit(10))

# Customer Segment Performance
print("\n Performance by Customer Segment:")
display(df_verify.groupBy("customer_segment") \
    .agg(
        count("*").alias("customer_count"),
        sum("customer_lifetime_value").alias("total_clv"),
        avg("customer_lifetime_value").alias("avg_clv"),
        avg("total_orders").alias("avg_orders"),
        avg("profit_margin_pct").alias("avg_profit_margin")
    ) \
    .orderBy("total_clv", ascending=False))

# Repeat vs One-time Customers
print("\n Repeat vs One-Time Customers:")
df_verify.groupBy("is_repeat_customer") \
    .agg(
        count("*").alias("customer_count"),
        sum("customer_lifetime_value").alias("total_clv"),
        avg("total_orders").alias("avg_orders")
    ) \
    .withColumn("customer_type", when(col("is_repeat_customer") == 1, "Repeat").otherwise("One-Time")) \
    .select("customer_type", "customer_count", "total_clv", "avg_orders") \
    .show()

# Purchase Frequency Analysis
print("\n Purchase Frequency Analysis:")
display(df_verify.groupBy("purchase_frequency_category") \
    .agg(
        count("*").alias("customer_count"),
        avg("total_orders").alias("avg_orders"),
        sum("customer_lifetime_value").alias("total_clv")
    ) \
    .orderBy("total_clv", ascending=False))

# Geographic Performance
print("\n Top 10 Countries by Customer Value:")
display(df_verify.groupBy("customer_country") \
    .agg(
        count("*").alias("customer_count"),
        sum("customer_lifetime_value").alias("total_clv"),
        avg("customer_lifetime_value").alias("avg_clv")
    ) \
    .orderBy("total_clv", ascending=False) \
    .limit(10))

# At-Risk and Churned Customers
print("\n At-Risk and Churned Customers:")
at_risk_count = df_verify.filter(col("customer_health_status").isin(["At Risk", "Churned"])).count()
total_count = df_verify.count()
at_risk_pct = (at_risk_count / total_count) * 100
print(f"   At-Risk/Churned Customers: {at_risk_count:,} ({at_risk_pct:.2f}%)")

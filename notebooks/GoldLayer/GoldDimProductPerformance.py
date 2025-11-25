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
dim_product = spark.read.format("delta").load(f"{silver_path}dim_product/")
dim_date = spark.read.format("delta").load(f"{silver_path}dim_date/")

print(f"    All tables loaded")

# Joining fact table with product dimension

df_product_perf = fact_orders.join(
    dim_product.select(
        "product_card_id",
        "product_name",
        "category_name",
        "department_name",
        "product_price",
        "product_status_desc"
    ),
    on="product_card_id",
    how="left"
)

# Join with date for time-based analysis
df_product_perf = df_product_perf.join(
    dim_date.select(
        col("date_id").alias("order_date_id"),
        col("year").alias("order_year"),
        col("quarter").alias("order_quarter"),
        col("month").alias("order_month")
    ),
    on="order_date_id",
    how="left"
)

print("    All dimensions joined")

# Calculating product performance metrics

df_product_agg = df_product_perf.groupBy(
    "product_card_id",
    "product_name",
    "category_name",
    "department_name",
    "product_price",
    "product_status_desc"
).agg(
    # Sales Metrics
    sum("sales").alias("total_sales"),
    sum("order_profit_per_order").alias("total_profit"),
    sum("order_item_quantity").alias("total_quantity_sold"),
    count("order_id").alias("total_orders"),
    
    # Average Metrics
    avg("sales").alias("avg_sales_per_order"),
    avg("order_profit_per_order").alias("avg_profit_per_order"),
    avg("order_item_quantity").alias("avg_quantity_per_order"),
    
    # Discount Metrics
    avg("order_item_discount_rate").alias("avg_discount_rate"),
    sum("order_item_discount").alias("total_discount_given"),
    
    # Date Range
    min("order_datetime").alias("first_order_date"),
    max("order_datetime").alias("last_order_date")
)

print("    Aggregations completed")

# Adding calculated performance metrics

df_gold_product = df_product_agg \
    .withColumn("profit_margin_pct", 
                when(col("total_sales") > 0, (col("total_profit") / col("total_sales")) * 100)
                .otherwise(0)) \
    .withColumn("revenue_per_unit", 
                when(col("total_quantity_sold") > 0, col("total_sales") / col("total_quantity_sold"))
                .otherwise(0)) \
    .withColumn("profit_per_unit",
                when(col("total_quantity_sold") > 0, col("total_profit") / col("total_quantity_sold"))
                .otherwise(0)) \
    .withColumn("is_profitable",
                when(col("total_profit") > 0, 1).otherwise(0)) \
    .withColumn("profitability_rating",
                when(col("profit_margin_pct") >= 20, "High")
                .when(col("profit_margin_pct").between(10, 20), "Medium")
                .when(col("profit_margin_pct").between(0, 10), "Low")
                .otherwise("Unprofitable")) \
    .withColumn("sales_volume_category",
                when(col("total_quantity_sold") >= 1000, "High Volume")
                .when(col("total_quantity_sold").between(100, 999), "Medium Volume")
                .otherwise("Low Volume")) \
    .withColumn("discount_impact_pct",
                when(col("total_sales") > 0, (col("total_discount_given") / col("total_sales")) * 100)
                .otherwise(0)) \
    .withColumn("days_in_catalog",
                datediff(col("last_order_date"), col("first_order_date"))) \
    .withColumn("avg_daily_sales",
                when(col("days_in_catalog") > 0, col("total_sales") / col("days_in_catalog"))
                .otherwise(0))

print("    Calculated metrics added:")
print("      - profit_margin_pct")
print("      - revenue_per_unit")
print("      - profit_per_unit")
print("      - is_profitable")
print("      - profitability_rating")
print("      - sales_volume_category")
print("      - discount_impact_pct")
print("      - days_in_catalog")
print("      - avg_daily_sales")

# Adding product rankings

from pyspark.sql.window import Window

# Overall rankings
window_overall = Window.orderBy(col("total_sales").desc())
window_profit = Window.orderBy(col("total_profit").desc())
window_quantity = Window.orderBy(col("total_quantity_sold").desc())

df_gold_product = df_gold_product \
    .withColumn("sales_rank", row_number().over(window_overall)) \
    .withColumn("profit_rank", row_number().over(window_profit)) \
    .withColumn("quantity_rank", row_number().over(window_quantity))

# Category rankings
window_category = Window.partitionBy("category_name").orderBy(col("total_sales").desc())
df_gold_product = df_gold_product \
    .withColumn("rank_in_category", row_number().over(window_category))

print("    Rankings added")

# Adding metadata columns

df_gold_product_final = df_gold_product \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

print(f"   Total columns: {len(df_gold_product_final.columns)}")

# Writing to Gold Delta table

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
gold_product_path = f"{gold_path}GoldDimProductPerformance/"

df_gold_product_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_product_path)

print(f" Data written to: {gold_product_path}")

# Verifying GoldDimProductPerformance table

df_verify = spark.read.format("delta").load(gold_product_path)

print(f"   Total products: {df_verify.count()}")
print(f"   Total columns: {len(df_verify.columns)}")

# Show sample
print("\n👀 Sample data:")
display(df_verify.select(
    "product_name",
    "category_name",
    "total_sales",
    "total_profit",
    "profit_margin_pct",
    "total_quantity_sold",
    "profitability_rating"
).limit(10))

# PRODUCT PERFORMANCE INSIGHTS:

# Overall product metrics
print("\n Overall Product Performance:")
df_verify.select(
    count("*").alias("total_products"),
    sum("total_sales").alias("total_revenue"),
    sum("total_profit").alias("total_profit"),
    avg("profit_margin_pct").alias("avg_profit_margin"),
    sum("total_quantity_sold").alias("total_units_sold")
).show()

# Profitability distribution
print("\n Product Profitability Distribution:")
display(df_verify.groupBy("profitability_rating") \
    .agg(
        count("*").alias("product_count"),
        sum("total_sales").alias("total_sales"),
        sum("total_profit").alias("total_profit")
    ) \
    .orderBy("total_sales", ascending=False))

# Top 10 products by sales
print("\n Top 10 Products by Sales:")
display(df_verify.select(
    "sales_rank",
    "product_name",
    "category_name",
    "total_sales",
    "total_profit",
    "profit_margin_pct",
    "total_quantity_sold"
).orderBy("sales_rank").limit(10))

# Bottom 10 products by profit
print("\n Bottom 10 Products by Profit (Loss Makers):")
display(df_verify.select(
    "product_name",
    "category_name",
    "total_sales",
    "total_profit",
    "profit_margin_pct",
    "total_orders"
).orderBy("total_profit", ascending=True).limit(10))

# Category performance
print("\n Performance by Category:")
display(df_verify.groupBy("category_name") \
    .agg(
        count("*").alias("product_count"),
        sum("total_sales").alias("total_sales"),
        sum("total_profit").alias("total_profit"),
        avg("profit_margin_pct").alias("avg_profit_margin"),
        sum("total_quantity_sold").alias("total_quantity")
    ) \
    .orderBy("total_sales", ascending=False))

# Department performance
print("\n Performance by Department:")
display(df_verify.groupBy("department_name") \
    .agg(
        count("*").alias("product_count"),
        sum("total_sales").alias("total_sales"),
        sum("total_profit").alias("total_profit"),
        avg("profit_margin_pct").alias("avg_profit_margin")
    ) \
    .orderBy("total_sales", ascending=False))

# High vs Low performers
print("\n High Performers vs Low Performers:")
df_verify.groupBy("sales_volume_category", "profitability_rating") \
    .agg(
        count("*").alias("product_count"),
        sum("total_sales").alias("total_sales")
    ) \
    .orderBy("total_sales", ascending=False) \
    .show()

# Discount impact analysis
print("\n Discount Impact Analysis:")
display(df_verify.select(
    "product_name",
    "category_name",
    "total_sales",
    "total_discount_given",
    "discount_impact_pct",
    "profit_margin_pct"
).orderBy("discount_impact_pct", ascending=False).limit(10))

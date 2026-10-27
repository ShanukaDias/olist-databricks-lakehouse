# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import avg, col, count, countDistinct, sum, to_date

# Read from Silver table
df = spark.read.table("ecommerce_db.default.silver_ecommerce_transactions")

# 1. Gold Daily Sales Summary Table
gold_daily = (
    df.withColumn("purchase_date", to_date(col("order_purchase_timestamp")))
    .groupBy("purchase_date")
    .agg(
        sum("total_item_value").alias("daily_total_revenue"),
        sum("price").alias("daily_product_revenue"),
        sum("freight_value").alias("daily_freight_revenue"),
        countDistinct("order_id").alias("total_orders"),
        count("product_id").alias("total_items_sold"),
        avg("total_item_value").alias("average_order_value"),
    )
    .orderBy(col("purchase_date").desc())
)

gold_daily.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.gold_daily_sales_summary"
)

# 2. Gold Sales by Customer State Table
gold_state = (
    df.groupBy("customer_state")
    .agg(
        sum("total_item_value").alias("total_state_revenue"),
        countDistinct("order_id").alias("state_total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy(col("total_state_revenue").desc())
)

gold_state.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.gold_sales_by_state"
)

print("Gold aggregation tables created successfully!")

# Preview the Daily Sales Summary
display(
    spark.sql(
        "SELECT * FROM ecommerce_db.default.gold_daily_sales_summary LIMIT 10"
    )
)
# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp

# Read from Bronze Delta tables
orders = spark.read.table("ecommerce_db.default.bronze_orders")
items = spark.read.table("ecommerce_db.default.bronze_order_items")
customers = spark.read.table("ecommerce_db.default.bronze_customers")

# Clean, transform, and join relational tables
silver_df = (
    items.join(orders, "order_id", "inner")
    .join(customers, "customer_id", "inner")
    .filter(
        col("order_status") == "delivered"
    )  # Data Quality Filter: Only analyze successfully delivered orders
    .dropna(subset=["order_id", "product_id", "price", "customer_id"])
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp")),
    )
    .withColumn("price", col("price").cast("double"))
    .withColumn("freight_value", col("freight_value").cast("double"))
    .withColumn("total_item_value", col("price") + col("freight_value"))
    .select(
        "order_id",
        "product_id",
        "customer_id",
        "customer_state",
        "customer_city",
        "order_status",
        "order_purchase_timestamp",
        "price",
        "freight_value",
        "total_item_value",
    )
)

# Write to Silver Delta Table using overwrite mode
silver_df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.silver_ecommerce_transactions"
)

print(
    "Silver transformation complete! Table saved as"
    " ecommerce_db.default.silver_ecommerce_transactions"
)

# Preview the clean data
display(
    spark.sql(
        "SELECT * FROM ecommerce_db.default.silver_ecommerce_transactions LIMIT"
        " 10"
    )
)
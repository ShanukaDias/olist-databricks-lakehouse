# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Define raw data volume path
raw_path = "/Volumes/ecommerce_db/default/raw_data/"

# 1. Ingest Orders Bronze Table
orders_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path + "olist_orders_dataset.csv")
    .withColumn("ingestion_time", current_timestamp())
)

orders_df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.bronze_orders"
)

# 2. Ingest Order Items Bronze Table
items_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path + "olist_order_items_dataset.csv")
    .withColumn("ingestion_time", current_timestamp())
)

items_df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.bronze_order_items"
)

# 3. Ingest Customers Bronze Table
customers_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path + "olist_customers_dataset.csv")
    .withColumn("ingestion_time", current_timestamp())
)

customers_df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.bronze_customers"
)

# 4. Ingest Category Translations Bronze Table
categories_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path + "product_category_name_translation.csv")
    .withColumn("ingestion_time", current_timestamp())
)

categories_df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce_db.default.bronze_category_translation"
)

print("All Bronze tables ingested successfully!")
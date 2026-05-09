# Databricks notebook source
# -------------------------------------------
# 01 - Ingestion Notebook (Automobile Dataset)
# -------------------------------------------

from pyspark.sql.functions import current_timestamp, lit

# File path (adjust if needed)
raw_path = "abfss://automotive@datalake18vicky.dfs.core.windows.net/raw/automobile_data.csv"
bronze_path = "abfss://automotive@datalake18vicky.dfs.core.windows.net/bronze/automobile_data"

# Read raw CSV
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

# Add metadata columns
df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("automobile_data.csv"))
)

# Write to Delta (Bronze Layer)
df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

# Display sample
display(df_bronze)

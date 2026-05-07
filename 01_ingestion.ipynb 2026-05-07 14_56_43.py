# Databricks notebook source
# 01 Ingestion notebook
from pyspark.sql.functions import current_ timestamp, lit

# file path
raw_path = "/data/raw/automobile_dataset.csv"
bronze_path = "/data/bronze/automobile_dataset"

# read raw data
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path)

# add ingestion date
df_bronze =(
    df_raw.withColumn("ingestion_date", current_timestamp())
    .withcolumn("source_file", lit("automobile_dataset.csv"))
)


# write data to delta
df_bronze.write.mode("overwrite").format("delta").save(bronze_path)
 
 display(df_bronze)

# 02 Bronze to Silver notebook
from pyspark.sql.functions import col, lit

# file path
bronze_path = "/data/bronze/automobile_dataset"
silver_path = "/data/silver/automobile_dataset"

# read bronze data
df = spark.read.format("delta").load(bronze_path)

# add ingestion date
df_silver = df.withColumn("ingestion_date", current_timestamp())

# write data
df.write.mode("overwrite").format("delta").saveAsTable("automobile_data")
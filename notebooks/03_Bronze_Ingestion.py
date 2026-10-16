# Databricks notebook source
from pathlib import Path

source_path = Path(
    "/Volumes/workspace/next_third_party_landing/source_files/olist"
)

csv_files = sorted(source_path.glob("*.csv"))

print(f"{len(csv_files)} CSV files found")

for file in csv_files:
    print(file.name)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating bronze schema tables

# COMMAND ----------

bronze_schema = "workspace.next_third_party_bronze"

for file in sorted(source_path.glob("*.csv")):
    table_name = file.stem.replace("_dataset", "")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(file))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{bronze_schema}.{table_name}")
    )

    print(f"Created: {bronze_schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.next_third_party_bronze;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

reviews_path = (
    "/Volumes/workspace/"
    "next_third_party_landing/source_files/olist/"
    "olist_order_reviews_dataset.csv"
)

reviews_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True)
])

reviews_fixed = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .schema(reviews_schema)
    .csv(reviews_path)
)

reviews_fixed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        "workspace.next_third_party_bronze.olist_order_reviews"
    )
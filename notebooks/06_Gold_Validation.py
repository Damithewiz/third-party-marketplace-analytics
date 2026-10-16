# Databricks notebook source
# MAGIC %md
# MAGIC This notebook validates the Gold layer after all dimensions and fact tables have been created.

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

gold_schema = "workspace.next_third_party_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC List of gold tables

# COMMAND ----------

gold_table_list = spark.sql(
    f"SHOW TABLES IN {gold_schema}"
)
display(gold_table_list)

# COMMAND ----------

expected_gold_tables = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_seller",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_reviews"
]

actual_gold_tables = [
    row.tableName
    for row in gold_table_list.collect()
]

missing_gold_tables = [
    table
    for table in expected_gold_tables
    if table not in actual_gold_tables
]

unexpected_gold_tables = [
    table
    for table in actual_gold_tables
    if table not in expected_gold_tables
]

print("Missing expected tables:", missing_gold_tables)
print("Unexpected Gold tables:", unexpected_gold_tables)

assert len(missing_gold_tables) == 0, (
    f"Missing Gold tables: {missing_gold_tables}"
)

print("Gold table availability validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Load Gold Tables

# COMMAND ----------

dim_date = spark.table(
    f"{gold_schema}.dim_date"
)

dim_customer = spark.table(
    f"{gold_schema}.dim_customer"
)

dim_product = spark.table(
    f"{gold_schema}.dim_product"
)

dim_seller = spark.table(
    f"{gold_schema}.dim_seller"
)

fact_orders = spark.table(
    f"{gold_schema}.fact_orders"
)

fact_order_items = spark.table(
    f"{gold_schema}.fact_order_items"
)

fact_payments = spark.table(
    f"{gold_schema}.fact_payments"
)

fact_reviews = spark.table(
    f"{gold_schema}.fact_reviews"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Row Counts

# COMMAND ----------

gold_tables = {
    "dim_date": dim_date,
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_seller": dim_seller,
    "fact_orders": fact_orders,
    "fact_order_items": fact_order_items,
    "fact_payments": fact_payments,
    "fact_reviews": fact_reviews
}

row_count_results = []

for table_name, dataframe in gold_tables.items():
    row_count_results.append(
        (
            table_name,
            dataframe.count()
        )
    )

row_count_df = spark.createDataFrame(
    row_count_results,
    [
        "table_name",
        "row_count"
    ]
)

display(
    row_count_df.orderBy("table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Review

# COMMAND ----------

for table_name, dataframe in gold_tables.items():
    print(f"\n{'=' * 70}")
    print(table_name)
    print("=" * 70)

    dataframe.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Table Reviews

# COMMAND ----------

for table_name, dataframe in gold_tables.items():
    print(f"\nPreview: {table_name}")

    display(
        dataframe.limit(10)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate Dimension Keys Check

# COMMAND ----------

dimension_key_checks = {
    "dim_date": (
        dim_date,
        ["date_key"]
    ),
    "dim_customer": (
        dim_customer,
        ["customer_id"]
    ),
    "dim_product": (
        dim_product,
        ["product_id"]
    ),
    "dim_seller": (
        dim_seller,
        ["seller_id"]
    )
}

duplicate_key_results = []

for table_name, configuration in dimension_key_checks.items():
    dataframe, key_columns = configuration

    duplicate_count = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_key_results.append(
        (
            table_name,
            ", ".join(key_columns),
            duplicate_count,
            duplicate_count == 0
        )
    )

duplicate_key_results_df = spark.createDataFrame(
    duplicate_key_results,
    [
        "table_name",
        "key_columns",
        "duplicate_key_count",
        "validation_passed"
    ]
)

display(duplicate_key_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate Fact-Grain Check

# COMMAND ----------

fact_grain_checks = {
    "fact_orders": (
        fact_orders,
        ["order_id"]
    ),
    "fact_order_items": (
        fact_order_items,
        [
            "order_id",
            "order_item_id"
        ]
    ),
    "fact_payments": (
        fact_payments,
        [
            "order_id",
            "payment_sequential"
        ]
    )
}

fact_grain_results = []

for table_name, configuration in fact_grain_checks.items():
    dataframe, key_columns = configuration

    duplicate_count = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    fact_grain_results.append(
        (
            table_name,
            ", ".join(key_columns),
            duplicate_count,
            duplicate_count == 0
        )
    )

fact_grain_results_df = spark.createDataFrame(
    fact_grain_results,
    [
        "table_name",
        "grain_columns",
        "duplicate_grain_count",
        "validation_passed"
    ]
)

display(fact_grain_results_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as totalrows
# MAGIC from workspace.next_third_party_gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct order_id) as orders
# MAGIC from workspace.next_third_party_gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct customer_id) as customers
# MAGIC from workspace.next_third_party_gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, count(*) as ordersplaced
# MAGIC from workspace.next_third_party_gold.fact_orders
# MAGIC group by customer_id
# MAGIC having count(*) > 1
# MAGIC limit 20;
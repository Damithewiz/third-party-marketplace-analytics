# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_schema = "workspace.next_third_party_bronze"
silver_schema = "workspace.next_third_party_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

print("Silver environment ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC Verifying data in bronze tables

# COMMAND ----------

tables = spark.sql(
    f"SHOW TABLES IN {bronze_schema}"
).collect()

for table in tables:
    table_name = table.tableName
    print(f"\n--- {table_name} ---")
    spark.table(f"{bronze_schema}.{table_name}").printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

bronze_schema = "workspace.next_third_party_bronze"

expected_unique_keys = {
    "olist_customers": ["customer_id"],
    "olist_orders": ["order_id"],
    "olist_products": ["product_id"],
    "olist_sellers": ["seller_id"],
    "olist_order_items": ["order_id", "order_item_id"],
    "olist_order_payments": ["order_id", "payment_sequential"],
    "olist_order_reviews": ["review_id", "order_id"],
    "product_category_name_translation": ["product_category_name"]
}

for table_name in [
    "olist_customers",
    "olist_geolocation",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_orders",
    "olist_products",
    "olist_sellers",
    "product_category_name_translation"
]:
    df = spark.table(f"{bronze_schema}.{table_name}")

    print(" ")
    print(table_name)
    print(f"{'=' * 60}")
    print(f"Row count: {df.count():,}")

    if table_name in expected_unique_keys:
        key_columns = expected_unique_keys[table_name]

        duplicate_key_groups = (
            df.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        null_key_rows = (
            df.filter(
                F.expr(
                    " OR ".join([f"{column} IS NULL" for column in key_columns])
                )
            )
            .count()
        )

        print(f"Duplicate key groups: {duplicate_key_groups:,}")
        print(f"Rows with null key values: {null_key_rows:,}")

    null_counts = df.select([
        F.sum(F.col(column).isNull().cast("integer")).alias(column)
        for column in df.columns
    ]).first().asDict()

    null_columns = {
        column: count
        for column, count in null_counts.items()
        if count > 0
    }

    print(f"Columns with nulls: {null_columns}")

# COMMAND ----------

bronze = "workspace.next_third_party_bronze"

orders = spark.table(f"{bronze}.olist_orders")
reviews = spark.table(f"{bronze}.olist_order_reviews")
items = spark.table(f"{bronze}.olist_order_items")
payments = spark.table(f"{bronze}.olist_order_payments")
products = spark.table(f"{bronze}.olist_products")
customers = spark.table(f"{bronze}.olist_customers")
sellers = spark.table(f"{bronze}.olist_sellers")

print("ORDER STATUS COUNTS")
display(
    orders.groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

print("MISSING ORDER DATES BY STATUS")
display(
    orders.groupBy("order_status")
    .agg(
        F.count("*").alias("orders"),
        F.sum(F.col("order_approved_at").isNull().cast("int"))
            .alias("missing_approved_at"),
        F.sum(F.col("order_delivered_carrier_date").isNull().cast("int"))
            .alias("missing_carrier_date"),
        F.sum(F.col("order_delivered_customer_date").isNull().cast("int"))
            .alias("missing_customer_delivery_date")
    )
    .orderBy(F.desc("orders"))
)

print("REVIEW SCORE VALUES")
display(
    reviews.groupBy("review_score")
    .count()
    .orderBy("review_score")
)

print("FOREIGN KEY ORPHANS")

checks = {
    "Orders without customer": orders.join(
        customers,
        "customer_id",
        "left_anti"
    ).count(),

    "Items without order": items.join(
        orders,
        "order_id",
        "left_anti"
    ).count(),

    "Items without product": items.join(
        products,
        "product_id",
        "left_anti"
    ).count(),

    "Items without seller": items.join(
        sellers,
        "seller_id",
        "left_anti"
    ).count(),

    "Payments without order": payments.join(
        orders,
        "order_id",
        "left_anti"
    ).count(),

    "Reviews without order": reviews.filter(
        F.col("order_id").isNotNull()
    ).join(
        orders,
        "order_id",
        "left_anti"
    ).count()
}

for check, result in checks.items():
    print(f"{check}: {result:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC Building customers silver table from bronze layer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers_bronze = spark.table(
    f"{bronze_schema}.olist_customers"
)

customers_silver = (
    customers_bronze
    .dropDuplicates(["customer_id"])
    .withColumn(
        "customer_city",
        F.initcap(F.trim(F.col("customer_city")))
    )
    .withColumn(
        "customer_state",
        F.upper(F.trim(F.col("customer_state")))
    )
    .withColumn(
        "customer_data_quality_flag",
        F.when(
            F.col("customer_id").isNull()
            | F.col("customer_unique_id").isNull()
            | F.col("customer_zip_code_prefix").isNull()
            | F.col("customer_city").isNull()
            | F.col("customer_state").isNull(),
            F.lit("Incomplete")
        ).otherwise(F.lit("Complete"))
    )
)

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_customers"
    )

# COMMAND ----------

reviews = spark.table(
    "workspace.next_third_party_bronze.olist_order_reviews"
)

print(f"Row count: {reviews.count():,}")

display(
    reviews.groupBy("review_score")
    .count()
    .orderBy("review_score")
)

print(
    "Null order IDs:",
    reviews.filter(F.col("order_id").isNull()).count()
)

print(
    "Reviews without orders:",
    reviews.filter(F.col("order_id").isNotNull())
    .join(orders, "order_id", "left_anti")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

customers_check = spark.table(
    f"{silver_schema}.olist_customers"
)

print(f"Silver row count: {customers_check.count():,}")

display(
    customers_check
    .groupBy("customer_data_quality_flag")
    .count()
)

display(customers_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Building sellers silver table from bronze layer

# COMMAND ----------

from pyspark.sql import functions as F

sellers_bronze = spark.table(
    f"{bronze_schema}.olist_sellers"
)

sellers_silver = (
    sellers_bronze
    .dropDuplicates(["seller_id"])
    .withColumn(
        "seller_city",
        F.initcap(F.trim(F.col("seller_city")))
    )
    .withColumn(
        "seller_state",
        F.upper(F.trim(F.col("seller_state")))
    )
    .withColumn(
        "seller_data_quality_flag",
        F.when(
            F.col("seller_id").isNull()
            | F.col("seller_zip_code_prefix").isNull()
            | F.col("seller_city").isNull()
            | F.col("seller_state").isNull(),
            F.lit("Incomplete")
        ).otherwise(F.lit("Complete"))
    )
)

sellers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_sellers"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

sellers_check = spark.table(
    f"{silver_schema}.olist_sellers"
)

print(f"Silver row count: {sellers_check.count():,}")

display(
    sellers_check
    .groupBy("seller_data_quality_flag")
    .count()
)

display(sellers_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Building products silver table from bronze layer

# COMMAND ----------

from pyspark.sql import functions as F

products_bronze = spark.table(
    f"{bronze_schema}.olist_products"
)

products_silver = (
    products_bronze
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )
    .withColumnRenamed(
        "product_name_lenght",
        "product_name_length"
    )
    .withColumnRenamed(
        "product_description_lenght",
        "product_description_length"
    )
    .withColumn(
        "product_data_quality_flag",
        F.when(
            F.col("product_id").isNull(),
            F.lit("Invalid Key")
        )
        .when(
            F.col("product_category_name").isNull()
            | F.col("product_name_length").isNull()
            | F.col("product_description_length").isNull()
            | F.col("product_photos_qty").isNull(),
            F.lit("Incomplete Description")
        )
        .when(
            F.col("product_weight_g").isNull()
            | F.col("product_length_cm").isNull()
            | F.col("product_height_cm").isNull()
            | F.col("product_width_cm").isNull(),
            F.lit("Incomplete Dimensions")
        )
        .otherwise(F.lit("Complete"))
    )
)

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_products"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------


products_check = spark.table(
    f"{silver_schema}.olist_products"
)

print(f"Silver row count: {products_check.count():,}")

display(
    products_check
    .groupBy("product_data_quality_flag")
    .count()
    .orderBy("product_data_quality_flag")
)

display(products_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Building orders silver table from bronze layer

# COMMAND ----------

from pyspark.sql import functions as F

orders_bronze = spark.table(
    f"{bronze_schema}.olist_orders"
)

orders_silver = (
    orders_bronze
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )
    .withColumn(
        "order_data_quality_flag",
        F.when(
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_purchase_timestamp").isNull(),
            F.lit("Invalid Core Data")
        )
        .when(
            (F.col("order_status") == "delivered")
            & (
                F.col("order_approved_at").isNull()
                | F.col("order_delivered_carrier_date").isNull()
                | F.col("order_delivered_customer_date").isNull()
            ),
            F.lit("Delivered With Missing Dates")
        )
        .otherwise(F.lit("Valid"))
    )
)

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_orders"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

orders_check = spark.table(
    f"{silver_schema}.olist_orders"
)

print(f"Silver row count: {orders_check.count():,}")

display(
    orders_check
    .groupBy("order_data_quality_flag")
    .count()
    .orderBy("order_data_quality_flag")
)

display(
    orders_check
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F

delivered_missing = (
    orders_bronze
    .filter(F.lower(F.trim(F.col("order_status"))) == "delivered")
    .filter(
        F.col("order_approved_at").isNull()
        | F.col("order_delivered_carrier_date").isNull()
        | F.col("order_delivered_customer_date").isNull()
    )
)

print(
    f"Unique delivered orders with any missing date: "
    f"{delivered_missing.select('order_id').distinct().count()}"
)

display(
    delivered_missing
    .withColumn(
        "missing_date_count",
        F.col("order_approved_at").isNull().cast("int")
        + F.col("order_delivered_carrier_date").isNull().cast("int")
        + F.col("order_delivered_customer_date").isNull().cast("int")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Building order items silver table from bronze layer

# COMMAND ----------

from pyspark.sql import functions as F

items_bronze = spark.table(
    f"{bronze_schema}.olist_order_items"
)

items_silver = (
    items_bronze
    .dropDuplicates(["order_id", "order_item_id"])
    .withColumn(
        "item_data_quality_flag",
        F.when(
            F.col("order_id").isNull()
            | F.col("order_item_id").isNull()
            | F.col("product_id").isNull()
            | F.col("seller_id").isNull(),
            F.lit("Invalid Key")
        )
        .when(
            F.col("price").isNull()
            | F.col("freight_value").isNull(),
            F.lit("Missing Financial Data")
        )
        .otherwise(F.lit("Valid"))
    )
)

items_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_order_items"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

items_check = spark.table(
    f"{silver_schema}.olist_order_items"
)

print(f"Silver row count: {items_check.count():,}")

display(
    items_check
    .groupBy("item_data_quality_flag")
    .count()
)

display(items_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Building payments silver table from bronze layer

# COMMAND ----------

payments_bronze = spark.table(
    f"{bronze_schema}.olist_order_payments"
)

payments_silver = (
    payments_bronze
    .dropDuplicates()
    .withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )
    .withColumn(
        "payment_data_quality_flag",
        F.when(
            F.col("order_id").isNull(),
            F.lit("Invalid Order")
        )
        .when(
            F.col("payment_type").isNull(),
            F.lit("Missing Payment Type")
        )
        .when(
            F.col("payment_value").isNull(),
            F.lit("Missing Payment Value")
        )
        .when(
            F.col("payment_value") < 0,
            F.lit("Invalid Payment Value")
        )
        .otherwise(F.lit("Valid"))
    )
)

payments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_order_payments"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

payments_check = spark.table(
    f"{silver_schema}.olist_order_payments"
)

print(f"Silver row count: {payments_check.count():,}")

display(
    payments_check
    .groupBy("payment_data_quality_flag")
    .count()
)

display(
    payments_check
    .groupBy("payment_type")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Building reviews silver table from bronze layer

# COMMAND ----------

reviews_bronze = spark.table(
    f"{bronze_schema}.olist_order_reviews"
)

reviews_silver = (
    reviews_bronze
    .dropDuplicates(["review_id"])
    .withColumn(
        "review_score",
        F.col("review_score").cast("int")
    )
    .withColumn(
        "review_creation_date",
        F.to_timestamp(
            F.col("review_creation_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "review_answer_timestamp",
        F.to_timestamp(
            F.col("review_answer_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "review_comment_title",
        F.trim(F.col("review_comment_title"))
    )
    .withColumn(
        "review_comment_message",
        F.trim(F.col("review_comment_message"))
    )
    .withColumn(
        "review_data_quality_flag",
        F.when(
            F.col("review_id").isNull()
            | F.col("order_id").isNull(),
            F.lit("Invalid Key")
        )
        .when(
            F.col("review_score").isNull(),
            F.lit("Missing or Invalid Review Score")
        )
        .when(
            ~F.col("review_score").isin(1, 2, 3, 4, 5),
            F.lit("Invalid Review Score")
        )
        .when(
            F.col("review_creation_date").isNull()
            | F.col("review_answer_timestamp").isNull(),
            F.lit("Missing or Invalid Review Date")
        )
        .otherwise(F.lit("Valid"))
    )
)

reviews_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.olist_order_reviews"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the table

# COMMAND ----------

reviews_check = spark.table(
    f"{silver_schema}.olist_order_reviews"
)

reviews_check.printSchema()

display(
    reviews_check
    .groupBy("review_score")
    .count()
    .orderBy("review_score")
)

display(
    reviews_check
    .groupBy("review_data_quality_flag")
    .count()
    .orderBy("review_data_quality_flag")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Building product category silver table from bronze layer

# COMMAND ----------

translation_bronze = spark.table(
    f"{bronze_schema}.product_category_name_translation"
)

translation_silver = (
    translation_bronze
    .dropDuplicates(["product_category_name"])
    .withColumn(
        "product_category_name",
        F.trim(F.col("product_category_name"))
    )
    .withColumn(
        "product_category_name_english",
        F.trim(F.col("product_category_name_english"))
        )
    )

translation_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(
        f"{silver_schema}.product_category_name_translation"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Checking date ranges of all tables with timestamps

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(f"{silver_schema}.olist_orders")
items = spark.table(f"{silver_schema}.olist_order_items")
reviews = spark.table(f"{silver_schema}.olist_order_reviews")

date_ranges = []

date_columns = [
    ("Orders", "order_purchase_timestamp", orders),
    ("Orders", "order_approved_at", orders),
    ("Orders", "order_delivered_carrier_date", orders),
    ("Orders", "order_delivered_customer_date", orders),
    ("Orders", "order_estimated_delivery_date", orders),
    ("Order Items", "shipping_limit_date", items),
    ("Reviews", "review_creation_date", reviews),
    ("Reviews", "review_answer_timestamp", reviews)
]

for table_name, column_name, df in date_columns:
    result = (
        df.select(
            F.min(column_name).alias("min_date"),
            F.max(column_name).alias("max_date")
        )
        .withColumn("table", F.lit(table_name))
        .withColumn("column", F.lit(column_name))
    )

    date_ranges.append(result)

display(date_ranges[0].unionByName(date_ranges[1])
                    .unionByName(date_ranges[2])
                    .unionByName(date_ranges[3])
                    .unionByName(date_ranges[4])
                    .unionByName(date_ranges[5])
                    .unionByName(date_ranges[6])
                    .unionByName(date_ranges[7]))

# COMMAND ----------

items = spark.table(f"{silver_schema}.olist_order_items")

display(
    items
    .filter(F.col("shipping_limit_date") >= "2019-01-01")
    .orderBy("shipping_limit_date")
)

# COMMAND ----------

orders = spark.table(f"{silver_schema}.olist_orders")

display(
    orders.filter(
        F.col("order_id").isin(
            "9c94a4ea2f7876660fa6f1b59b69c8e6",
            "13bdf405f961a6deec817d817f5c6624",
            "c2bb89b5c1dd978d507284be78a04cb2"
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Previewing all final silver tables

# COMMAND ----------

# Customers

customers = spark.table(f"{silver_schema}.olist_customers")

print("===== CUSTOMERS =====")
print(f"Rows: {customers.count():,}")

customers.printSchema()

display(
    customers
    .groupBy("customer_data_quality_flag")
    .count()
)

display(customers.limit(10))

# COMMAND ----------

# Sellers

sellers = spark.table(f"{silver_schema}.olist_sellers")

print("===== SELLERS =====")
print(f"Rows: {sellers.count():,}")

sellers.printSchema()

display(
    sellers
    .groupBy("seller_data_quality_flag")
    .count()
)

display(sellers.limit(10))

# COMMAND ----------

# Products

products = spark.table(f"{silver_schema}.olist_products")

print("===== PRODUCTS =====")
print(f"Rows: {products.count():,}")

products.printSchema()

display(
    products
    .groupBy("product_data_quality_flag")
    .count()
)

display(products.limit(10))

# COMMAND ----------

# Orders

orders = spark.table(f"{silver_schema}.olist_orders")

print("===== ORDERS =====")
print(f"Rows: {orders.count():,}")

orders.printSchema()

display(
    orders
    .groupBy("order_data_quality_flag")
    .count()
)

display(orders.limit(10))

# COMMAND ----------

# Order Items

items = spark.table(f"{silver_schema}.olist_order_items")

print("===== ORDER ITEMS =====")
print(f"Rows: {items.count():,}")

items.printSchema()

display(
    items
    .groupBy("item_data_quality_flag")
    .count()
)

display(items.limit(10))

# COMMAND ----------

# Payments 

payments = spark.table(f"{silver_schema}.olist_order_payments")

print("===== PAYMENTS =====")
print(f"Rows: {payments.count():,}")

payments.printSchema()

display(
    payments
    .groupBy("payment_data_quality_flag")
    .count()
)

display(payments.limit(10))

# COMMAND ----------

# Reviews

reviews = spark.table(f"{silver_schema}.olist_order_reviews")

print("===== REVIEWS =====")
print(f"Rows: {reviews.count():,}")

reviews.printSchema()

display(
    reviews
    .groupBy("review_data_quality_flag")
    .count()
)

display(reviews.limit(10))

# COMMAND ----------

# Category Translation 

translation = spark.table(
    f"{silver_schema}.product_category_name_translation"
)

print("===== CATEGORY TRANSLATION =====")
print(f"Rows: {translation.count():,}")

translation.printSchema()

display(translation.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# Databricks notebook source
# MAGIC %md
# MAGIC Building Date Dimension 

# COMMAND ----------

# MAGIC %md
# MAGIC Because of the anomalous 2020 shipping-limit dates, I will derive the date range from the valid operational columns only

# COMMAND ----------

from pyspark.sql import functions as F

silver_schema = "workspace.next_third_party_silver"
gold_schema = "workspace.next_third_party_gold"

orders = spark.table(f"{silver_schema}.olist_orders")
reviews = spark.table(f"{silver_schema}.olist_order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Date table

# COMMAND ----------

# Create the valid minimum and maximum dates
date_bounds = (
    orders.select(
        F.least(
            F.min("order_purchase_timestamp"),
            F.min("order_approved_at"),
            F.min("order_delivered_carrier_date"),
            F.min("order_delivered_customer_date"),
            F.min("order_estimated_delivery_date")
        ).alias("min_date"),
        F.greatest(
            F.max("order_purchase_timestamp"),
            F.max("order_approved_at"),
            F.max("order_delivered_carrier_date"),
            F.max("order_delivered_customer_date"),
            F.max("order_estimated_delivery_date")
        ).alias("max_date")
    )
)

review_bounds = (
    reviews.select(
        F.least(
            F.min("review_creation_date"),
            F.min("review_answer_timestamp")
        ).alias("min_date"),
        F.greatest(
            F.max("review_creation_date"),
            F.max("review_answer_timestamp")
        ).alias("max_date")
    )
)

overall_bounds = (
    date_bounds
    .unionByName(review_bounds)
    .select(
        F.to_date(F.min("min_date")).alias("start_date"),
        F.to_date(F.max("max_date")).alias("end_date")
    )
)

display(overall_bounds)

# COMMAND ----------

# Create the dim_date table
dim_date = (
    overall_bounds
    .select(
        F.explode(
            F.sequence(
                F.col("start_date"),
                F.col("end_date"),
                F.expr("interval 1 day")
            )
        ).alias("full_date")
    )
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn(
        "quarter_name",
        F.concat(F.lit("Q"), F.quarter("full_date"))
    )
    .withColumn(
        "calendar_quarter_label",
        F.concat(
            F.lit("Q"),
            F.quarter("full_date"),
            F.lit(" "),
            F.year("full_date")
        )
    )
    .withColumn("month_number", F.month("full_date"))
    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )
    .withColumn(
        "month_short",
        F.date_format("full_date", "MMM")
    )
    .withColumn(
        "year_month",
        F.date_format("full_date", "yyyy-MM")
    )
    .withColumn(
        "month_year_label",
        F.date_format("full_date", "MMM yyyy")
    )
    .withColumn(
        "week_of_year",
        F.weekofyear("full_date")
    )
    .withColumn(
        "day_of_month",
        F.dayofmonth("full_date")
    )
    .withColumn(
        "day_name",
        F.date_format("full_date", "EEEE")
    )
    .withColumn(
        "day_short",
        F.date_format("full_date", "EEE")
    )
    .withColumn(
        "day_of_week",
        F.when(F.dayofweek("full_date") == 1, 7)
        .otherwise(F.dayofweek("full_date") - 1)
    )
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin(6, 7)
    )
    .withColumn(
        "is_month_start",
        F.col("full_date") == F.trunc("full_date", "month")
    )
    .withColumn(
        "is_month_end",
        F.col("full_date") == F.last_day("full_date")
    )
)

# COMMAND ----------

# adding the dates dimension to the gold layer
dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.dim_date")

# COMMAND ----------

dim_date_check = spark.table(f"{gold_schema}.dim_date")

print(f"Rows: {dim_date_check.count():,}")
dim_date_check.printSchema()

display(
    dim_date_check
    .orderBy("full_date")
    .limit(10)
)

display(
    dim_date_check
    .orderBy(F.desc("full_date"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Building other gold tables

# COMMAND ----------

# Customers table
# Grain: one row per customer
customers = spark.table(
    f"{silver_schema}.olist_customers"
)

dim_customer = (
    customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "customer_data_quality_flag"
    )
    .dropDuplicates(["customer_id"])
)

# COMMAND ----------

# write to gold layer
dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.dim_customer")

# COMMAND ----------

dim_customer_check = spark.table(
    f"{gold_schema}.dim_customer"
)

print(f"Rows: {dim_customer_check.count():,}")

dim_customer_check.printSchema()

display(
    dim_customer_check
    .groupBy("customer_data_quality_flag")
    .count()
)

display(dim_customer_check.limit(10))

# COMMAND ----------

products = spark.table(
    f"{silver_schema}.olist_products"
)

category_translation = spark.table(
    f"{silver_schema}.product_category_name_translation"
)

# COMMAND ----------

# Products and product categories name translation are joined here so they don't need to be later
# Grain: one row per product
dim_product = (
    products.alias("p")
    .join(
        category_translation.alias("t"),
        on="product_category_name",
        how="left"
    )
    .select(
        F.col("p.product_id"),
        F.col("p.product_category_name"),
        F.col("t.product_category_name_english"),
        F.col("p.product_name_length"),
        F.col("p.product_description_length"),
        F.col("p.product_photos_qty"),
        F.col("p.product_weight_g"),
        F.col("p.product_length_cm"),
        F.col("p.product_height_cm"),
        F.col("p.product_width_cm"),
        F.col("p.product_data_quality_flag")
    )
)

# COMMAND ----------

# write to gold layer
dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.dim_product")

# COMMAND ----------

dim_product_check = spark.table(
    f"{gold_schema}.dim_product"
)

print(f"Rows: {dim_product_check.count():,}")

dim_product_check.printSchema()

display(
    dim_product_check
    .groupBy("product_data_quality_flag")
    .count()
)

display(dim_product_check.limit(10))

# COMMAND ----------

# sellers table
# Grain: one row per seller
sellers = spark.table(
    f"{silver_schema}.olist_sellers"
)

dim_seller = (
    sellers
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_data_quality_flag"
    )
    .dropDuplicates(["seller_id"])
)

# COMMAND ----------

# write to gold layer
dim_seller.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.dim_seller")

# COMMAND ----------

dim_seller_check = spark.table(
    f"{gold_schema}.dim_seller"
)

print(f"Rows: {dim_seller_check.count():,}")

dim_seller_check.printSchema()

display(
    dim_seller_check
    .groupBy("seller_data_quality_flag")
    .count()
)

display(dim_seller_check.limit(10))

# COMMAND ----------

# orders table
# Grain: one row per order
orders = spark.table(
    f"{silver_schema}.olist_orders"
)

items = spark.table(
    f"{silver_schema}.olist_order_items"
)

order_item_summary = (
    items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),
        F.sum("price").alias("merchandise_value"),
        F.sum("freight_value").alias("freight_value")
    )
)

fact_orders = (
    orders.alias("o")
    .join(
        order_item_summary.alias("i"),
        on="order_id",
        how="left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),

        # Date keys for relationships
        F.date_format(
            F.to_date("o.order_purchase_timestamp"),
            "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        F.date_format(
            F.to_date("o.order_approved_at"),
            "yyyyMMdd"
        ).cast("int").alias("approval_date_key"),

        F.date_format(
            F.to_date("o.order_delivered_carrier_date"),
            "yyyyMMdd"
        ).cast("int").alias("carrier_date_key"),

        F.date_format(
            F.to_date("o.order_delivered_customer_date"),
            "yyyyMMdd"
        ).cast("int").alias("delivery_date_key"),

        F.date_format(
            F.to_date("o.order_estimated_delivery_date"),
            "yyyyMMdd"
        ).cast("int").alias("estimated_delivery_date_key"),

        # Original timestamps for detailed analysis
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_approved_at"),
        F.col("o.order_delivered_carrier_date"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),

        F.col("o.order_status"),

        F.coalesce(
            F.col("i.item_count"),
            F.lit(0)
        ).alias("item_count"),

        F.coalesce(
            F.col("i.merchandise_value"),
            F.lit(0.0)
        ).alias("merchandise_value"),

        F.coalesce(
            F.col("i.freight_value"),
            F.lit(0.0)
        ).alias("freight_value"),

        (
            F.coalesce(F.col("i.merchandise_value"), F.lit(0.0))
            + F.coalesce(F.col("i.freight_value"), F.lit(0.0))
        ).alias("order_total_value"),

        # Time-based metrics
        (
            (
                F.col("o.order_approved_at").cast("long")
                - F.col("o.order_purchase_timestamp").cast("long")
            ) / F.lit(3600.0)
        ).alias("approval_time_hours"),

        (
            (
                F.col("o.order_delivered_carrier_date").cast("long")
                - F.col("o.order_approved_at").cast("long")
            ) / F.lit(3600.0)
        ).alias("approval_to_carrier_hours"),

        (
            (
                F.col("o.order_delivered_customer_date").cast("long")
                - F.col("o.order_purchase_timestamp").cast("long")
            ) / F.lit(86400.0)
        ).alias("delivery_lead_time_days"),

        F.datediff(
            F.to_date("o.order_delivered_customer_date"),
            F.to_date("o.order_estimated_delivery_date")
        ).alias("delivery_delay_days"),

        F.when(
            F.col("o.order_delivered_customer_date")
            > F.col("o.order_estimated_delivery_date"),
            True
        )
        .when(
            F.col("o.order_delivered_customer_date").isNull()
            | F.col("o.order_estimated_delivery_date").isNull(),
            F.lit(None).cast("boolean")
        )
        .otherwise(False)
        .alias("is_late_delivery"),

        F.col("o.order_data_quality_flag")
    )
)

# COMMAND ----------

# write to gold layer
fact_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.fact_orders")

# COMMAND ----------

fact_orders_check = spark.table(
    f"{gold_schema}.fact_orders"
)

print(f"Rows: {fact_orders_check.count():,}")

fact_orders_check.printSchema()

display(
    fact_orders_check
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

display(fact_orders_check.limit(10))

# COMMAND ----------

# order items table
# Grain: one row per order item
# Intentionally bringing customer_id and purchase_date_key into this fact. This avoids fact-to-fact joins in Power BI
orders = spark.table(
    f"{silver_schema}.olist_orders"
)

items = spark.table(
    f"{silver_schema}.olist_order_items"
)

fact_order_items = (
    items.alias("i")
    .join(
        orders.alias("o"),
        on="order_id",
        how="left"
    )
    .select(
        F.col("i.order_id"),

        F.col("i.order_item_id"),

        F.col("o.customer_id"),

        F.col("i.product_id"),

        F.col("i.seller_id"),

        F.date_format(
            F.to_date("o.order_purchase_timestamp"),
            "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        # Original shipping timestamp
        F.col("i.shipping_limit_date"),

        F.col("i.price"),

        F.col("i.freight_value"),

        (
            F.col("i.price")
            + F.col("i.freight_value")
        ).alias("line_total"),

        F.col("i.item_data_quality_flag")
    )
)

# COMMAND ----------

print("Silver orders:", orders.count())
print("Silver order items:", items.count())
print("Gold fact_order_items:", fact_order_items.count())

# COMMAND ----------

# write to gold layer
fact_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.fact_order_items")

# COMMAND ----------

# View all gold tables
spark.sql(f"SHOW TABLES IN {gold_schema}").show(truncate=False)

# COMMAND ----------

# check gold table row counts
gold_tables = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_seller",
    "fact_orders",
    "fact_order_items"
]

for table in gold_tables:
    count = spark.table(f"{gold_schema}.{table}").count()
    print(f"{table:<20} {count:,}")

# COMMAND ----------

# view the table schemas
for table in gold_tables:
    print(f"\n===== {table} =====")
    spark.table(f"{gold_schema}.{table}").printSchema()

# COMMAND ----------

# preview full gold tables
for table in gold_tables:
    print(f"\n===== {table} =====")
    display(
        spark.table(f"{gold_schema}.{table}").limit(10)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Verify relationships between gold tables

# COMMAND ----------

# Orders without customers
fact_orders = spark.table(f"{gold_schema}.fact_orders")
dim_customer = spark.table(f"{gold_schema}.dim_customer")

fact_orders.join(
    dim_customer,
    "customer_id",
    "left_anti"
).count()

# COMMAND ----------

# Order items without products
fact_order_items = spark.table(f"{gold_schema}.fact_order_items")
dim_product = spark.table(f"{gold_schema}.dim_product")

fact_order_items.join(
    dim_product,
    "product_id",
    "left_anti"
).count()

# COMMAND ----------

# Order items without sellers
dim_seller = spark.table(f"{gold_schema}.dim_seller")

fact_order_items.join(
    dim_seller,
    "seller_id",
    "left_anti"
).count()

# COMMAND ----------

# payments table
# Grain: one row per payment transaction

payments = spark.table(
    f"{silver_schema}.olist_order_payments"
)

orders = spark.table(
    f"{silver_schema}.olist_orders"
)

fact_payments = (
    payments.alias("p")
    .join(
        orders.alias("o"),
        F.col("p.order_id") == F.col("o.order_id"),
        "left"
    )
    .select(
        F.col("p.order_id"),
        F.col("p.payment_sequential"),
        F.col("o.customer_id"),

        F.date_format(
            F.to_date(F.col("o.order_purchase_timestamp")),
            "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        F.col("p.payment_type"),
        F.col("p.payment_installments"),
        F.col("p.payment_value"),
        F.col("p.payment_data_quality_flag")
    )
)


# COMMAND ----------

print(f"Rows: {fact_payments.count():,}")
fact_payments.printSchema()

display(fact_payments.limit(10))

# COMMAND ----------

# write to gold layer
fact_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.fact_payments")

# COMMAND ----------

fact_payments_check = spark.table(
    f"{gold_schema}.fact_payments"
)

print(f"Rows: {fact_payments_check.count():,}")

fact_payments_check.printSchema()

display(
    fact_payments_check
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("payment_transactions"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_installments").alias("average_installments")
    )
    .orderBy(F.desc("total_payment_value"))
)

display(fact_payments_check.limit(10))

# COMMAND ----------

# reviews table
# Grain: one row per review
reviews = spark.table(
    f"{silver_schema}.olist_order_reviews"
)

orders = spark.table(
    f"{silver_schema}.olist_orders"
)

fact_reviews = (
    reviews.alias("r")
    .join(
        orders.alias("o"),
        F.col("r.order_id") == F.col("o.order_id"),
        "left"
    )
    .select(
        F.col("r.review_id"),
        F.col("r.order_id"),
        F.col("o.customer_id"),

        F.date_format(
            F.to_date(F.col("o.order_purchase_timestamp")),
            "yyyyMMdd"
        ).cast("int").alias("purchase_date_key"),

        F.date_format(
            F.to_date(F.col("r.review_creation_date")),
            "yyyyMMdd"
        ).cast("int").alias("review_creation_date_key"),

        F.date_format(
            F.to_date(F.col("r.review_answer_timestamp")),
            "yyyyMMdd"
        ).cast("int").alias("review_answer_date_key"),

        # Original timestamps
        F.col("r.review_creation_date"),
        F.col("r.review_answer_timestamp"),

        F.col("r.review_score"),
        F.col("r.review_comment_title"),
        F.col("r.review_comment_message"),

        (
            (
                F.col("r.review_answer_timestamp").cast("long")
                - F.col("r.review_creation_date").cast("long")
            ) / F.lit(3600.0)
        ).alias("review_response_time_hours"),

        F.when(
            F.col("r.review_score") >= 4,
            True
        ).otherwise(False).alias("is_positive_review"),

        F.when(
            F.col("r.review_score") <= 2,
            True
        ).otherwise(False).alias("is_negative_review"),

        F.col("r.review_data_quality_flag")
    )
)

# COMMAND ----------

print(f"Rows: {fact_reviews.count():,}")
fact_reviews.printSchema()

display(
    fact_reviews.select(
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp",
        "review_response_time_hours",
        "is_positive_review",
        "is_negative_review"
    ).limit(10)
)

# COMMAND ----------

silver_review_count = reviews.count()
gold_review_count = fact_reviews.count()

print(f"Silver reviews: {silver_review_count:,}")
print(f"Gold reviews:   {gold_review_count:,}")
print(f"Difference:     {silver_review_count - gold_review_count:,}")

# COMMAND ----------

# write to gold layer
fact_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{gold_schema}.fact_reviews")

# COMMAND ----------

fact_reviews_check = spark.table(
    f"{gold_schema}.fact_reviews"
)

print(f"Rows: {fact_reviews_check.count():,}")

fact_reviews_check.printSchema()

display(
    fact_reviews_check
    .groupBy("review_score")
    .agg(
        F.count("*").alias("review_count"),
        F.avg("review_response_time_hours").alias(
            "average_response_time_hours"
        )
    )
    .orderBy("review_score")
)

display(fact_reviews_check.limit(10))

# COMMAND ----------

unmatched_reviews = (
    reviews.alias("r")
    .join(
        orders.alias("o"),
        F.col("r.order_id") == F.col("o.order_id"),
        "left_anti"
    )
    .count()
)

print(f"Reviews without matching orders: {unmatched_reviews:,}")
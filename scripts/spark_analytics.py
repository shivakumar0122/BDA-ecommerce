from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    datediff,
    expr,
    regexp_replace,
    round,
    sum as spark_sum,
    to_timestamp,
    when,
)
import csv
import os


def write_rows(path, headers, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(list(r))


spark = (
    SparkSession.builder.appName("BDA-Ecommerce-Spark-Analytics")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

cleaned_path = "hdfs://localhost:9000/user/saimohaneesh/ecommerce/cleaned/part-r-00000"
reviews_path = "hdfs://localhost:9000/user/saimohaneesh/ecommerce/raw/reviews/olist_order_reviews_dataset.csv"

base = (
    spark.read.option("sep", "\t").option("header", "false").csv(cleaned_path)
    .toDF(
        "order_id",
        "customer_id",
        "customer_state",
        "customer_city",
        "product_id",
        "category_name",
        "seller_id",
        "price",
        "freight_value",
        "purchase_timestamp",
        "order_year",
        "order_month",
        "estimated_delivery",
        "actual_delivery",
    )
)

clean = (
    base.withColumn("order_id", regexp_replace(col("order_id"), '"', ""))
    .withColumn("customer_id", regexp_replace(col("customer_id"), '"', ""))
    .withColumn("product_id", regexp_replace(col("product_id"), '"', ""))
    .withColumn("category_name", regexp_replace(col("category_name"), '"', ""))
    .withColumn("seller_id", regexp_replace(col("seller_id"), '"', ""))
    .withColumn("price_num", col("price").cast("double"))
    .withColumn("freight_num", col("freight_value").cast("double"))
    .withColumn("purchase_ts", to_timestamp(col("purchase_timestamp")))
    .withColumn("estimated_ts", to_timestamp(col("estimated_delivery")))
    .withColumn("actual_ts", to_timestamp(col("actual_delivery")))
)

# 1) Top products by revenue
top_products = (
    clean.filter(col("price_num").isNotNull())
    .groupBy("product_id", "category_name")
    .agg(
        round(spark_sum("price_num"), 2).alias("total_revenue"),
        count("order_id").alias("units_sold"),
    )
    .orderBy(col("total_revenue").desc())
    .limit(10)
)

# 2) Month-by-month sales trend
monthly_trend = (
    clean.filter(col("price_num").isNotNull() & col("order_year").isNotNull() & col("order_month").isNotNull())
    .groupBy("order_year", "order_month")
    .agg(
        round(spark_sum("price_num"), 2).alias("monthly_revenue"),
        countDistinct("order_id").alias("orders_count"),
    )
    .orderBy("order_year", "order_month")
)

# 3) Revenue by customer state
state_revenue = (
    clean.filter(col("price_num").isNotNull() & col("customer_state").isNotNull())
    .groupBy("customer_state")
    .agg(
        round(spark_sum("price_num"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("orders_count"),
    )
    .orderBy(col("total_revenue").desc())
)

# 4) Delivery performance
delivery = (
    clean.filter(col("estimated_ts").isNotNull() & col("actual_ts").isNotNull())
    .withColumn("delay_days", datediff(col("actual_ts"), col("estimated_ts")))
)

delivery_summary = delivery.agg(
    round(avg("delay_days"), 2).alias("avg_delay_days"),
    count(when(col("delay_days") > 0, True)).alias("late_deliveries"),
    count("order_id").alias("total_deliveries"),
)

# 5) Review score by category (join with reviews)
reviews = (
    spark.read.option("header", "true").csv(reviews_path)
    .select("order_id", "review_score")
    .withColumn("order_id", regexp_replace(col("order_id"), '"', ""))
    .withColumn("review_score_num", col("review_score").cast("double"))
)

category_review = (
    clean.join(reviews, on="order_id", how="inner")
    .filter(col("category_name").isNotNull() & col("review_score_num").isNotNull())
    .groupBy("category_name")
    .agg(
        round(avg("review_score_num"), 2).alias("avg_review_score"),
        count("order_id").alias("review_count"),
    )
    .orderBy(col("review_count").desc())
    .limit(20)
)

out_dir = "/home/saimohaneesh/bda-ecommerce/results/spark"

write_rows(
    f"{out_dir}/top_products.csv",
    ["product_id", "category_name", "total_revenue", "units_sold"],
    top_products.collect(),
)
write_rows(
    f"{out_dir}/monthly_trend.csv",
    ["order_year", "order_month", "monthly_revenue", "orders_count"],
    monthly_trend.collect(),
)
write_rows(
    f"{out_dir}/state_revenue.csv",
    ["customer_state", "total_revenue", "orders_count"],
    state_revenue.collect(),
)
write_rows(
    f"{out_dir}/delivery_summary.csv",
    ["avg_delay_days", "late_deliveries", "total_deliveries"],
    delivery_summary.collect(),
)
write_rows(
    f"{out_dir}/category_review.csv",
    ["category_name", "avg_review_score", "review_count"],
    category_review.collect(),
)

print("Spark analytics completed.")
print(f"Output files written under: {out_dir}")

spark.stop()

"""
spark/spark_metrics.py
----------------------
PySpark job that reads raw order data from PostgreSQL
and writes aggregated daily metrics to marts schema.

Submit via Spark master UI or:
  spark-submit --master spark://localhost:7077 spark/spark_metrics.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5433")
DB_NAME     = os.getenv("DB_NAME", "ecommerce")
DB_USER     = os.getenv("DB_USER", "pipeline")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline")
JDBC_URL    = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_PROPS = {
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "driver":   "org.postgresql.Driver",
}


def main():
    spark = (
        SparkSession.builder
        .appName("EcommerceMetrics")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[spark] Reading raw tables...")

    orders = spark.read.jdbc(JDBC_URL, "raw.orders", properties=JDBC_PROPS)
    items  = spark.read.jdbc(JDBC_URL, "raw.order_items", properties=JDBC_PROPS)

    # Item-level aggregation per order
    item_agg = items.groupBy("order_id").agg(
        F.sum(F.col("price") * F.col("quantity")).alias("gross_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.countDistinct("product_id").alias("distinct_products"),
    )

    # Join orders with item aggregates
    joined = orders.join(item_agg, on="order_id", how="left")

    # Daily metrics
    daily = (
        joined
        .withColumn("order_date", F.to_date("order_purchase_ts"))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
            F.round(F.avg("gross_revenue"), 2).alias("avg_order_value"),
            F.round(F.avg(
                F.when(F.col("order_delivered_ts").isNotNull(),
                       F.datediff("order_delivered_ts", "order_purchase_ts"))
            ), 1).alias("avg_delivery_days"),
            F.sum(F.when(F.col("order_status") == "returned", 1).otherwise(0)).alias("returned_orders"),
            F.round(
                100.0 * F.sum(F.when(F.col("order_status") == "returned", 1).otherwise(0))
                / F.countDistinct("order_id"), 2
            ).alias("return_rate_pct"),
        )
        .orderBy(F.col("order_date").desc())
    )

    print(f"[spark] Writing {daily.count()} daily metric rows to marts...")

    daily.write.jdbc(
        JDBC_URL,
        "marts.spark_order_metrics",
        mode="overwrite",
        properties=JDBC_PROPS,
    )

    print("[spark] Done.")
    spark.stop()


if __name__ == "__main__":
    main()

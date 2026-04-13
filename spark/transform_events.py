"""
transform_events.py — PySpark batch transformation for eCommerce behavior data.

Reads raw CSVs from GCS (or local), applies transformations, writes Parquet.

Transformations:
  - Parse event_time → event_date, hour, day_of_week
  - Split category_code → category_level1, category_level2
  - Fill null brands with "unknown"
  - Compute per-session metrics: duration, event count, has_purchase
  - Compute funnel flags per session: viewed, carted, purchased
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ecom-behavior-transform")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def transform(spark: SparkSession, input_path: str, output_path: str) -> None:
    # ---- Read raw CSVs ----
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # ---- Basic cleaning and enrichment ----
    df = (
        df_raw
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss z"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("hour", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
        .withColumn(
            "category_level1",
            F.coalesce(F.split(F.col("category_code"), r"\.").getItem(0), F.lit("unknown")),
        )
        .withColumn(
            "category_level2",
            F.split(F.col("category_code"), r"\.").getItem(1),
        )
    )

    # ---- Session-level metrics ----
    session_window = Window.partitionBy("user_session")

    df = (
        df
        .withColumn("session_start", F.min("event_time").over(session_window))
        .withColumn("session_end", F.max("event_time").over(session_window))
        .withColumn(
            "session_duration_sec",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
        .withColumn("session_event_count", F.count("*").over(session_window))
        .withColumn(
            "session_has_purchase",
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).over(session_window),
        )
        .withColumn(
            "session_has_cart",
            F.max(F.when(F.col("event_type") == "cart", 1).otherwise(0)).over(session_window),
        )
        .withColumn(
            "session_has_view",
            F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).over(session_window),
        )
    )

    # ---- Drop intermediate columns and write ----
    df_final = df.drop("session_start", "session_end")

    print(f"[spark] Writing {df_final.count()} rows to {output_path}")

    (
        df_final
        .repartition("event_date")
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    print("[spark] Transform complete")


def main() -> None:
    gcs_bucket = os.getenv("GCS_BUCKET", "")

    if gcs_bucket:
        input_path = f"gs://{gcs_bucket}/raw/*.csv"
        output_path = f"gs://{gcs_bucket}/processed/"
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        input_path = os.path.join(base, "data", "raw", "*.csv")
        output_path = os.path.join(base, "data", "processed")

    print(f"[spark] Input:  {input_path}")
    print(f"[spark] Output: {output_path}")

    spark = get_spark()
    transform(spark, input_path, output_path)
    spark.stop()


if __name__ == "__main__":
    main()

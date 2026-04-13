"""
transform_events.py — PySpark batch transformation for NYC TLC trip data.

Reads raw parquet files from GCS (or local), normalizes taxi schema, and writes
partitioned Parquet for BigQuery load.
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def col_or_null(df, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nyc-tlc-transform")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def transform(spark: SparkSession, input_paths: str | list[str], output_path: str) -> None:
    # ---- Read raw parquet files ----
    if isinstance(input_paths, list):
        df_raw = spark.read.parquet(*input_paths)
    else:
        df_raw = spark.read.parquet(input_paths)

    # Normalize mixed taxi schemas (yellow/green variants)
    pickup_ts = F.coalesce(
        col_or_null(df_raw, "tpep_pickup_datetime"),
        col_or_null(df_raw, "lpep_pickup_datetime"),
        col_or_null(df_raw, "pickup_datetime"),
    )
    dropoff_ts = F.coalesce(
        col_or_null(df_raw, "tpep_dropoff_datetime"),
        col_or_null(df_raw, "lpep_dropoff_datetime"),
        col_or_null(df_raw, "dropoff_datetime"),
    )

    df = (
        df_raw
        .withColumn("pickup_datetime", F.to_timestamp(pickup_ts))
        .withColumn("dropoff_datetime", F.to_timestamp(dropoff_ts))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("pickup_datetime"))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .withColumn(
            "vendor_id",
            F.coalesce(col_or_null(df_raw, "VendorID"), col_or_null(df_raw, "vendorid")).cast("int"),
        )
        .withColumn(
            "passenger_count",
            F.coalesce(col_or_null(df_raw, "passenger_count"), F.lit(0)).cast("double"),
        )
        .withColumn(
            "trip_distance",
            F.coalesce(col_or_null(df_raw, "trip_distance"), F.lit(0.0)).cast("double"),
        )
        .withColumn(
            "pulocation_id",
            F.coalesce(col_or_null(df_raw, "PULocationID"), col_or_null(df_raw, "pu_location_id")).cast("int"),
        )
        .withColumn(
            "dolocation_id",
            F.coalesce(col_or_null(df_raw, "DOLocationID"), col_or_null(df_raw, "do_location_id")).cast("int"),
        )
        .withColumn("payment_type", col_or_null(df_raw, "payment_type").cast("int"))
        .withColumn(
            "fare_amount",
            F.coalesce(col_or_null(df_raw, "fare_amount"), F.lit(0.0)).cast("double"),
        )
        .withColumn(
            "tip_amount",
            F.coalesce(col_or_null(df_raw, "tip_amount"), F.lit(0.0)).cast("double"),
        )
        .withColumn(
            "total_amount",
            F.coalesce(col_or_null(df_raw, "total_amount"), F.lit(0.0)).cast("double"),
        )
    )

    # Keep valid completed trips and selected normalized columns
    df_final = (
        df
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("trip_duration_min") > 0)
        .select(
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_date",
            "pickup_hour",
            "day_of_week",
            "passenger_count",
            "trip_distance",
            "pulocation_id",
            "dolocation_id",
            "payment_type",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "trip_duration_min",
        )
    )

    print(f"[spark] Writing {df_final.count()} rows to {output_path}")

    (
        df_final
        .repartition("pickup_date")
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(output_path)
    )

    print("[spark] Transform complete")


def main() -> None:
    gcs_bucket = os.getenv("GCS_BUCKET", "")

    if gcs_bucket:
        input_paths: str | list[str] = f"gs://{gcs_bucket}/raw/*.parquet"
        output_path = f"gs://{gcs_bucket}/processed/"
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_dir = Path(base) / "data" / "raw"
        local_files = sorted(str(p) for p in raw_dir.glob("*.parquet"))
        if not local_files:
            raise FileNotFoundError(f"No parquet files found in {raw_dir}")
        input_paths = local_files
        output_path = os.path.join(base, "data", "processed")

    print(f"[spark] Input:  {input_paths}")
    print(f"[spark] Output: {output_path}")

    spark = get_spark()
    transform(spark, input_paths, output_path)
    spark.stop()


if __name__ == "__main__":
    main()

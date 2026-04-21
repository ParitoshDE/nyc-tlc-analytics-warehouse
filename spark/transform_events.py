"""
transform_events.py — PySpark batch transformation for NYC TLC trip data.

Reads raw parquet files from GCS (or local), normalizes taxi schema, and writes
partitioned Parquet for BigQuery load.
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def col_or_null(df, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def expand_glob_paths(spark: SparkSession, pattern: str) -> list[str]:
    """Resolve wildcard paths (including gs://) into concrete file paths via Hadoop FS."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path_obj = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(path_obj.toUri(), hconf)
    statuses = fs.globStatus(path_obj)
    if statuses is None:
        return []
    return [str(status.getPath()) for status in statuses]


def get_spark(enable_gcs: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("nyc-tlc-transform")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    if enable_gcs:
        builder = (
            builder
            # Add GCS connector so Spark can read/write gs:// paths directly.
            .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        )

        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path:
            normalized_path = credentials_path.replace("\\", "/")
            builder = (
                builder
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", normalized_path)
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", normalized_path)
            )

    return builder.getOrCreate()


def write_with_pyarrow_fallback(df_final, output_path: str, batch_size: int = 50000) -> None:
    out_dir = Path(output_path)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_batch: list[dict] = []
    rows_written = 0
    file_index = 0

    for row in df_final.toLocalIterator():
        rows_batch.append(row.asDict(recursive=True))
        if len(rows_batch) >= batch_size:
            table = pa.Table.from_pylist(rows_batch)
            file_path = out_dir / f"part-{file_index:05d}.parquet"
            pq.write_table(table, file_path, compression="snappy")
            rows_written += len(rows_batch)
            rows_batch = []
            file_index += 1

    if rows_batch:
        table = pa.Table.from_pylist(rows_batch)
        file_path = out_dir / f"part-{file_index:05d}.parquet"
        pq.write_table(table, file_path, compression="snappy")
        rows_written += len(rows_batch)

    print(f"[spark] Fallback write complete: {rows_written} rows at {output_path}")


def transform(
    spark: SparkSession,
    input_paths: str | list[str],
    output_path: str,
    sample_rows: int | None = None,
) -> None:
    # ---- Read raw parquet files ----
    reader = spark.read.option("mergeSchema", "true")
    sampled_per_input = False

    if isinstance(input_paths, str) and "*" in input_paths:
        expanded = expand_glob_paths(spark, input_paths)
        if not expanded:
            raise FileNotFoundError(f"No parquet input files matched pattern: {input_paths}")
        input_paths = sorted(expanded)

    if isinstance(input_paths, list):
        df_raw = None
        rows_per_input = None
        if sample_rows is not None and sample_rows > 0 and len(input_paths) > 0:
            # Keep monthly coverage by sampling each input file instead of limiting after union.
            rows_per_input = max(1, (sample_rows + len(input_paths) - 1) // len(input_paths))
            sampled_per_input = True
            print(f"[spark] Applying per-file sample cap: {rows_per_input} rows x {len(input_paths)} files")
        for path in input_paths:
            df_part = reader.parquet(path)
            if rows_per_input is not None:
                df_part = df_part.limit(rows_per_input)
            if df_raw is None:
                df_raw = df_part
            else:
                df_raw = df_raw.unionByName(df_part, allowMissingColumns=True)
        if df_raw is None:
            raise FileNotFoundError("No parquet input files were found")
    else:
        df_raw = reader.parquet(input_paths)

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

    if sample_rows is not None and sample_rows > 0 and not sampled_per_input:
        df_final = df_final.limit(sample_rows)
        print(f"[spark] Applying local sample cap: {sample_rows} rows")

    print(f"[spark] Writing {df_final.count()} rows to {output_path}")

    try:
        (
            df_final
            .repartition(16)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
    except Py4JJavaError as exc:
        message = str(exc)
        if (
            (
                "NativeIO$Windows.access0" in message
                or "HADOOP_HOME and hadoop.home.dir are unset" in message
                or "getWinUtilsPath" in message
            )
            and not output_path.startswith("gs://")
        ):
            print("[spark] Native Hadoop Windows IO issue detected, using pyarrow fallback writer")
            write_with_pyarrow_fallback(df_final, output_path)
        else:
            raise

    print("[spark] Transform complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC TLC Spark transform")
    parser.add_argument("--gcs-bucket", default=None, help="GCS bucket name without gs:// prefix")
    parser.add_argument("--sample-rows", type=int, default=None, help="Optional sample row cap")
    args = parser.parse_args()

    gcs_bucket = args.gcs_bucket if args.gcs_bucket is not None else os.getenv("GCS_BUCKET", "")
    sample_rows: int | None = None

    if gcs_bucket:
        enable_gcs = True
        input_paths: str | list[str] = f"gs://{gcs_bucket}/raw/*.parquet"
        output_path = f"gs://{gcs_bucket}/processed/"
    else:
        enable_gcs = False
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_dir = Path(base) / "data" / "raw"
        local_files = sorted(str(p) for p in raw_dir.glob("*.parquet"))
        if not local_files:
            raise FileNotFoundError(f"No parquet files found in {raw_dir}")
        input_paths = local_files
        output_path = os.path.join(base, "data", "processed")
        if os.name == "nt":
            sample_rows = int(os.getenv("TLC_LOCAL_SAMPLE_ROWS", "200000"))

    if args.sample_rows is not None:
        sample_rows = args.sample_rows

    print(f"[spark] Input:  {input_paths}")
    print(f"[spark] Output: {output_path}")

    spark = get_spark(enable_gcs=enable_gcs)
    transform(spark, input_paths, output_path, sample_rows=sample_rows)
    spark.stop()


if __name__ == "__main__":
    main()

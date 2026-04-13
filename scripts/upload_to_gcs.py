"""
upload_to_gcs.py — Upload raw parquet files from data/raw/ to GCS data lake.

Uploads each parquet file to gs://<bucket>/raw/<filename>.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def upload_to_gcs() -> None:
    bucket_name = os.environ["GCS_BUCKET"]

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    parquet_files = sorted(RAW_DIR.glob("*.parquet"))
    if not parquet_files:
        print(f"[upload] No parquet files found in {RAW_DIR}")
        return

    for parquet_path in parquet_files:
        blob_name = f"raw/{parquet_path.name}"
        blob = bucket.blob(blob_name)

        if blob.exists():
            print(f"[upload] gs://{bucket_name}/{blob_name} already exists, skipping")
            continue

        print(f"[upload] Uploading {parquet_path.name} → gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(str(parquet_path), timeout=600)
        print(
            f"[upload] {parquet_path.name} uploaded ({parquet_path.stat().st_size / 1e9:.2f} GB)"
        )

    print(f"[upload] Done. {len(parquet_files)} files processed.")


if __name__ == "__main__":
    upload_to_gcs()

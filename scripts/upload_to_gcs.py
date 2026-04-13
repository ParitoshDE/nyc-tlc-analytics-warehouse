"""
upload_to_gcs.py — Upload raw CSV files from data/raw/ to GCS data lake.

Uploads each CSV to gs://<bucket>/raw/<filename>.
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

    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        print(f"[upload] No CSV files found in {RAW_DIR}")
        return

    for csv_path in csv_files:
        blob_name = f"raw/{csv_path.name}"
        blob = bucket.blob(blob_name)

        if blob.exists():
            print(f"[upload] gs://{bucket_name}/{blob_name} already exists, skipping")
            continue

        print(f"[upload] Uploading {csv_path.name} → gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(str(csv_path), timeout=600)
        print(f"[upload] {csv_path.name} uploaded ({csv_path.stat().st_size / 1e9:.2f} GB)")

    print(f"[upload] Done. {len(csv_files)} files processed.")


if __name__ == "__main__":
    upload_to_gcs()

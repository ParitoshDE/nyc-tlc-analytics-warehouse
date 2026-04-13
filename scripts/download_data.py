"""
download_data.py — Download eCommerce behavior dataset from Kaggle.

Uses the Kaggle API to download and extract CSVs to data/raw/.
Optionally downloads additional months from REES46 direct URLs.
"""
from __future__ import annotations

import gzip
import os
import shutil
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

KAGGLE_DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"

# Additional months hosted directly by REES46 (beyond Kaggle's Oct+Nov 2019)
REES46_URLS = {
    "2019-Dec.csv.gz": "https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz",
    "2020-Jan.csv.gz": "https://data.rees46.com/datasets/marketplace/2020-Jan.csv.gz",
    "2020-Feb.csv.gz": "https://data.rees46.com/datasets/marketplace/2020-Feb.csv.gz",
    "2020-Mar.csv.gz": "https://data.rees46.com/datasets/marketplace/2020-Mar.csv.gz",
    "2020-Apr.csv.gz": "https://data.rees46.com/datasets/marketplace/2020-Apr.csv.gz",
}


def download_kaggle() -> None:
    """Download Oct + Nov 2019 from Kaggle."""
    from kaggle.api.kaggle_api_extended import KaggleApi

    print(f"[download] Downloading {KAGGLE_DATASET} from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=str(RAW_DIR), unzip=True)
    print(f"[download] Kaggle files extracted to {RAW_DIR}")


def download_rees46(months: list[str] | None = None) -> None:
    """Download additional months from REES46 direct URLs.

    Args:
        months: List of filenames to download (e.g. ['2019-Dec.csv.gz']).
                 If None, downloads all available months.
    """
    targets = {k: v for k, v in REES46_URLS.items() if months is None or k in months}

    for filename, url in targets.items():
        gz_path = RAW_DIR / filename
        csv_path = RAW_DIR / filename.replace(".gz", "")

        if csv_path.exists():
            print(f"[download] {csv_path.name} already exists, skipping")
            continue

        print(f"[download] Downloading {filename} from REES46...")
        resp = requests.get(url, stream=True, timeout=600)
        resp.raise_for_status()

        with open(gz_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"[download] Extracting {filename}...")
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        gz_path.unlink()
        print(f"[download] {csv_path.name} ready ({csv_path.stat().st_size / 1e9:.2f} GB)")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Always download Kaggle data (Oct + Nov 2019)
    download_kaggle()

    # Optionally download extra months — set DOWNLOAD_EXTRA_MONTHS=true in .env
    if os.getenv("DOWNLOAD_EXTRA_MONTHS", "false").lower() == "true":
        download_rees46()
    else:
        print("[download] Skipping extra months (set DOWNLOAD_EXTRA_MONTHS=true to include)")

    csv_files = list(RAW_DIR.glob("*.csv"))
    print(f"[download] Done. {len(csv_files)} CSV files in {RAW_DIR}")


if __name__ == "__main__":
    main()

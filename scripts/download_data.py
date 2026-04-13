"""
download_data.py — Download NYC TLC trip parquet data.

Downloads monthly parquet files from the official NYC TLC data host.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def month_range(start_ym: str, end_ym: str) -> list[str]:
    start = datetime.strptime(start_ym, "%Y-%m")
    end = datetime.strptime(end_ym, "%Y-%m")
    months: list[str] = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=year, month=month)
    return months


def download_tlc_parquet(taxi_type: str, start_ym: str, end_ym: str) -> None:
    for ym in month_range(start_ym, end_ym):
        filename = f"{taxi_type}_tripdata_{ym}.parquet"
        url = f"{BASE_URL}/{filename}"
        out_path = RAW_DIR / filename

        if out_path.exists():
            print(f"[download] {filename} already exists, skipping")
            continue

        print(f"[download] Fetching {url}")
        resp = requests.get(url, stream=True, timeout=600)
        if resp.status_code != 200:
            print(f"[download] Missing {filename} (status={resp.status_code}), skipping")
            continue

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        print(f"[download] Saved {filename} ({out_path.stat().st_size / 1e9:.2f} GB)")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    taxi_type = os.getenv("TLC_TAXI_TYPE", "yellow")
    start_ym = os.getenv("TLC_START_MONTH", "2023-01")
    end_ym = os.getenv("TLC_END_MONTH", "2023-12")

    print(
        f"[download] Downloading NYC TLC data: type={taxi_type}, range={start_ym}..{end_ym}"
    )
    download_tlc_parquet(taxi_type=taxi_type, start_ym=start_ym, end_ym=end_ym)

    parquet_files = list(RAW_DIR.glob("*.parquet"))
    print(f"[download] Done. {len(parquet_files)} parquet files in {RAW_DIR}")


if __name__ == "__main__":
    main()

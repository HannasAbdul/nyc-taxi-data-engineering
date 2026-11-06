from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

TRIPS_FILE = ROOT / "data/raw/yellow_tripdata_2025-01.parquet"
ZONES_FILE = ROOT / "data/metadata/taxi_zone_lookup.csv"


def main():
    if not TRIPS_FILE.exists():
        raise FileNotFoundError(f"Trip file not found: {TRIPS_FILE}")

    if not ZONES_FILE.exists():
        raise FileNotFoundError(f"Zone file not found: {ZONES_FILE}")

    # Read Parquet metadata without loading every trip into memory.
    parquet = pq.ParquetFile(TRIPS_FILE)
    metadata = parquet.metadata

    print("\n=== TRIP FILE METADATA ===")
    print(f"Filename: {TRIPS_FILE.name}")
    print(f"Rows: {metadata.num_rows:,}")
    print(f"Row groups: {metadata.num_row_groups}")
    print(f"File size: {TRIPS_FILE.stat().st_size:,} bytes")

    print("\n=== TRIP SCHEMA ===")
    print(parquet.schema)

    # Read just five records from the first available row group.
    print("\n=== SAMPLE TRIPS ===")
    batch = next(parquet.iter_batches(batch_size=5), None)

    if batch is None:
        print("The Parquet file contains no rows.")
    else:
        print(batch.to_pandas().to_string(index=False))

    # Read a small sample of the zone lookup.
    print("\n=== TAXI ZONE LOOKUP ===")
    zones = pd.read_csv(ZONES_FILE, nrows=5)
    print(f"Columns: {list(zones.columns)}")
    print(zones.to_string(index=False))

    print("\nSource inspection completed.")


if __name__ == "__main__":
    main()

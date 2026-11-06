
from pathlib import Path

import pyarrow.parquet as pq


file_path = Path(
    "data/raw/yellow_tripdata_2025-01_download_test.parquet"
)

# Check whether the file exists.
if not file_path.exists():
    raise FileNotFoundError(f"File not found: {file_path}")

# Check whether the file is empty.
if file_path.stat().st_size == 0:
    raise ValueError("Downloaded file is empty.")

# Read Parquet metadata.
parquet_file = pq.ParquetFile(file_path)

# Display file information.
print("File:", file_path.name)
print("File size:", file_path.stat().st_size, "bytes")
print("Number of rows:", parquet_file.metadata.num_rows)
print("Number of columns:", parquet_file.metadata.num_columns)
print("Number of row groups:", parquet_file.metadata.num_row_groups)

# Display the column names.
print("\nColumns:")
print(parquet_file.schema.names)

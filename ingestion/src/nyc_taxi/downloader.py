
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib
import logging
import re


from ingestion.src.nyc_taxi.validator import validate_taxi_zones_csv
from ingestion.src.nyc_taxi.logging_config import setup_logging

from ingestion.src.nyc_taxi.config import (
    METADATA_DIR,
    RAW_DATA_DIR,
    YELLOW_TAXI_URL,
    TAXI_ZONE_URL
)
setup_logging()

logger = logging.getLogger(__name__)




def create_http_session() -> requests.Session:
    """
    Create an HTTP session with automatic retry support.
    """

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()

    session.mount("https://", adapter)

    return session


def calculate_sha256(file_path: Path) -> str:
    """
    Calculate the SHA-256 checksum of a file.
    """

    # Create a SHA-256 hashing object.
    sha256_hash = hashlib.sha256()

    # Open the file in binary read mode.
    with open(file_path, "rb") as file:

        # Read the file in 1 MB chunks.
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):

            # Add each chunk to the hash calculation.
            sha256_hash.update(chunk)

    # Return the final hexadecimal checksum.
    return sha256_hash.hexdigest()



def save_checksum(file_path: Path, checksum: str) -> None:
    """Atomically save one checksum per normalized absolute file path.

    Conflicting duplicate records are rejected instead of silently allowing
    whichever duplicate happened to appear last in the metadata file.
    """
    if not re.fullmatch(r"[0-9a-fA-F]{64}", checksum):
        raise ValueError("SHA-256 checksum must contain exactly 64 hexadecimal characters.")

    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    checksum_file = METADATA_DIR / "source-checksums.sha256"
    file_path = file_path.resolve()
    records = {}

    if checksum_file.exists():
        with checksum_file.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                parts = line.strip().split(maxsplit=1)
                if not parts:
                    continue
                if len(parts) != 2:
                    raise ValueError(
                        f"Malformed checksum record at line {line_number}: {line.rstrip()}"
                    )
                existing_checksum, existing_path = parts
                normalized_path = str(Path(existing_path).expanduser().resolve())
                previous = records.get(normalized_path)
                if previous is not None and previous != existing_checksum:
                    raise ValueError(
                        "Conflicting checksum records exist for "
                        f"{normalized_path}; repair {checksum_file} before continuing."
                    )
                records[normalized_path] = existing_checksum

    path_key = str(file_path)
    if records.get(path_key) == checksum:
        logger.info("Checksum already recorded for %s", file_path.name)
        return

    if path_key in records:
        logger.info("Checksum changed for %s; updating metadata", file_path.name)
    else:
        logger.info("Recording checksum for new file %s", file_path.name)

    records[path_key] = checksum
    temporary = checksum_file.with_suffix(checksum_file.suffix + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            for recorded_path, recorded_checksum in sorted(records.items()):
                handle.write(f"{recorded_checksum}  {recorded_path}\n")
        temporary.replace(checksum_file)
    finally:
        if temporary.exists():
            temporary.unlink()

    logger.info("Checksum metadata updated: %s", checksum_file)



def validate_parquet(file_path: Path) -> None:
    """
    Validate that a file is a readable, non-empty Parquet dataset.
    """

    # Check that the file exists.
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Check that the file is not empty.
    if file_path.stat().st_size == 0:
        raise ValueError("Parquet file is empty.")

    # Import only when Parquet validation is requested. This keeps
    # checksum-only utilities and their unit tests lightweight.
    try:
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(file_path)

    except Exception as exc:
        raise ValueError(
            f"File is not a valid Parquet file: {file_path}"
        ) from exc

    # Retrieve the file metadata.
    metadata = parquet_file.metadata

    # Check that the dataset contains records.
    if metadata.num_rows == 0:
        raise ValueError("Parquet file contains no records.")

    # Define important columns expected in NYC Yellow Taxi data.
    required_columns = {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "total_amount",
    }

    # Retrieve the actual column names.
    actual_columns = set(parquet_file.schema.names)

    # Identify any required columns that are missing.
    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    print("Parquet validation successful.")
    print("Rows:", metadata.num_rows)
    print("Columns:", metadata.num_columns)
    print("Row groups:", metadata.num_row_groups)





def download_file(url: str, destination: Path) -> None:
    """
    Download a file safely using a temporary file.

    The final filename is only created after the download succeeds.
    """

    # Create the destination directory if necessary.
    destination.parent.mkdir(parents=True, exist_ok=True)

    # Create a temporary filename.
    temporary_file = destination.with_suffix(
        destination.suffix + ".part"
    )

    # Create our retry-enabled HTTP session.
    session = create_http_session()
    logger.info("Starting download from %s", url)

    try:
        # Download the file using the configured session.
        with session.get(
            url,
            stream=True,
            timeout=60
        ) as response:

            response.raise_for_status()

            # Write the download into the temporary file.
            with open(temporary_file, "wb") as file:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if chunk:
                        file.write(chunk)


        # Validate the downloaded Parquet file before publishing it.
        validate_parquet(temporary_file)
        # Publish the file only after downloading succeeds.
        temporary_file.replace(destination)
        # Calculate the checksum of the published file.
        checksum = calculate_sha256(destination)
        # Save the checksum in metadata.
        save_checksum(destination, checksum)

        logger.info("Download completed: %s", destination)
        logger.info("SHA-256: %s", checksum)



    except Exception:
        logger.exception("Download or validation failed for %s", destination)

        if temporary_file.exists():
            temporary_file.unlink()

        raise

    finally:

        # Close the HTTP session.
        session.close()

def download_csv_file(url: str, destination: Path) -> None:
    """
    Download and validate a CSV file safely.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = destination.with_suffix(
        destination.suffix + ".part"
    )

    session = create_http_session()

    try:
        logger.info("Starting CSV download from %s", url)

        with session.get(
            url,
            stream=True,
            timeout=60
        ) as response:

            response.raise_for_status()

            with open(temporary_file, "wb") as file:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        # Validate before publishing the file.
        validate_taxi_zones_csv(temporary_file)

        # Publish the validated file.
        temporary_file.replace(destination)

        # Calculate and save checksum.
        checksum = calculate_sha256(destination)

        save_checksum(destination, checksum)

        logger.info("CSV download completed: %s", destination)
        logger.info("SHA-256: %s", checksum)

    except Exception:
        logger.exception(
            "CSV download or validation failed for %s",
            destination,
        )

        if temporary_file.exists():
            temporary_file.unlink()

        raise

    finally:
        session.close()



def download_source_data() -> None:
    """
    Download all required NYC Taxi source datasets.
    """

    logger.info("Starting NYC Taxi source data ingestion.")

    yellow_taxi_destination = (
        RAW_DATA_DIR / "yellow_tripdata_2025-01.parquet"
    )

    taxi_zone_destination = (
        RAW_DATA_DIR / "taxi_zone_lookup.csv"
    )

    download_file(
        YELLOW_TAXI_URL,
        yellow_taxi_destination,
    )

    download_csv_file(
        TAXI_ZONE_URL,
        taxi_zone_destination,
    )

    logger.info("All source datasets downloaded successfully.")


if __name__ == "__main__":
    download_source_data()
'''if __name__ == "__main__":

    # Use a separate filename so we don't overwrite your existing file.
    destination = RAW_DATA_DIR / "yellow_tripdata_2025-01_download_test.parquet"

    download_file(YELLOW_TAXI_URL, destination)'''






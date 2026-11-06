import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def validate_taxi_zones_csv(file_path: Path) -> None:
    """
    Validate the NYC Taxi Zone Lookup CSV file.
    """

    # Check whether the file exists.
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Check whether the file is empty.
    if file_path.stat().st_size == 0:
        raise ValueError("Taxi zone CSV file is empty.")

    # Read the CSV file.
    try:
        dataframe = pd.read_csv(file_path)
    except Exception as exc:
        raise ValueError(
            f"Unable to read CSV file: {file_path}"
        ) from exc

    # Define the columns we expect.
    required_columns = {
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    }

    # Compare expected columns with actual columns.
    actual_columns = set(dataframe.columns)

    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    # Check that the file contains records.
    if dataframe.empty:
        raise ValueError("Taxi zone CSV contains no records.")

    # Check that LocationID is not null.
    if dataframe["LocationID"].isnull().any():
        raise ValueError("LocationID contains null values.")

    # Check that LocationID is unique.
    if dataframe["LocationID"].duplicated().any():
        raise ValueError("Duplicate LocationID values found.")

    # Check that LocationID contains numeric values.
    if not pd.api.types.is_numeric_dtype(dataframe["LocationID"]):
        raise ValueError("LocationID must contain numeric values.")

    logger.info(
        "Taxi zone CSV validation successful. Rows=%s, Columns=%s",
        len(dataframe),
        len(dataframe.columns),
    )

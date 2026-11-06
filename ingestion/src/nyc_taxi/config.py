"""Central configuration for the NYC Taxi ingestion project."""

import os
from pathlib import Path

from dotenv import load_dotenv

# config.py is located at ingestion/src/nyc_taxi/config.py.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
METADATA_DIR = DATA_DIR / "metadata"
LOG_DIR = PROJECT_ROOT / "logs"

ALLOW_FIRST_LOAD = os.getenv("INGESTION_ALLOW_FIRST_LOAD", "false").strip().lower() in {"1", "true", "yes"}

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "NYC_TAXI_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "NYC_TAXI_DW"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

YELLOW_TAXI_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2025-01.parquet"
)
TAXI_ZONE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "misc/taxi_zone_lookup.csv"
)


def validate_snowflake_config() -> None:
    """Fail with a clear message if mandatory credentials are missing."""
    required = ("account", "user", "password")
    missing = [key for key in required if not SNOWFLAKE_CONFIG.get(key)]
    if missing:
        env_names = ", ".join(f"SNOWFLAKE_{key.upper()}" for key in missing)
        raise ValueError(f"Missing required Snowflake settings: {env_names}")

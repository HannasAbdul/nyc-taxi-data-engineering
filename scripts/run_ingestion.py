"""Command-line entry point for the NYC Taxi Snowflake ingestion pipeline."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.src.nyc_taxi.snowflake_loader import run_ingestion_pipeline  # noqa: E402


if __name__ == "__main__":
    result = run_ingestion_pipeline()
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") != "SUCCESS":
        sys.exit(1)

"""Reliable loading of NYC Taxi source files into Snowflake RAW tables."""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from pathlib import Path

import snowflake.connector

from ingestion.src.nyc_taxi.config import (
    ALLOW_FIRST_LOAD,
    RAW_DATA_DIR,
    SNOWFLAKE_CONFIG,
    validate_snowflake_config,
)
from ingestion.src.nyc_taxi.logging_config import setup_logging
from ingestion.src.nyc_taxi.manifest import (
    get_manifest_record,
    register_manifest,
    update_manifest_status,
)

setup_logging()
logger = logging.getLogger(__name__)

DATABASE = "NYC_TAXI_DW"
SCHEMA = "RAW"
STAGE = f"{DATABASE}.{SCHEMA}.TAXI_TRIP_STAGE"
AUDIT_TABLE = f"{DATABASE}.{SCHEMA}.INGESTION_AUDIT"

DATASETS = {
    "taxi_zones": {
        "source_file": "taxi_zone_lookup.csv",
        "target_table": "TAXI_ZONES",
        "file_format": f"{DATABASE}.{SCHEMA}.TAXI_CSV_FORMAT",
        "pattern": ".*taxi_zone_lookup[.]csv",
        "kind": "csv",
    },
    "yellow_trips": {
        "source_file": "yellow_tripdata_2025-01.parquet",
        "target_table": "YELLOW_TRIPS",
        "file_format": f"{DATABASE}.{SCHEMA}.TAXI_PARQUET_FORMAT_LOGICAL",
        "pattern": ".*yellow_tripdata_2025-01[.]parquet",
        "kind": "parquet",
    },
}


def get_snowflake_connection():
    """Connect to Snowflake and activate the configured warehouse."""
    validate_snowflake_config()
    warehouse = SNOWFLAKE_CONFIG.get("warehouse")
    if not warehouse or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", warehouse):
        raise ValueError("SNOWFLAKE_WAREHOUSE must be a valid unquoted Snowflake identifier.")

    logger.info("Connecting to Snowflake.")
    connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        execute_query(connection, f"USE WAREHOUSE {warehouse}")
    except Exception:
        connection.close()
        logger.exception("Failed to activate the Snowflake warehouse.")
        raise
    logger.info("Connected to Snowflake.")
    return connection


def execute_query(connection, sql: str, params=None) -> list:
    """Execute SQL, return rows for queries, and always close the cursor."""
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description is not None else []
    except Exception:
        logger.exception("Snowflake SQL execution failed.")
        raise
    finally:
        cursor.close()


def calculate_sha256(file_path: Path) -> str:
    """Return a streaming SHA-256 checksum for a local file."""
    digest = hashlib.sha256()
    with file_path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_stage_name(stage_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.$]+", stage_name):
        raise ValueError(f"Invalid Snowflake stage identifier: {stage_name}")


def upload_file_to_stage(
    connection,
    file_path: Path,
    stage_name: str = STAGE,
    stage_subpath: str | None = None,
) -> str:
    """PUT a local file into a unique folder in the Snowflake internal stage."""
    file_path = Path(file_path).resolve()
    if not file_path.is_file():
        raise FileNotFoundError(f"Source file does not exist: {file_path}")
    _validate_stage_name(stage_name)

    if stage_subpath is not None and not re.fullmatch(r"[A-Za-z0-9_-]+", stage_subpath):
        raise ValueError("Stage subpath may contain only letters, digits, '_' and '-'.")

    destination = f"@{stage_name}"
    if stage_subpath:
        destination += f"/{stage_subpath}"

    # Path.as_uri() correctly quotes spaces and special characters in local paths.
    sql = (
        f"PUT '{file_path.as_uri()}' {destination} "
        "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
    )
    logger.info("Uploading %s to %s", file_path.name, destination)
    execute_query(connection, sql)
    return destination


def is_file_already_loaded(connection, source_file: str, target_table: str) -> bool:
    """Check legacy audit records created before checksum manifests existed."""
    sql = f"""
        SELECT COUNT(*)
        FROM {AUDIT_TABLE}
        WHERE SOURCE_FILE = %s
          AND TARGET_TABLE = %s
          AND LOAD_STATUS = 'LOADED'
    """
    rows = execute_query(connection, sql, (source_file, target_table))
    return bool(rows and int(rows[0][0] or 0) > 0)


def get_successful_file_load_history(
    connection,
    source_file: str,
    target_table: str,
    batch_id: str | None = None,
) -> dict | None:
    """Find recent successful COPY evidence (LOAD_HISTORY covers 14 days)."""
    sql = f"""
        SELECT FILE_NAME, TABLE_NAME, LAST_LOAD_TIME, STATUS,
               ROW_COUNT, ERROR_COUNT
        FROM {DATABASE}.INFORMATION_SCHEMA.LOAD_HISTORY
        WHERE SCHEMA_NAME = %s
          AND TABLE_NAME = %s
          AND FILE_NAME ILIKE %s
          AND STATUS = 'LOADED'
          AND ERROR_COUNT = 0
          AND LAST_LOAD_TIME >= DATEADD(DAY, -14, CURRENT_TIMESTAMP())
        ORDER BY LAST_LOAD_TIME DESC
        LIMIT 1
    """
    rows = execute_query(
        connection,
        sql,
        (
            SCHEMA,
            target_table,
            f"%{batch_id}/{source_file}" if batch_id else f"%{source_file}",
        ),
    )
    if not rows:
        # LOAD_HISTORY is limited to the most recent 14 days. Fall back to
        # ACCOUNT_USAGE for longer-lived evidence (up to 365 days; visibility
        # can be delayed and the executing role needs the required privileges).
        account_usage_sql = f"""
            SELECT FILE_NAME, TABLE_NAME, LAST_LOAD_TIME, STATUS,
                   ROW_COUNT, ERROR_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE TABLE_SCHEMA_NAME = %s
              AND TABLE_NAME = %s
              AND FILE_NAME ILIKE %s
              AND UPPER(STATUS) = 'LOADED'
              AND ERROR_COUNT = 0
              AND LAST_LOAD_TIME >= DATEADD(DAY, -365, CURRENT_TIMESTAMP())
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT 1
        """
        try:
            rows = execute_query(
                connection,
                account_usage_sql,
                (
                    SCHEMA,
                    target_table,
                    f"%{batch_id}/{source_file}" if batch_id else f"%{source_file}",
                ),
            )
        except Exception:
            logger.warning(
                "Could not query SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY. "
                "The role may lack privileges or the view may be unavailable.",
                exc_info=True,
            )
            return None

    if not rows:
        return None
    row = rows[0]
    return {
        "file_name": row[0],
        "target_table": row[1],
        "source_load_time": row[2],
        "status": row[3],
        "rows_loaded": int(row[4] or 0),
        "error_count": int(row[5] or 0),
    }


def record_ingestion_audit(
    connection,
    source_file: str,
    target_table: str,
    batch_id: str | None,
    load_status: str,
    rows_loaded: int = 0,
    error_message: str | None = None,
    source_load_time=None,
) -> None:
    """Append one attempt outcome to INGESTION_AUDIT."""
    sql = f"""
        INSERT INTO {AUDIT_TABLE}
        (SOURCE_FILE, TARGET_TABLE, BATCH_ID, LOAD_STATUS,
         ROWS_LOADED, ERROR_MESSAGE, SOURCE_LOAD_TIME)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    execute_query(
        connection,
        sql,
        (
            source_file,
            target_table,
            batch_id,
            load_status,
            int(rows_loaded or 0),
            error_message[:4000] if error_message else None,
            source_load_time,
        ),
    )


def _copy_taxi_zones(connection, batch_id: str, stage_path: str) -> list:
    sql = f"""
        COPY INTO {DATABASE}.{SCHEMA}.TAXI_ZONES
        (LOCATIONID, BOROUGH, ZONE, SERVICE_ZONE, SOURCE_FILE, BATCH_ID)
        FROM (
            SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR, $4::VARCHAR,
                   REGEXP_SUBSTR(METADATA$FILENAME, '[^/]+$'), '{batch_id}'
            FROM {stage_path}
        )
        FILE_FORMAT = (FORMAT_NAME = '{DATASETS['taxi_zones']['file_format']}')
        PATTERN = '{DATASETS['taxi_zones']['pattern']}'
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE
    """
    return execute_query(connection, sql)


def _copy_yellow_trips(connection, batch_id: str, stage_path: str) -> list:
    sql = f"""
        COPY INTO {DATABASE}.{SCHEMA}.YELLOW_TRIPS
        (
            VENDORID, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME,
            PASSENGER_COUNT, TRIP_DISTANCE, RATECODEID, STORE_AND_FWD_FLAG,
            PULOCATIONID, DOLOCATIONID, PAYMENT_TYPE, FARE_AMOUNT, EXTRA,
            MTA_TAX, TIP_AMOUNT, TOLLS_AMOUNT, IMPROVEMENT_SURCHARGE,
            TOTAL_AMOUNT, CONGESTION_SURCHARGE, AIRPORT_FEE,
            CBD_CONGESTION_FEE, SOURCE_FILE, BATCH_ID
        )
        FROM (
            SELECT
                $1:VendorID::NUMBER,
                $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
                $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
                $1:passenger_count::NUMBER,
                $1:trip_distance::FLOAT,
                $1:RatecodeID::NUMBER,
                $1:store_and_fwd_flag::VARCHAR,
                $1:PULocationID::NUMBER,
                $1:DOLocationID::NUMBER,
                $1:payment_type::NUMBER,
                $1:fare_amount::FLOAT,
                $1:extra::FLOAT,
                $1:mta_tax::FLOAT,
                $1:tip_amount::FLOAT,
                $1:tolls_amount::FLOAT,
                $1:improvement_surcharge::FLOAT,
                $1:total_amount::FLOAT,
                $1:congestion_surcharge::FLOAT,
                $1:Airport_fee::FLOAT,
                $1:cbd_congestion_fee::FLOAT,
                REGEXP_SUBSTR(METADATA$FILENAME, '[^/]+$'),
                '{batch_id}'
            FROM {stage_path}
        )
        FILE_FORMAT = (FORMAT_NAME = '{DATASETS['yellow_trips']['file_format']}')
        PATTERN = '{DATASETS['yellow_trips']['pattern']}'
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE
    """
    return execute_query(connection, sql)


def _rows_loaded(copy_rows: list) -> int:
    """Snowflake COPY result column 4 (zero-based index 3) is ROWS_LOADED."""
    return sum(int(row[3] or 0) for row in copy_rows)


def _load_dataset(connection, dataset_key: str) -> dict:
    """Run one dataset through manifest, PUT, COPY, and audit handling."""
    spec = DATASETS[dataset_key]
    source_file = spec["source_file"]
    target_table = spec["target_table"]
    local_file = RAW_DATA_DIR / source_file

    if not local_file.is_file():
        raise FileNotFoundError(f"Required source file is missing: {local_file}")

    checksum = calculate_sha256(local_file)
    batch_id = str(uuid.uuid4())
    copy_started = False

    manifest = get_manifest_record(
        connection, source_file, checksum, target_table
    )

    if manifest and manifest["status"] == "LOADED":
        # A previous run may have completed COPY and manifest update but
        # failed before writing the audit row. Repair that gap on rerun.
        if not is_file_already_loaded(connection, source_file, target_table):
            try:
                record_ingestion_audit(
                    connection,
                    source_file,
                    target_table,
                    manifest.get("batch_id"),
                    "LOADED",
                    rows_loaded=manifest["rows_loaded"],
                )
            except Exception as exc:
                logger.exception("Manifest says LOADED but audit repair failed.")
                return {
                    "status": "AUDIT_FAILED",
                    "copy_succeeded": True,
                    "source_file": source_file,
                    "target_table": target_table,
                    "file_checksum": checksum,
                    "rows_loaded": manifest["rows_loaded"],
                    "batch_id": manifest.get("batch_id"),
                    "error": str(exc),
                }
            return {
                "status": "RECONCILED",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "rows_loaded": manifest["rows_loaded"],
                "batch_id": manifest.get("batch_id"),
            }
        return {
            "status": "SKIPPED",
            "reason": "MANIFEST_ALREADY_LOADED",
            "source_file": source_file,
            "target_table": target_table,
            "file_checksum": checksum,
            "rows_loaded": manifest["rows_loaded"],
            "batch_id": manifest["batch_id"],
        }

    if manifest and manifest["status"] in {"LOADING", "UNKNOWN"}:
        history = get_successful_file_load_history(
            connection, source_file, target_table, manifest.get("batch_id")
        )
        if history:
            update_manifest_status(
                connection,
                source_file,
                checksum,
                target_table,
                "LOADED",
                batch_id=manifest.get("batch_id"),
                rows_loaded=history["rows_loaded"],
                completed=True,
            )
            if not is_file_already_loaded(connection, source_file, target_table):
                record_ingestion_audit(
                    connection,
                    source_file,
                    target_table,
                    manifest.get("batch_id"),
                    "LOADED",
                    history["rows_loaded"],
                    source_load_time=history["source_load_time"],
                )
            return {
                "status": "RECONCILED",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "rows_loaded": history["rows_loaded"],
                "batch_id": manifest.get("batch_id"),
            }
        update_manifest_status(
            connection, source_file, checksum, target_table,
            "UNKNOWN", batch_id=manifest.get("batch_id"),
            error_message=(
                "Prior attempt has no confirmed successful load in available history; "
                "automatic retry blocked."
            ),
        )
        return {
            "status": "LOAD_STATUS_UNKNOWN",
            "source_file": source_file,
            "target_table": target_table,
            "file_checksum": checksum,
            "rows_loaded": 0,
            "batch_id": manifest.get("batch_id"),
            "error": (
                "A prior attempt may have reached Snowflake. No successful "
                "load evidence was found; automatic retry blocked."
            ),
        }

    if manifest is None:
        # Existing audit records pre-date checksum manifests. Do not reload
        # these known historical loads automatically: the audit lacks a hash.
        if is_file_already_loaded(connection, source_file, target_table):
            return {
                "status": "SKIPPED",
                "reason": "LEGACY_AUDIT_ALREADY_LOADED",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "rows_loaded": 0,
                "batch_id": None,
            }

        # Load history is keyed by staged filename, not by our local SHA-256.
        # If history exists but no manifest/audit checksum exists, we cannot
        # prove it represents today's exact file version; block for review.
        historical_load = get_successful_file_load_history(
            connection, source_file, target_table
        )
        if historical_load:
            return {
                "status": "LOAD_STATUS_UNKNOWN",
                "reason": "HISTORICAL_LOAD_WITHOUT_CHECKSUM",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "rows_loaded": historical_load["rows_loaded"],
                "batch_id": None,
                "error": (
                    "Snowflake has successful load history for this filename, "
                    "but the historical record has no local SHA-256 checksum. "
                    "The current file version cannot be matched automatically. "
                    "Reconcile it manually before loading."
                ),
            }

        if not ALLOW_FIRST_LOAD:
            return {
                "status": "FIRST_LOAD_REQUIRES_APPROVAL",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "rows_loaded": 0,
                "batch_id": None,
                "error": (
                    "No manifest, legacy audit, or successful load-history record exists. "
                    "Set INGESTION_ALLOW_FIRST_LOAD=true for an intentional first load, "
                    "then set it back to false."
                ),
            }
        register_manifest(connection, source_file, checksum, target_table)
        manifest = get_manifest_record(
            connection, source_file, checksum, target_table
        )
        if manifest is None:
            raise RuntimeError("Manifest registration did not create a readable row.")

    if manifest["status"] == "FAILED":
        update_manifest_status(
            connection, source_file, checksum, target_table, "REGISTERED"
        )

    if manifest["status"] not in {"REGISTERED", "FAILED"}:
        return {
            "status": "LOAD_STATUS_UNKNOWN",
            "source_file": source_file,
            "target_table": target_table,
            "file_checksum": checksum,
            "rows_loaded": 0,
            "error": f"Unexpected manifest state: {manifest['status']}",
        }

    try:
        stage_path = upload_file_to_stage(
            connection,
            local_file,
            STAGE,
            stage_subpath=batch_id,
        )

        update_manifest_status(
            connection,
            source_file,
            checksum,
            target_table,
            "LOADING",
            batch_id=batch_id,
        )

        # Once COPY is submitted, a lost connection can make its outcome
        # ambiguous. In that case we use UNKNOWN and require reconciliation.
        copy_started = True
        if spec["kind"] == "csv":
            copy_rows = _copy_taxi_zones(connection, batch_id, stage_path)
        else:
            copy_rows = _copy_yellow_trips(connection, batch_id, stage_path)
        rows_loaded = _rows_loaded(copy_rows)

    except Exception as exc:
        state = "UNKNOWN" if copy_started else "FAILED"
        try:
            update_manifest_status(
                connection,
                source_file,
                checksum,
                target_table,
                state,
                batch_id=batch_id,
                error_message=str(exc),
                completed=True,
            )
            record_ingestion_audit(
                connection,
                source_file,
                target_table,
                batch_id,
                state,
                error_message=str(exc),
            )
        except Exception:
            logger.exception("Could not persist failure state for %s", source_file)
        logger.exception("Ingestion failed for %s; manifest state is %s", source_file, state)
        if state == "UNKNOWN":
            return {
                "status": "LOAD_STATUS_UNKNOWN",
                "source_file": source_file,
                "target_table": target_table,
                "file_checksum": checksum,
                "batch_id": batch_id,
                "rows_loaded": 0,
                "error": str(exc),
            }
        raise

    # The data load succeeded. Persist manifest success before audit history.
    try:
        update_manifest_status(
            connection,
            source_file,
            checksum,
            target_table,
            "LOADED",
            batch_id=batch_id,
            rows_loaded=rows_loaded,
            completed=True,
        )
    except Exception as exc:
        logger.exception("COPY succeeded but manifest update failed for %s", source_file)
        return {
            "status": "AUDIT_FAILED",
            "copy_succeeded": True,
            "source_file": source_file,
            "target_table": target_table,
            "file_checksum": checksum,
            "batch_id": batch_id,
            "rows_loaded": rows_loaded,
            "error": f"Manifest update failed after COPY: {exc}",
        }

    try:
        record_ingestion_audit(
            connection,
            source_file,
            target_table,
            batch_id,
            "LOADED",
            rows_loaded=rows_loaded,
        )
    except Exception as exc:
        logger.exception("COPY and manifest succeeded, but audit insert failed.")
        return {
            "status": "AUDIT_FAILED",
            "copy_succeeded": True,
            "source_file": source_file,
            "target_table": target_table,
            "file_checksum": checksum,
            "batch_id": batch_id,
            "rows_loaded": rows_loaded,
            "error": str(exc),
        }

    return {
        "status": "LOADED",
        "copy_succeeded": True,
        "source_file": source_file,
        "target_table": target_table,
        "file_checksum": checksum,
        "batch_id": batch_id,
        "rows_loaded": rows_loaded,
    }


def load_taxi_zones(connection) -> dict:
    return _load_dataset(connection, "taxi_zones")


def load_yellow_trips(connection) -> dict:
    return _load_dataset(connection, "yellow_trips")


def run_ingestion_pipeline() -> dict:
    """Run both independent loads on one connection and return aggregate status."""
    logger.info("Starting NYC Taxi ingestion pipeline.")
    connection = get_snowflake_connection()
    results = {}
    try:
        for key, loader in (
            ("taxi_zones", load_taxi_zones),
            ("yellow_trips", load_yellow_trips),
        ):
            try:
                results[key] = loader(connection)
            except Exception as exc:
                logger.exception("Dataset ingestion failed: %s", key)
                results[key] = {"status": "FAILED", "error": str(exc)}

        statuses = [item["status"] for item in results.values()]
        bad = {"FAILED", "AUDIT_FAILED", "LOAD_STATUS_UNKNOWN", "FIRST_LOAD_REQUIRES_APPROVAL"}
        bad_count = sum(status in bad for status in statuses)
        if bad_count == len(statuses):
            overall = "FAILED"
        elif bad_count:
            overall = "PARTIAL_SUCCESS"
        else:
            overall = "SUCCESS"
        return {"status": overall, **results}
    finally:
        connection.close()
        logger.info("Snowflake connection closed.")

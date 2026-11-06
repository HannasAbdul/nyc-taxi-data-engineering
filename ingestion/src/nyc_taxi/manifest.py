"""Snowflake-backed manifest operations for idempotent file ingestion.

A manifest row represents one source-file version and destination:
(source filename, SHA-256 checksum, target table).
The manifest stores current state; INGESTION_AUDIT stores attempt history.
"""

from __future__ import annotations

from typing import Any

def _execute(connection, sql: str, params=None):
    """Execute SQL without importing the loader (avoids circular imports)."""
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description is not None else []
    finally:
        cursor.close()

MANIFEST_TABLE = "NYC_TAXI_DW.RAW.INGESTION_MANIFEST"

VALID_STATUSES = {"REGISTERED", "LOADING", "LOADED", "FAILED", "UNKNOWN"}


def get_manifest_record(
    connection,
    source_file: str,
    file_checksum: str,
    target_table: str,
) -> dict[str, Any] | None:
    """Return the manifest state for one exact file version and target."""
    sql = f"""
        SELECT STATUS, BATCH_ID, ROWS_LOADED, ERROR_MESSAGE
        FROM {MANIFEST_TABLE}
        WHERE SOURCE_FILE = %s
          AND FILE_CHECKSUM = %s
          AND TARGET_TABLE = %s
        ORDER BY UPDATED_AT DESC
        LIMIT 1
    """
    rows = _execute(
        connection, sql, (source_file, file_checksum, target_table)
    )
    if not rows:
        return None

    row = rows[0]
    return {
        "status": row[0],
        "batch_id": row[1],
        "rows_loaded": int(row[2] or 0),
        "error_message": row[3],
    }


def register_manifest(
    connection,
    source_file: str,
    file_checksum: str,
    target_table: str,
) -> None:
    """Register a new file version without resetting an existing state.

    Snowflake standard-table constraints are not enforced. This MERGE is
    safe for a single active ingestion run; orchestration must prevent
    concurrent runs for the same source/target.
    """
    sql = f"""
        MERGE INTO {MANIFEST_TABLE} AS target
        USING (
            SELECT %s AS SOURCE_FILE,
                   %s AS FILE_CHECKSUM,
                   %s AS TARGET_TABLE
        ) AS source
          ON target.SOURCE_FILE = source.SOURCE_FILE
         AND target.FILE_CHECKSUM = source.FILE_CHECKSUM
         AND target.TARGET_TABLE = source.TARGET_TABLE
        WHEN NOT MATCHED THEN INSERT (
            SOURCE_FILE, FILE_CHECKSUM, TARGET_TABLE, STATUS,
            ROWS_LOADED, REGISTERED_AT, UPDATED_AT
        ) VALUES (
            source.SOURCE_FILE, source.FILE_CHECKSUM, source.TARGET_TABLE,
            'REGISTERED', 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
    """
    _execute(
        connection, sql, (source_file, file_checksum, target_table)
    )


def update_manifest_status(
    connection,
    source_file: str,
    file_checksum: str,
    target_table: str,
    status: str,
    *,
    batch_id: str | None = None,
    rows_loaded: int = 0,
    error_message: str | None = None,
    completed: bool = False,
) -> None:
    """Update the current state for a file version and destination."""
    normalized_status = status.upper()
    if normalized_status not in VALID_STATUSES:
        raise ValueError(f"Invalid manifest status: {status}")

    sql = f"""
        UPDATE {MANIFEST_TABLE}
        SET STATUS = %s,
            BATCH_ID = COALESCE(%s, BATCH_ID),
            ROWS_LOADED = %s,
            ERROR_MESSAGE = %s,
            UPDATED_AT = CURRENT_TIMESTAMP(),
            COMPLETED_AT = CASE
                WHEN %s THEN CURRENT_TIMESTAMP()
                WHEN %s IN ('REGISTERED', 'LOADING') THEN NULL
                ELSE COMPLETED_AT
            END
        WHERE SOURCE_FILE = %s
          AND FILE_CHECKSUM = %s
          AND TARGET_TABLE = %s
    """
    _execute(
        connection,
        sql,
        (
            normalized_status,
            batch_id,
            int(rows_loaded or 0),
            error_message[:4000] if error_message else None,
            completed,
            normalized_status,
            source_file,
            file_checksum,
            target_table,
        ),
    )

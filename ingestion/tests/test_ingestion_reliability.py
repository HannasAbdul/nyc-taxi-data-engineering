import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.src.nyc_taxi import snowflake_loader as loader


class TestPipelineRunner(unittest.TestCase):
    def test_pipeline_runs_both_loaders_and_closes_connection(self):
        connection = unittest.mock.Mock()
        with patch.object(loader, "get_snowflake_connection", return_value=connection), \
             patch.object(loader, "load_taxi_zones", return_value={"status": "SKIPPED"}), \
             patch.object(loader, "load_yellow_trips", return_value={"status": "SKIPPED"}):
            result = loader.run_ingestion_pipeline()
        self.assertEqual(result["status"], "SUCCESS")
        connection.close.assert_called_once()

    def test_pipeline_marks_partial_success_for_unknown_load(self):
        connection = unittest.mock.Mock()
        with patch.object(loader, "get_snowflake_connection", return_value=connection), \
             patch.object(loader, "load_taxi_zones", return_value={"status": "LOAD_STATUS_UNKNOWN"}), \
             patch.object(loader, "load_yellow_trips", return_value={"status": "SKIPPED"}):
            result = loader.run_ingestion_pipeline()
        self.assertEqual(result["status"], "PARTIAL_SUCCESS")
        connection.close.assert_called_once()


class TestDatasetLoading(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.raw_dir = Path(self.temp_dir.name)
        (self.raw_dir / "taxi_zone_lookup.csv").write_text(
            "LocationID,Borough,Zone,service_zone\n1,Manhattan,Test,Yellow\n",
            encoding="utf-8",
        )
        (self.raw_dir / "yellow_tripdata_2025-01.parquet").write_bytes(b"test-parquet-bytes")
        self.connection = object()
        self.manifest = {"status": "REGISTERED", "batch_id": None, "rows_loaded": 0}

    def tearDown(self):
        self.temp_dir.cleanup()

    def _common_patches(self):
        return (
            patch.object(loader, "RAW_DATA_DIR", self.raw_dir),
            patch.object(loader, "get_manifest_record", return_value=self.manifest.copy()),
            patch.object(loader, "register_manifest"),
            patch.object(loader, "update_manifest_status"),
            patch.object(loader, "upload_file_to_stage", return_value="@STAGE/batch"),
            patch.object(loader, "_copy_taxi_zones", return_value=[("taxi_zone_lookup.csv", "LOADED", 1, 1, 0, None)]),
            patch.object(loader, "_copy_yellow_trips", return_value=[("yellow_tripdata_2025-01.parquet", "LOADED", 1, 1, 0, None)]),
            patch.object(loader, "record_ingestion_audit"),
        )

    def test_taxi_zones_success_uploads_copies_and_audits(self):
        patches = self._common_patches()
        with patches[0], patches[1], patches[2], patches[3] as update, \
             patches[4] as upload, patches[5] as copy_csv, patches[7] as audit:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "LOADED")
        self.assertEqual(result["rows_loaded"], 1)
        upload.assert_called_once()
        copy_csv.assert_called_once()
        audit.assert_called_once()
        self.assertTrue(any(call.args[4] == "LOADING" for call in update.call_args_list))
        self.assertTrue(any(call.args[4] == "LOADED" for call in update.call_args_list))

    def test_yellow_trips_success(self):
        patches = self._common_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4] as upload, \
             patches[6] as copy_parquet, patches[7] as audit:
            result = loader.load_yellow_trips(self.connection)
        self.assertEqual(result["status"], "LOADED")
        upload.assert_called_once()
        copy_parquet.assert_called_once()
        audit.assert_called_once()

    def test_loaded_manifest_skips_without_upload(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", return_value={
                 "status": "LOADED", "batch_id": "old", "rows_loaded": 265
             }), patch.object(loader, "is_file_already_loaded", return_value=True), \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "SKIPPED")
        upload.assert_not_called()

    def test_loaded_manifest_repairs_missing_audit_record(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", return_value={
                 "status": "LOADED", "batch_id": "old", "rows_loaded": 265
             }), patch.object(loader, "is_file_already_loaded", return_value=False), \
             patch.object(loader, "record_ingestion_audit") as audit, \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "RECONCILED")
        audit.assert_called_once()
        upload.assert_not_called()

    def test_legacy_audit_prevents_historical_reload(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", return_value=None), \
             patch.object(loader, "is_file_already_loaded", return_value=True), \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "SKIPPED")
        self.assertEqual(result["reason"], "LEGACY_AUDIT_ALREADY_LOADED")
        upload.assert_not_called()

    def test_unknown_manifest_blocks_automatic_retry(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", return_value={
                 "status": "UNKNOWN", "batch_id": "old", "rows_loaded": 0
             }), patch.object(loader, "get_successful_file_load_history", return_value=None), \
             patch.object(loader, "update_manifest_status"), \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "LOAD_STATUS_UNKNOWN")
        upload.assert_not_called()

    def test_unknown_manifest_reconciles_when_history_confirms_load(self):
        history = {"rows_loaded": 265, "source_load_time": "2026-11-01"}
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", return_value={
                 "status": "LOADING", "batch_id": "old", "rows_loaded": 0
             }), patch.object(loader, "get_successful_file_load_history", return_value=history), \
             patch.object(loader, "update_manifest_status") as update, \
             patch.object(loader, "is_file_already_loaded", return_value=True):
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "RECONCILED")
        update.assert_called_once()

    def test_copy_exception_marks_outcome_unknown(self):
        patches = self._common_patches()
        with patches[0], patches[1], patches[2], patches[3] as update, \
             patches[4], patch.object(loader, "_copy_taxi_zones", side_effect=TimeoutError("network lost")), \
             patch.object(loader, "record_ingestion_audit"):
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "LOAD_STATUS_UNKNOWN")
        self.assertTrue(any(call.args[4] == "UNKNOWN" for call in update.call_args_list))

    def test_upload_exception_marks_attempt_failed(self):
        patches = self._common_patches()
        with patches[0], patches[1], patches[2], patches[3] as update, \
             patch.object(loader, "upload_file_to_stage", side_effect=OSError("PUT failed")), \
             patch.object(loader, "record_ingestion_audit"):
            with self.assertRaises(OSError):
                loader.load_taxi_zones(self.connection)
        self.assertTrue(any(call.args[4] == "FAILED" for call in update.call_args_list))

    def test_audit_failure_does_not_claim_copy_failed(self):
        patches = self._common_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], \
             patch.object(loader, "record_ingestion_audit", side_effect=RuntimeError("audit unavailable")):
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "AUDIT_FAILED")
        self.assertTrue(result["copy_succeeded"])

    def test_load_history_falls_back_to_account_usage(self):
        history_row = ("batch/yellow_tripdata_2025-01.parquet", "YELLOW_TRIPS", "2026-11-03", "LOADED", 3475226, 0)
        with patch.object(loader, "execute_query", side_effect=[[], [history_row]]) as execute:
            result = loader.get_successful_file_load_history(
                self.connection, "yellow_tripdata_2025-01.parquet", "YELLOW_TRIPS"
            )
        self.assertEqual(result["rows_loaded"], 3475226)
        self.assertIn("SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY", execute.call_args_list[1].args[1])

    def test_stage_upload_uses_unique_subfolder_and_does_not_overwrite(self):
        source = self.raw_dir / "taxi_zone_lookup.csv"
        with patch.object(loader, "execute_query") as execute:
            stage_path = loader.upload_file_to_stage(
                self.connection, source, stage_subpath="batch-123"
            )
        self.assertEqual(stage_path, "@NYC_TAXI_DW.RAW.TAXI_TRIP_STAGE/batch-123")
        sql = execute.call_args.args[1]
        self.assertIn("PUT", sql)
        self.assertIn("OVERWRITE=FALSE", sql)

    def test_unregistered_first_load_requires_explicit_approval(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "ALLOW_FIRST_LOAD", False), \
             patch.object(loader, "get_manifest_record", return_value=None), \
             patch.object(loader, "is_file_already_loaded", return_value=False), \
             patch.object(loader, "get_successful_file_load_history", return_value=None), \
             patch.object(loader, "register_manifest") as register, \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "FIRST_LOAD_REQUIRES_APPROVAL")
        register.assert_not_called()
        upload.assert_not_called()

    def test_historical_load_without_checksum_blocks_first_load(self):
        history = {"rows_loaded": 265, "source_load_time": "2026-10-30"}
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "ALLOW_FIRST_LOAD", True), \
             patch.object(loader, "get_manifest_record", return_value=None), \
             patch.object(loader, "is_file_already_loaded", return_value=False), \
             patch.object(loader, "get_successful_file_load_history", return_value=history), \
             patch.object(loader, "register_manifest") as register, \
             patch.object(loader, "upload_file_to_stage") as upload:
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "LOAD_STATUS_UNKNOWN")
        self.assertEqual(result["reason"], "HISTORICAL_LOAD_WITHOUT_CHECKSUM")
        register.assert_not_called()
        upload.assert_not_called()

    def test_explicit_first_load_approval_allows_registration(self):
        patches = self._common_patches()
        with patch.object(loader, "ALLOW_FIRST_LOAD", True), \
             patch.object(loader, "RAW_DATA_DIR", self.raw_dir), \
             patch.object(loader, "get_manifest_record", side_effect=[None, self.manifest.copy()]), \
             patch.object(loader, "is_file_already_loaded", return_value=False), \
             patch.object(loader, "get_successful_file_load_history", return_value=None), \
             patch.object(loader, "register_manifest") as register, \
             patch.object(loader, "update_manifest_status"), \
             patch.object(loader, "upload_file_to_stage"), \
             patch.object(loader, "_copy_taxi_zones", return_value=[("f", "LOADED", 1, 1, 0, None)]), \
             patch.object(loader, "record_ingestion_audit"):
            result = loader.load_taxi_zones(self.connection)
        self.assertEqual(result["status"], "LOADED")
        register.assert_called_once()

    def test_missing_local_file_fails_before_manifest_access(self):
        with patch.object(loader, "RAW_DATA_DIR", self.raw_dir / "missing"), \
             patch.object(loader, "get_manifest_record") as get_manifest:
            with self.assertRaises(FileNotFoundError):
                loader.load_taxi_zones(self.connection)
        get_manifest.assert_not_called()


if __name__ == "__main__":
    unittest.main()

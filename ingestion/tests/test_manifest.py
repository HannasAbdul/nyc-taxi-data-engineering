import unittest
from unittest.mock import Mock, patch

from ingestion.src.nyc_taxi import manifest


class TestManifestFunctions(unittest.TestCase):
    def test_get_manifest_record_returns_normalized_fields(self):
        connection = Mock()
        fake_cursor = Mock()
        fake_cursor.description = ["STATUS", "BATCH_ID", "ROWS_LOADED", "ERROR_MESSAGE"]
        fake_cursor.fetchall.return_value = [("LOADED", "batch-1", 265, None)]
        connection.cursor.return_value = fake_cursor

        result = manifest.get_manifest_record(
            connection, "taxi_zone_lookup.csv", "a" * 64, "TAXI_ZONES"
        )
        self.assertEqual(result["status"], "LOADED")
        self.assertEqual(result["rows_loaded"], 265)
        fake_cursor.close.assert_called_once()

    def test_get_manifest_record_returns_none_when_absent(self):
        connection = Mock()
        fake_cursor = Mock()
        fake_cursor.description = ["STATUS"]
        fake_cursor.fetchall.return_value = []
        connection.cursor.return_value = fake_cursor
        self.assertIsNone(
            manifest.get_manifest_record(
                connection, "missing.csv", "b" * 64, "TAXI_ZONES"
            )
        )

    def test_register_manifest_uses_merge_and_bound_parameters(self):
        connection = Mock()
        fake_cursor = Mock()
        fake_cursor.description = None
        connection.cursor.return_value = fake_cursor
        manifest.register_manifest(
            connection, "zones.csv", "c" * 64, "TAXI_ZONES"
        )
        sql, params = fake_cursor.execute.call_args.args
        self.assertIn("MERGE INTO", sql)
        self.assertEqual(params, ("zones.csv", "c" * 64, "TAXI_ZONES"))
        fake_cursor.close.assert_called_once()

    def test_update_rejects_invalid_status_before_sql(self):
        connection = Mock()
        with self.assertRaises(ValueError):
            manifest.update_manifest_status(
                connection, "zones.csv", "d" * 64, "TAXI_ZONES", "FINISHED"
            )
        connection.cursor.assert_not_called()


if __name__ == "__main__":
    unittest.main()

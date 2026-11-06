import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.src.nyc_taxi import downloader


class TestChecksumMetadata(unittest.TestCase):
    def test_checksum_is_written_and_not_duplicated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_dir = Path(temp_dir) / "metadata"
            source = Path(temp_dir) / "raw" / "sample.csv"
            source.parent.mkdir(parents=True)
            source.write_text("a,b\n1,2\n", encoding="utf-8")
            with patch.object(downloader, "METADATA_DIR", metadata_dir):
                downloader.save_checksum(source, "a" * 64)
                downloader.save_checksum(source, "a" * 64)
            lines = (metadata_dir / "source-checksums.sha256").read_text().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertIn(str(source.resolve()), lines[0])

    def test_conflicting_duplicate_records_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_dir = Path(temp_dir) / "metadata"
            metadata_dir.mkdir()
            source = Path(temp_dir) / "raw" / "sample.csv"
            source.parent.mkdir()
            checksum_file = metadata_dir / "source-checksums.sha256"
            checksum_file.write_text(
                f"{'a' * 64}  {source.resolve()}\n"
                f"{'b' * 64}  {source.resolve()}\n",
                encoding="utf-8",
            )
            with patch.object(downloader, "METADATA_DIR", metadata_dir):
                with self.assertRaises(ValueError):
                    downloader.save_checksum(source, "c" * 64)


if __name__ == "__main__":
    unittest.main()

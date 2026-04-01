# tests/test_gtfs_loader.py
"""
Tests for src/ingest/gtfs_loader.py

Covers:
  - download(): streaming write (iter_content), skip-if-cached, force flag
  - load(): parallel CSV parsing via ThreadPoolExecutor, missing file handling
"""

import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

import src.ingest.gtfs_loader as gtfs_loader
from src.ingest.gtfs_loader import download, load, _parse_file


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_zip(files: dict[str, str]) -> bytes:
    """Build an in-memory zip containing the given {filename: csv_content} pairs."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


# ── download() ────────────────────────────────────────────────────────────────

class TestDownload:
    def test_skips_if_zip_exists(self, tmp_path):
        zip_path = tmp_path / "gtfs.zip"
        zip_path.write_bytes(b"existing")

        with patch.object(gtfs_loader, "RAW_DIR", tmp_path), \
             patch("src.ingest.gtfs_loader.requests.get") as mock_get:
            result = download(force=False)

        mock_get.assert_not_called()
        assert result == zip_path

    def test_downloads_when_zip_missing(self, tmp_path):
        chunk = b"Z" * 1024
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [chunk]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(gtfs_loader, "RAW_DIR", tmp_path), \
             patch("src.ingest.gtfs_loader.get_config") as mock_cfg, \
             patch("src.ingest.gtfs_loader.requests.get", return_value=mock_response):
            mock_cfg.return_value.gtfs_feed_url = "http://fake/gtfs.zip"
            mock_cfg.return_value.wmata_api_key = "key"
            result = download()

        assert result == tmp_path / "gtfs.zip"
        assert (tmp_path / "gtfs.zip").read_bytes() == chunk

    def test_force_redownloads_existing_zip(self, tmp_path):
        zip_path = tmp_path / "gtfs.zip"
        zip_path.write_bytes(b"old")

        new_chunk = b"new_content"
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [new_chunk]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(gtfs_loader, "RAW_DIR", tmp_path), \
             patch("src.ingest.gtfs_loader.get_config") as mock_cfg, \
             patch("src.ingest.gtfs_loader.requests.get", return_value=mock_response):
            mock_cfg.return_value.gtfs_feed_url = "http://fake/gtfs.zip"
            mock_cfg.return_value.wmata_api_key = "key"
            download(force=True)

        assert zip_path.read_bytes() == new_chunk

    def test_uses_streaming_request(self, tmp_path):
        """Verifies stream=True is passed — ensures no full-buffer download."""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = []
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(gtfs_loader, "RAW_DIR", tmp_path), \
             patch("src.ingest.gtfs_loader.get_config") as mock_cfg, \
             patch("src.ingest.gtfs_loader.requests.get", return_value=mock_response) as mock_get:
            mock_cfg.return_value.gtfs_feed_url = "http://fake/gtfs.zip"
            mock_cfg.return_value.wmata_api_key = "key"
            download()

        _, kwargs = mock_get.call_args
        assert kwargs.get("stream") is True

    def test_raises_on_http_error(self, tmp_path):
        import requests as req
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req.HTTPError("404")
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(gtfs_loader, "RAW_DIR", tmp_path), \
             patch("src.ingest.gtfs_loader.get_config") as mock_cfg, \
             patch("src.ingest.gtfs_loader.requests.get", return_value=mock_response):
            mock_cfg.return_value.gtfs_feed_url = "http://fake/gtfs.zip"
            mock_cfg.return_value.wmata_api_key = "key"
            with pytest.raises(req.HTTPError):
                download()


# ── load() parallel parsing ───────────────────────────────────────────────────

class TestLoad:
    def _make_gtfs_dir(self, tmp_path: Path, files: dict[str, str]) -> Path:
        gtfs_dir = tmp_path / "gtfs"
        gtfs_dir.mkdir()
        for name, content in files.items():
            (gtfs_dir / f"{name}.txt").write_text(content)
        return gtfs_dir

    def test_all_present_files_loaded(self, tmp_path):
        gtfs_dir = self._make_gtfs_dir(tmp_path, {
            "routes": "route_id,route_short_name\nA,Red",
            "stops": "stop_id,stop_name\nS1,Metro Center",
        })
        zip_path = tmp_path / "raw" / "gtfs.zip"
        zip_path.parent.mkdir()
        zip_path.write_bytes(b"")

        with patch.object(gtfs_loader, "GTFS_DIR", gtfs_dir), \
             patch.object(gtfs_loader, "RAW_DIR", zip_path.parent), \
             patch.object(gtfs_loader, "GTFS_FILES", ["routes", "stops"]), \
             patch("src.ingest.gtfs_loader.download", return_value=zip_path), \
             patch("src.ingest.gtfs_loader.extract_zip"):
            data = load()

        assert set(data.keys()) == {"routes", "stops"}
        assert len(data["routes"]) == 1
        assert len(data["stops"]) == 1

    def test_missing_file_is_omitted(self, tmp_path):
        gtfs_dir = self._make_gtfs_dir(tmp_path, {
            "routes": "route_id\nA",
        })
        zip_path = tmp_path / "raw" / "gtfs.zip"
        zip_path.parent.mkdir()
        zip_path.write_bytes(b"")

        with patch.object(gtfs_loader, "GTFS_DIR", gtfs_dir), \
             patch.object(gtfs_loader, "RAW_DIR", zip_path.parent), \
             patch.object(gtfs_loader, "GTFS_FILES", ["routes", "stops"]), \
             patch("src.ingest.gtfs_loader.download", return_value=zip_path), \
             patch("src.ingest.gtfs_loader.extract_zip"):
            data = load()

        assert "routes" in data
        assert "stops" not in data

    def test_all_files_missing_returns_empty(self, tmp_path):
        gtfs_dir = tmp_path / "gtfs"
        gtfs_dir.mkdir()
        zip_path = tmp_path / "raw" / "gtfs.zip"
        zip_path.parent.mkdir()
        zip_path.write_bytes(b"")

        with patch.object(gtfs_loader, "GTFS_DIR", gtfs_dir), \
             patch.object(gtfs_loader, "RAW_DIR", zip_path.parent), \
             patch.object(gtfs_loader, "GTFS_FILES", ["routes", "stops"]), \
             patch("src.ingest.gtfs_loader.download", return_value=zip_path), \
             patch("src.ingest.gtfs_loader.extract_zip"):
            data = load()

        assert data == {}

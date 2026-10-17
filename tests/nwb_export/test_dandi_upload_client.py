"""
Tests for DandiUploadClient (T064 / US7).

All tests are no_db — they mock ``neuroconv.tools.data_transfers.automatic_dandi_upload``
and never touch the file system beyond a temporary NWB stub.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from u19_pipeline.nwb_export.dandi.upload_client import DandiUploadClient

pytestmark = pytest.mark.no_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_nwb_file(tmp_path: Path, name: str = "output.nwb") -> Path:
    """Create a minimal (empty) NWB stub file for testing."""
    p = tmp_path / name
    p.write_bytes(b"")
    return p


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------

class TestDandiUploadClientInit:
    def test_valid_construction(self):
        client = DandiUploadClient(api_key="my-key", dandiset_id="000123")
        assert client._api_key == "my-key"
        assert client._dandiset_id == "000123"
        assert client._sandbox is False

    def test_sandbox_flag_stored(self):
        client = DandiUploadClient(api_key="k", dandiset_id="000999", sandbox=True)
        assert client._sandbox is True

    def test_empty_api_key_raises(self):
        with pytest.raises(ValueError, match="api_key"):
            DandiUploadClient(api_key="", dandiset_id="000123")

    def test_whitespace_api_key_raises(self):
        with pytest.raises(ValueError, match="api_key"):
            DandiUploadClient(api_key="   ", dandiset_id="000123")

    def test_empty_dandiset_id_raises(self):
        with pytest.raises(ValueError, match="dandiset_id"):
            DandiUploadClient(api_key="key", dandiset_id="")

    def test_whitespace_dandiset_id_raises(self):
        with pytest.raises(ValueError, match="dandiset_id"):
            DandiUploadClient(api_key="key", dandiset_id="  ")


# ---------------------------------------------------------------------------
# upload() — file-not-found guard
# ---------------------------------------------------------------------------

class TestUploadFileNotFound:
    def test_raises_file_not_found_for_missing_path(self):
        client = DandiUploadClient(api_key="k", dandiset_id="000123")
        with pytest.raises(FileNotFoundError, match="NWB file not found"):
            client.upload("/does/not/exist.nwb")


# ---------------------------------------------------------------------------
# upload() — happy path
# ---------------------------------------------------------------------------

class TestUploadHappyPath:
    def test_returns_organised_paths_list(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)
        organised = ["/tmp/staged/sub-ms1/sub-ms1_ses-001.nwb"]

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            return_value=organised,
        ) as mock_upload:
            client = DandiUploadClient(api_key="secret", dandiset_id="000123")
            result = client.upload(nwb_file)

        assert result == organised
        mock_upload.assert_called_once()

    def test_dandiset_id_forwarded_to_neuroconv(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            return_value=[],
        ) as mock_upload:
            client = DandiUploadClient(api_key="key", dandiset_id="000456")
            client.upload(nwb_file)

        call_kwargs = mock_upload.call_args.kwargs
        assert call_kwargs["dandiset_id"] == "000456"

    def test_sandbox_flag_forwarded_to_neuroconv(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            return_value=[],
        ) as mock_upload:
            client = DandiUploadClient(api_key="key", dandiset_id="000123", sandbox=True)
            client.upload(nwb_file)

        call_kwargs = mock_upload.call_args.kwargs
        assert call_kwargs["sandbox"] is True

    def test_api_key_set_in_env_during_upload(self, tmp_path):
        """DANDI_API_KEY must be set in the environment while neuroconv runs."""
        nwb_file = _make_nwb_file(tmp_path)
        captured_env: dict = {}

        def _capture(*args, **kwargs):
            captured_env.update(os.environ.copy())
            return []

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            side_effect=_capture,
        ):
            client = DandiUploadClient(api_key="my-secret-key", dandiset_id="000123")
            client.upload(nwb_file)

        assert captured_env.get("DANDI_API_KEY") == "my-secret-key"

    def test_sandbox_api_key_env_var_used_when_sandbox(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)
        captured_env: dict = {}

        def _capture(*args, **kwargs):
            captured_env.update(os.environ.copy())
            return []

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            side_effect=_capture,
        ):
            client = DandiUploadClient(api_key="sandbox-key", dandiset_id="000123", sandbox=True)
            client.upload(nwb_file)

        assert captured_env.get("DANDI_SANDBOX_API_KEY") == "sandbox-key"

    def test_api_key_env_var_restored_after_upload(self, tmp_path):
        """Env var must be restored (or removed) once upload completes."""
        nwb_file = _make_nwb_file(tmp_path)
        os.environ.pop("DANDI_API_KEY", None)

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            return_value=[],
        ):
            client = DandiUploadClient(api_key="temp-key", dandiset_id="000123")
            client.upload(nwb_file)

        assert "DANDI_API_KEY" not in os.environ

    def test_api_key_env_var_previous_value_restored(self, tmp_path):
        """If DANDI_API_KEY was already set, restore its original value."""
        nwb_file = _make_nwb_file(tmp_path)
        os.environ["DANDI_API_KEY"] = "original-value"

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            return_value=[],
        ):
            client = DandiUploadClient(api_key="temp-key", dandiset_id="000123")
            client.upload(nwb_file)

        assert os.environ["DANDI_API_KEY"] == "original-value"
        del os.environ["DANDI_API_KEY"]


# ---------------------------------------------------------------------------
# upload() — error handling
# ---------------------------------------------------------------------------

class TestUploadErrors:
    def test_neuroconv_exception_wrapped_in_runtime_error(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            side_effect=Exception("network timeout"),
        ):
            client = DandiUploadClient(api_key="key", dandiset_id="000123")
            with pytest.raises(RuntimeError, match="DANDI upload failed"):
                client.upload(nwb_file)

    def test_env_var_restored_even_on_error(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)
        os.environ.pop("DANDI_API_KEY", None)

        with patch(
            "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
            side_effect=Exception("boom"),
        ):
            client = DandiUploadClient(api_key="ephemeral", dandiset_id="000123")
            with pytest.raises(RuntimeError):
                client.upload(nwb_file)

        assert "DANDI_API_KEY" not in os.environ

    def test_import_error_raises_runtime_error(self, tmp_path):
        nwb_file = _make_nwb_file(tmp_path)

        with patch.dict("sys.modules", {"neuroconv": None, "neuroconv.tools": None,
                                         "neuroconv.tools.data_transfers": None}):
            # Re-import triggers ImportError inside _do_upload
            with patch(
                "u19_pipeline.nwb_export.dandi.upload_client.automatic_dandi_upload",
                side_effect=ImportError("no neuroconv"),
            ):
                client = DandiUploadClient(api_key="key", dandiset_id="000123")
                with pytest.raises(RuntimeError, match="neuroconv"):
                    client.upload(nwb_file)

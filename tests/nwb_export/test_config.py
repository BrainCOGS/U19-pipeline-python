"""
Tests for NWB export configuration constants.

All tests are no_db — only test that config values are defined and sane.

TDD Note: These tests were written BEFORE creating config.py (Constitution
Principle V). Confirm ImportError on first run before implementing T004.
"""

import pytest


@pytest.mark.no_db
class TestNwbExportConfigImport:
    """Config module must be importable and expose expected names."""

    def test_module_importable(self):
        """config module is importable from nwb_export package."""
        from u19_pipeline.nwb_export import config  # noqa: F401

    def test_max_dandi_retries_defined(self):
        """MAX_DANDI_RETRIES defined and matches FR-034 (must be 3)."""
        from u19_pipeline.nwb_export.config import MAX_DANDI_RETRIES

        assert MAX_DANDI_RETRIES == 3, (
            "FR-034 requires exactly 3 automatic retry attempts before surfacing failure"
        )

    def test_dandi_retry_base_delay_defined(self):
        """DANDI_RETRY_BASE_DELAY_SECONDS is a positive number for exponential backoff."""
        from u19_pipeline.nwb_export.config import DANDI_RETRY_BASE_DELAY_SECONDS

        assert isinstance(DANDI_RETRY_BASE_DELAY_SECONDS, (int, float))
        assert DANDI_RETRY_BASE_DELAY_SECONDS > 0

    def test_dandi_retry_jitter_fraction_defined(self):
        """DANDI_RETRY_JITTER_FRACTION in (0, 1] for bounded jitter."""
        from u19_pipeline.nwb_export.config import DANDI_RETRY_JITTER_FRACTION

        assert 0 < DANDI_RETRY_JITTER_FRACTION <= 1.0

    def test_log_retention_days_defined(self):
        """LOG_RETENTION_DAYS defined and ≥30 to satisfy SC-009."""
        from u19_pipeline.nwb_export.config import LOG_RETENTION_DAYS

        assert isinstance(LOG_RETENTION_DAYS, int)
        assert LOG_RETENTION_DAYS >= 30, (
            "SC-009 requires historical logs retained for at least 30 days"
        )

    def test_required_metadata_fields_defined(self):
        """REQUIRED_NWB_METADATA_FIELDS is a non-empty sequence (FR-019)."""
        from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS

        assert len(REQUIRED_NWB_METADATA_FIELDS) > 0
        # Must include the fields called out in FR-019
        for field in ("session_start_time", "institution", "experimenter"):
            assert field in REQUIRED_NWB_METADATA_FIELDS, (
                f"FR-019 requires '{field}' in REQUIRED_NWB_METADATA_FIELDS"
            )


@pytest.mark.no_db
class TestNwbExportConfigTypes:
    """Config values must have the expected Python types."""

    def test_max_dandi_retries_is_int(self):
        from u19_pipeline.nwb_export.config import MAX_DANDI_RETRIES
        assert isinstance(MAX_DANDI_RETRIES, int)

    def test_log_retention_days_is_int(self):
        from u19_pipeline.nwb_export.config import LOG_RETENTION_DAYS
        assert isinstance(LOG_RETENTION_DAYS, int)

    def test_required_metadata_fields_is_sequence(self):
        from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS
        assert hasattr(REQUIRED_NWB_METADATA_FIELDS, "__iter__")
        assert hasattr(REQUIRED_NWB_METADATA_FIELDS, "__len__")

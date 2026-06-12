"""
Tests for NWB export custom exception hierarchy.

All tests are no_db — only test exception class definitions and behaviour.

TDD Note: These tests were written BEFORE creating errors.py (Constitution
Principle V). Confirm ImportError on first run before implementing T003.
"""

import pytest


@pytest.mark.no_db
class TestNwbExportErrorHierarchy:
    """The exception module must exist and export the right hierarchy."""

    def test_module_importable(self):
        """errors module is importable from nwb_export package."""
        from u19_pipeline.nwb_export import errors  # noqa: F401

    def test_base_exception_exists(self):
        """NwbExportError base class exists and is an Exception subclass."""
        from u19_pipeline.nwb_export.errors import NwbExportError

        assert issubclass(NwbExportError, Exception)

    def test_validation_error_exists(self):
        """NwbValidationError is a subclass of NwbExportError."""
        from u19_pipeline.nwb_export.errors import NwbValidationError, NwbExportError

        assert issubclass(NwbValidationError, NwbExportError)

    def test_conversion_error_exists(self):
        """NwbConversionError is a subclass of NwbExportError."""
        from u19_pipeline.nwb_export.errors import NwbConversionError, NwbExportError

        assert issubclass(NwbConversionError, NwbExportError)

    def test_status_transition_error_exists(self):
        """NwbStatusTransitionError is a subclass of NwbExportError."""
        from u19_pipeline.nwb_export.errors import NwbStatusTransitionError, NwbExportError

        assert issubclass(NwbStatusTransitionError, NwbExportError)

    def test_dandi_upload_error_exists(self):
        """NwbDandiUploadError is a subclass of NwbExportError."""
        from u19_pipeline.nwb_export.errors import NwbDandiUploadError, NwbExportError

        assert issubclass(NwbDandiUploadError, NwbExportError)


@pytest.mark.no_db
class TestNwbExportErrorBehaviour:
    """Exceptions carry expected message and context attributes."""

    def test_base_error_carries_message(self):
        """NwbExportError stores message as str(e)."""
        from u19_pipeline.nwb_export.errors import NwbExportError

        err = NwbExportError("something broke")
        assert "something broke" in str(err)

    def test_validation_error_carries_modality(self):
        """NwbValidationError stores the failing modality name."""
        from u19_pipeline.nwb_export.errors import NwbValidationError

        err = NwbValidationError("No trials found", modality="behavior")
        assert err.modality == "behavior"
        assert "No trials found" in str(err)

    def test_status_transition_error_carries_states(self):
        """NwbStatusTransitionError records from_state and to_state."""
        from u19_pipeline.nwb_export.errors import NwbStatusTransitionError

        err = NwbStatusTransitionError(from_state="COMPLETED", to_state="QUEUED")
        assert err.from_state == "COMPLETED"
        assert err.to_state == "QUEUED"

    def test_dandi_upload_error_carries_attempt_count(self):
        """NwbDandiUploadError records how many attempts were made."""
        from u19_pipeline.nwb_export.errors import NwbDandiUploadError

        err = NwbDandiUploadError("rate limited", attempt=3)
        assert err.attempt == 3
        assert "rate limited" in str(err)

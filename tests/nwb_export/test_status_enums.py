"""
Tests for NWB export status enumerations.

Covers NwbExportStatusEnum, DataModalityTypeEnum, and DandiUploadStatusEnum.
All tests are no_db: they verify enum definitions only, no database required.

TDD Note: test_uploaded_state_exists and test_state_machine_uploaded_transition
were written BEFORE adding UPLOADED to NwbExportStatusEnum (per Constitution
Principle V). Confirm these fail before implementing T002.
"""

import pytest
from enum import IntEnum


@pytest.mark.no_db
class TestNwbExportStatusEnum:
    """Tests for the core pipeline status enum."""

    def test_all_required_pipeline_states_present(self):
        """Enum contains every state required by FR-029."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        required = {
            "QUEUED",
            "DATA_VALIDATION",
            "PROCESSING",
            "VALIDATION",
            "UPLOAD",
            "UPLOADED",       # Added by I2 fix — must exist
            "COMPLETED",
            "FAILED",
        }
        actual = {m.name for m in NwbExportStatusEnum}
        missing = required - actual
        assert not missing, f"Missing required states: {missing}"

    def test_uploaded_state_exists(self):
        """UPLOADED state exists as a distinct value from UPLOAD and COMPLETED (FR-027 / I2 fix)."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert hasattr(NwbExportStatusEnum, "UPLOADED"), (
            "UPLOADED state required for VALIDATION → UPLOAD → UPLOADED → COMPLETED "
            "transition chain (spec FR-027 / analysis finding I2)"
        )

    def test_uploaded_is_not_terminal(self):
        """UPLOADED is not a terminal state — job must still reach COMPLETED."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert not NwbExportStatusEnum.UPLOADED.is_terminal, (
            "UPLOADED should not be terminal; job transitions to COMPLETED after asset ID persisted"
        )

    def test_completed_and_failed_are_terminal(self):
        """Only COMPLETED and FAILED are terminal states."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        terminal = {m.name for m in NwbExportStatusEnum if m.is_terminal}
        assert terminal == {"COMPLETED", "FAILED"}

    def test_failed_has_negative_value(self):
        """FAILED uses a negative integer so it sorts separately from pipeline stages."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert NwbExportStatusEnum.FAILED.value < 0

    def test_queued_has_lowest_non_negative_value(self):
        """QUEUED is the entry point — value 0."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert NwbExportStatusEnum.QUEUED.value == 0

    def test_pipeline_order_queued_before_data_validation(self):
        """QUEUED.value < DATA_VALIDATION.value (pipeline progression)."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert NwbExportStatusEnum.QUEUED.value < NwbExportStatusEnum.DATA_VALIDATION.value

    def test_uploaded_between_upload_and_completed(self):
        """UPLOAD.value < UPLOADED.value < COMPLETED.value preserves pipeline order."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert NwbExportStatusEnum.UPLOAD.value < NwbExportStatusEnum.UPLOADED.value
        assert NwbExportStatusEnum.UPLOADED.value < NwbExportStatusEnum.COMPLETED.value

    def test_is_active_for_non_terminal_states(self):
        """is_active returns True for all non-terminal states."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        non_terminal = [m for m in NwbExportStatusEnum if not m.is_terminal]
        assert all(m.is_active for m in non_terminal), (
            "All non-terminal states should report is_active=True"
        )

    def test_str_returns_name(self):
        """str(state) returns the human-readable name."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        assert str(NwbExportStatusEnum.QUEUED) == "QUEUED"
        assert str(NwbExportStatusEnum.FAILED) == "FAILED"

    def test_int_cast_storable_as_tinyint(self):
        """All enum values fit in a signed TINYINT (-128..127) for DB storage."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

        for member in NwbExportStatusEnum:
            assert -128 <= member.value <= 127, (
                f"{member.name} value {member.value} out of TINYINT range"
            )


@pytest.mark.no_db
class TestDataModalityTypeEnum:
    """Tests for the data modality type enum."""

    def test_all_modality_types_present(self):
        """Enum covers all modalities from FR-001."""
        from u19_pipeline.nwb_export_enums import DataModalityTypeEnum

        required = {"BEHAVIOR", "EPHYS_RAW", "EPHYS_PROCESSED", "IMAGING_RAW", "IMAGING_PROCESSED"}
        actual = {m.name for m in DataModalityTypeEnum}
        missing = required - actual
        assert not missing, f"Missing modality types: {missing}"

    def test_modality_values_are_integers(self):
        """Modality types have integer values for DB storage."""
        from u19_pipeline.nwb_export_enums import DataModalityTypeEnum

        for member in DataModalityTypeEnum:
            assert isinstance(member.value, int)


@pytest.mark.no_db
class TestDandiUploadStatusEnum:
    """Tests for the DANDI upload status enum."""

    def test_all_dandi_states_present(self):
        """Enum covers all upload lifecycle states."""
        from u19_pipeline.nwb_export_enums import DandiUploadStatusEnum

        required = {"NOT_APPLICABLE", "PENDING", "IN_PROGRESS", "COMPLETED", "FAILED"}
        actual = {m.name for m in DandiUploadStatusEnum}
        missing = required - actual
        assert not missing, f"Missing DANDI upload states: {missing}"

    def test_not_applicable_is_zero(self):
        """NOT_APPLICABLE is the default/null state (value 0)."""
        from u19_pipeline.nwb_export_enums import DandiUploadStatusEnum

        assert DandiUploadStatusEnum.NOT_APPLICABLE.value == 0

    def test_failed_has_negative_value(self):
        """FAILED upload state uses negative integer consistent with export status convention."""
        from u19_pipeline.nwb_export_enums import DandiUploadStatusEnum

        assert DandiUploadStatusEnum.FAILED.value < 0

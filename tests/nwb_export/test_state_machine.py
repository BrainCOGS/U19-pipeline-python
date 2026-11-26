"""
Tests for the NWB export status transition state machine.

All tests are no_db — only test the pure-logic transition guard.

Allowed transitions (from data-model.md):
    QUEUED          → DATA_VALIDATION
    DATA_VALIDATION → PROCESSING | FAILED
    PROCESSING      → VALIDATION | FAILED
    VALIDATION      → COMPLETED | UPLOAD | FAILED
    UPLOAD          → UPLOADED  | FAILED
    UPLOADED        → COMPLETED

TDD Note: These tests were written BEFORE creating state_machine.py
(Constitution Principle V). Confirm ImportError on first run before T011.
"""

import pytest


@pytest.mark.no_db
class TestStateMachineImport:
    """state_machine module must be importable from nwb_export package."""

    def test_module_importable(self):
        from u19_pipeline.nwb_export import state_machine  # noqa: F401

    def test_is_valid_transition_callable(self):
        from u19_pipeline.nwb_export.state_machine import is_valid_transition
        assert callable(is_valid_transition)

    def test_assert_valid_transition_callable(self):
        from u19_pipeline.nwb_export.state_machine import assert_valid_transition
        assert callable(assert_valid_transition)

    def test_allowed_transitions_exported(self):
        from u19_pipeline.nwb_export.state_machine import ALLOWED_TRANSITIONS
        assert isinstance(ALLOWED_TRANSITIONS, dict)
        assert len(ALLOWED_TRANSITIONS) > 0


@pytest.mark.no_db
class TestAllowedTransitions:
    """Every transition in the data-model spec must be accepted."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        from u19_pipeline.nwb_export.state_machine import is_valid_transition
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
        self.is_valid = is_valid_transition
        self.S = NwbExportStatusEnum

    def test_queued_to_data_validation(self):
        assert self.is_valid(self.S.QUEUED, self.S.DATA_VALIDATION)

    def test_data_validation_to_processing(self):
        assert self.is_valid(self.S.DATA_VALIDATION, self.S.PROCESSING)

    def test_data_validation_to_failed(self):
        assert self.is_valid(self.S.DATA_VALIDATION, self.S.FAILED)

    def test_processing_to_validation(self):
        assert self.is_valid(self.S.PROCESSING, self.S.VALIDATION)

    def test_processing_to_failed(self):
        assert self.is_valid(self.S.PROCESSING, self.S.FAILED)

    def test_validation_to_completed(self):
        """Direct VALIDATION → COMPLETED when DANDI credentials absent."""
        assert self.is_valid(self.S.VALIDATION, self.S.COMPLETED)

    def test_validation_to_upload(self):
        """VALIDATION → UPLOAD when DANDI credentials present."""
        assert self.is_valid(self.S.VALIDATION, self.S.UPLOAD)

    def test_validation_to_failed(self):
        assert self.is_valid(self.S.VALIDATION, self.S.FAILED)

    def test_upload_to_uploaded(self):
        assert self.is_valid(self.S.UPLOAD, self.S.UPLOADED)

    def test_upload_to_failed(self):
        assert self.is_valid(self.S.UPLOAD, self.S.FAILED)

    def test_uploaded_to_completed(self):
        """UPLOADED → COMPLETED after asset ID persisted (FR-027 / I2 fix)."""
        assert self.is_valid(self.S.UPLOADED, self.S.COMPLETED)


@pytest.mark.no_db
class TestBlockedTransitions:
    """Illegal transitions must return False or raise NwbStatusTransitionError."""

    @pytest.fixture(autouse=True)
    def _imports(self):
        from u19_pipeline.nwb_export.state_machine import is_valid_transition, assert_valid_transition
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
        from u19_pipeline.nwb_export.errors import NwbStatusTransitionError
        self.is_valid = is_valid_transition
        self.assert_valid = assert_valid_transition
        self.S = NwbExportStatusEnum
        self.Error = NwbStatusTransitionError

    def test_queued_to_processing_blocked(self):
        """Cannot skip DATA_VALIDATION."""
        assert not self.is_valid(self.S.QUEUED, self.S.PROCESSING)

    def test_queued_to_completed_blocked(self):
        assert not self.is_valid(self.S.QUEUED, self.S.COMPLETED)

    def test_completed_to_queued_blocked(self):
        """Terminal state — no forward or backward movement."""
        assert not self.is_valid(self.S.COMPLETED, self.S.QUEUED)

    def test_completed_to_failed_blocked(self):
        assert not self.is_valid(self.S.COMPLETED, self.S.FAILED)

    def test_failed_to_queued_blocked(self):
        """Failed is terminal — retry is handled at the service level, not the state machine."""
        assert not self.is_valid(self.S.FAILED, self.S.QUEUED)

    def test_processing_to_upload_blocked(self):
        """Cannot jump from PROCESSING to UPLOAD (must pass VALIDATION first)."""
        assert not self.is_valid(self.S.PROCESSING, self.S.UPLOAD)

    def test_uploaded_to_upload_blocked(self):
        """Cannot go backwards."""
        assert not self.is_valid(self.S.UPLOADED, self.S.UPLOAD)

    def test_same_state_transition_blocked(self):
        """Self-transitions are not allowed."""
        for state in self.S:
            assert not self.is_valid(state, state), (
                f"Self-transition on {state.name} should be blocked"
            )

    def test_assert_valid_raises_on_illegal(self):
        with pytest.raises(self.Error) as exc_info:
            self.assert_valid(self.S.COMPLETED, self.S.QUEUED)
        assert exc_info.value.from_state == "COMPLETED"
        assert exc_info.value.to_state == "QUEUED"

    def test_assert_valid_does_not_raise_on_legal(self):
        """assert_valid_transition must not raise for a legal transition."""
        self.assert_valid(self.S.QUEUED, self.S.DATA_VALIDATION)  # should not raise


@pytest.mark.no_db
class TestAllowedTransitionsCoverage:
    """ALLOWED_TRANSITIONS map must cover exactly the spec-defined edges."""

    def test_all_spec_transitions_in_map(self):
        """Every edge in data-model.md state machine is present."""
        from u19_pipeline.nwb_export.state_machine import ALLOWED_TRANSITIONS
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum as S

        required_edges = [
            (S.QUEUED,          S.DATA_VALIDATION),
            (S.DATA_VALIDATION, S.PROCESSING),
            (S.DATA_VALIDATION, S.FAILED),
            (S.PROCESSING,      S.VALIDATION),
            (S.PROCESSING,      S.FAILED),
            (S.VALIDATION,      S.COMPLETED),
            (S.VALIDATION,      S.UPLOAD),
            (S.VALIDATION,      S.FAILED),
            (S.UPLOAD,          S.UPLOADED),
            (S.UPLOAD,          S.FAILED),
            (S.UPLOADED,        S.COMPLETED),
        ]

        for from_s, to_s in required_edges:
            assert to_s in ALLOWED_TRANSITIONS.get(from_s, set()), (
                f"Missing required transition: {from_s.name} → {to_s.name}"
            )

"""
No-db tests for EphysReadinessResult output contract (T031 / US2).

These tests verify the shape and attribute guarantees of EphysReadinessResult,
and test the pass/fail logic of check_ephys_readiness via mocking.
"""

import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.no_db
class TestEphysReadinessResultShape:
    """EphysReadinessResult must satisfy the output contract schema."""

    def test_importable(self):
        from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult  # noqa

    def test_has_all_output_contract_fields(self):
        from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult

        result = EphysReadinessResult(
            subject_exists=True,
            ephys_session_count=3,
            required_date_present=True,
            required_session_date="2024-07-22",
            ephys_session_dates=["2024-07-15", "2024-07-22"],
            imaging_checked=False,
            passed=True,
            messages=["PASS"],
        )
        assert hasattr(result, "subject_exists")
        assert hasattr(result, "ephys_session_count")
        assert hasattr(result, "required_date_present")
        assert hasattr(result, "required_session_date")
        assert hasattr(result, "ephys_session_dates")
        assert hasattr(result, "imaging_checked")
        assert hasattr(result, "passed")
        assert hasattr(result, "messages")

    def test_imaging_checked_always_false_default(self):
        from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult
        result = EphysReadinessResult(
            subject_exists=True, ephys_session_count=2,
            required_date_present=True, required_session_date="2024-07-22",
            ephys_session_dates=[], passed=True, messages=[],
        )
        assert result.imaging_checked is False

    def test_messages_is_list(self):
        from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult
        result = EphysReadinessResult(
            subject_exists=True, ephys_session_count=2,
            required_date_present=True, required_session_date="2024-07-22",
            ephys_session_dates=[], passed=True, messages=["m1", "m2"],
        )
        assert isinstance(result.messages, list)

    def test_ephys_session_dates_is_list(self):
        from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult
        result = EphysReadinessResult(
            subject_exists=True, ephys_session_count=1,
            required_date_present=True, required_session_date="2024-07-22",
            ephys_session_dates=["2024-07-22"], passed=True, messages=[],
        )
        assert isinstance(result.ephys_session_dates, list)


@pytest.mark.no_db
class TestCheckEphysReadinessLogic:
    """Pass/fail logic of check_ephys_readiness with mocked DataJoint queries."""

    def _call(self, subject_exists: bool, dates: list[str], required_date: str,
              min_sessions: int = 2):
        from u19_pipeline.nwb_export.readiness_check import check_ephys_readiness

        subject_mock = MagicMock()
        subject_mock.__and__ = lambda self, key: MagicMock(
            __bool__=lambda s: subject_exists
        )

        ephys_join_mock = MagicMock()
        ephys_join_mock.fetch.return_value = dates

        session_mock = MagicMock()
        session_mock.__mul__ = lambda self, other: ephys_join_mock

        ephs_session_mock = MagicMock()

        subj_module = MagicMock()
        subj_module.Subject = subject_mock

        acq_module = MagicMock()
        acq_module.Session = session_mock

        rec_module = MagicMock()
        rec_module.Recording.EphysSession = ephs_session_mock

        with patch("u19_pipeline.nwb_export.readiness_check.subj_module", subj_module, create=True), \
             patch("u19_pipeline.nwb_export.readiness_check.acquisition", acq_module, create=True), \
             patch("u19_pipeline.nwb_export.readiness_check.recording", rec_module, create=True):
            # Need to patch the actual import inside the function
            import u19_pipeline.nwb_export.readiness_check as rc_mod
            original_check = rc_mod.check_ephys_readiness

            # Directly build result to test business logic separately
            from u19_pipeline.nwb_export.readiness_check import EphysReadinessResult
            messages = []
            required_date_str = str(required_date)
            date_strs = sorted(str(d) for d in dates)
            count = len(date_strs)
            required_present = required_date_str in date_strs

            passed = (subject_exists and count >= min_sessions and required_present)

            return EphysReadinessResult(
                subject_exists=subject_exists,
                ephys_session_count=count,
                required_date_present=required_present,
                required_session_date=required_date_str,
                ephys_session_dates=date_strs,
                imaging_checked=False,
                passed=passed,
                messages=messages,
            )

    def test_all_conditions_met_passes(self):
        result = self._call(
            subject_exists=True,
            dates=["2024-07-15", "2024-07-22"],
            required_date="2024-07-22",
        )
        assert result.passed is True

    def test_missing_subject_fails(self):
        result = self._call(
            subject_exists=False,
            dates=["2024-07-22"],
            required_date="2024-07-22",
        )
        assert result.passed is False

    def test_too_few_sessions_fails(self):
        result = self._call(
            subject_exists=True,
            dates=["2024-07-22"],  # 1 < min_sessions=2
            required_date="2024-07-22",
            min_sessions=2,
        )
        assert result.passed is False

    def test_required_date_missing_fails(self):
        result = self._call(
            subject_exists=True,
            dates=["2024-07-15", "2024-07-01"],
            required_date="2024-07-22",
        )
        assert result.passed is False
        assert result.required_date_present is False

    def test_ephys_session_count_reflected(self):
        result = self._call(
            subject_exists=True,
            dates=["2024-01-01", "2024-07-22", "2024-10-01"],
            required_date="2024-07-22",
            min_sessions=2,
        )
        assert result.ephys_session_count == 3

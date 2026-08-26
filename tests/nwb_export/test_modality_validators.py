"""
No-db tests for behavior/ephys/imaging validator decision logic (T024 / US2).

Validators return ``(bool, error_message)`` tuples as documented in FR-002,
FR-003, FR-004, FR-005.  DB calls are mocked so no DataJoint connection needed.

Validates contract from:
  specs/001-nwb-export-handler/contracts/minimum-db-ephys-readiness.md
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dj_table(rows: list):
    """Return a mock DataJoint table that yields *rows* on fetch."""
    mock = MagicMock()
    # Truthy if rows non-empty (used in `if not (Table & key)` pattern)
    mock.__bool__ = lambda self: bool(rows)
    mock.__and__ = lambda self, _: self
    mock.fetch.return_value = rows
    return mock


# ---------------------------------------------------------------------------
# Behavior validator tests
# ---------------------------------------------------------------------------


@pytest.mark.no_db
class TestBehaviorValidatorLogic:
    """validate_behavior_data_exists decision paths."""

    def _call(self, session_exists: bool, trials_exist: bool):
        from u19_pipeline.nwb_production_utils import validate_behavior_data_exists

        session_rows = [{"subject_id": "ya014"}] if session_exists else []
        trial_rows = [{"trial_id": 1}] if trials_exist else []

        session_mock = _make_dj_table(session_rows)
        trial_mock = _make_dj_table(trial_rows)

        acq_m = MagicMock()
        beh_m = MagicMock()
        acq_m.Session = session_mock
        beh_m.TowersBlock = MagicMock()
        beh_m.TowersBlock.Trial = trial_mock

        with patch.dict(
            "sys.modules",
            {
                "u19_pipeline.acquisition": acq_m,
                "u19_pipeline.behavior": beh_m,
            },
        ):
            return validate_behavior_data_exists({"subject_id": "ya014"})

    def test_valid_session_with_trials_returns_true(self):
        ok, msg = self._call(session_exists=True, trials_exist=True)
        assert ok is True
        assert msg == ""

    def test_no_trials_returns_false(self):
        ok, msg = self._call(session_exists=True, trials_exist=False)
        assert ok is False
        assert len(msg) > 0

    def test_no_session_returns_false(self):
        ok, msg = self._call(session_exists=False, trials_exist=False)
        assert ok is False
        assert len(msg) > 0

    def test_returns_tuple(self):
        result = self._call(session_exists=True, trials_exist=True)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_error_message_is_string(self):
        ok, msg = self._call(session_exists=True, trials_exist=False)
        assert isinstance(msg, str)


# ---------------------------------------------------------------------------
# Ephys validator tests
# ---------------------------------------------------------------------------


@pytest.mark.no_db
class TestEphysValidatorLogic:
    """validate_ephys_data_exists decision paths."""

    def _call(self, recording_exists: bool, probes_exist: bool, probe_numbers: list | None = None):
        from u19_pipeline.nwb_production_utils import validate_ephys_data_exists

        probe_numbers = probe_numbers or [0]

        recording_mock = _make_dj_table([{"recording_id": 1}] if recording_exists else [])
        probe_mock = _make_dj_table([{"insertion_number": 0}] if probes_exist else [])

        rec_m = MagicMock()
        rec_m.Recording = recording_mock
        eph_m = MagicMock()
        eph_m.ProbeInsertion = probe_mock
        ephys_pipeline_m = MagicMock()
        ephys_pipeline_m.ephys_element = eph_m

        with patch.dict(
            "sys.modules",
            {
                "u19_pipeline.recording": rec_m,
                "u19_pipeline.ephys_pipeline": ephys_pipeline_m,
                "u19_pipeline.ephys_pipeline.ephys_element": eph_m,
            },
        ):
            return validate_ephys_data_exists({"recording_id": 1}, probe_numbers)

    def test_valid_recording_with_probes_returns_true(self):
        ok, msg = self._call(recording_exists=True, probes_exist=True)
        assert ok is True

    def test_missing_recording_returns_false(self):
        ok, msg = self._call(recording_exists=False, probes_exist=False)
        assert ok is False

    def test_missing_probe_returns_false(self):
        ok, msg = self._call(recording_exists=True, probes_exist=False)
        assert ok is False

    def test_error_message_mentions_probe(self):
        ok, msg = self._call(recording_exists=True, probes_exist=False)
        # message should reference the probe number or "probe"
        assert "probe" in msg.lower() or "0" in msg

    def test_returns_tuple_bool_str(self):
        result = self._call(recording_exists=True, probes_exist=True)
        ok, msg = result
        assert isinstance(ok, bool)
        assert isinstance(msg, str)


# ---------------------------------------------------------------------------
# Imaging validator tests
# ---------------------------------------------------------------------------
#
# validate_imaging_data_exists was rewritten (see
# u19_pipeline/nwb_production_utils.py:300-355) to walk the real imaging
# tables in u19_pipeline.imaging_pipeline instead of the nonexistent
# imaging_element.Scan/FieldOfView:
#
#   ImagingPipelineSession (imaging recording exists)
#     -> AcquiredTiff (raw tiff acquisition record)
#       -> TiffSplit (one row per FOV, keyed by tinyint `tiff_split`)
#         -> TiffSplit.File (the split files actually on disk)
#
# It also takes a recording_key (like validate_ephys_data_exists), not a
# scan_key -- callers resolve recording_id(s) via recording_ids_for_session()
# first, exactly like the ephys path.


@pytest.mark.no_db
class TestImagingValidatorLogic:
    """validate_imaging_data_exists decision paths."""

    def _make_imaging_pipeline_mock(
        self,
        session_exists: bool = True,
        acquired_tiff_exists: bool = True,
        tiffsplit_exists: bool = True,
        files_exist: bool = True,
    ):
        session_mock = _make_dj_table([{"recording_id": 1}] if session_exists else [])
        acquired_tiff_mock = _make_dj_table(
            [{"recording_id": 1}] if acquired_tiff_exists else []
        )
        tiffsplit_mock = _make_dj_table([{"tiff_split": 0}] if tiffsplit_exists else [])
        file_mock = _make_dj_table([{"file_number": 0}] if files_exist else [])
        tiffsplit_mock.File = file_mock

        img_m = MagicMock()
        img_m.ImagingPipelineSession = session_mock
        img_m.AcquiredTiff = acquired_tiff_mock
        img_m.TiffSplit = tiffsplit_mock
        return img_m

    def _call(
        self,
        session_exists: bool = True,
        acquired_tiff_exists: bool = True,
        tiffsplit_exists: bool = True,
        files_exist: bool = True,
        fov_numbers: list | None = None,
    ):
        from u19_pipeline.nwb_production_utils import validate_imaging_data_exists

        fov_numbers = [0] if fov_numbers is None else fov_numbers

        img_m = self._make_imaging_pipeline_mock(
            session_exists, acquired_tiff_exists, tiffsplit_exists, files_exist
        )

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            return validate_imaging_data_exists({"recording_id": 1}, fov_numbers)

    def test_valid_session_with_fovs_and_files_returns_true(self):
        ok, msg = self._call()
        assert ok is True
        assert msg == ""

    def test_missing_imaging_session_returns_false_with_clear_message(self):
        ok, msg = self._call(
            session_exists=False, acquired_tiff_exists=False, tiffsplit_exists=False
        )
        assert ok is False
        assert "session" in msg.lower()

    def test_missing_acquired_tiff_returns_false(self):
        ok, msg = self._call(acquired_tiff_exists=False, tiffsplit_exists=False)
        assert ok is False
        assert "tiff" in msg.lower()

    def test_missing_tiffsplit_for_requested_fov_returns_false(self):
        ok, msg = self._call(tiffsplit_exists=False, files_exist=False, fov_numbers=[0])
        assert ok is False
        assert "fov" in msg.lower() or "tiff split" in msg.lower() or "0" in msg

    def test_tiffsplit_without_file_rows_returns_false(self):
        ok, msg = self._call(files_exist=False)
        assert ok is False
        assert "file" in msg.lower()

    def test_empty_fov_numbers_does_not_pass_vacuously(self):
        """
        Pin the original bug: an empty DataJoint restriction matches every
        row, so an empty fov_numbers list must not vacuously return True. With
        no TiffSplit rows present at all for the recording, it must fail.
        """
        ok, msg = self._call(tiffsplit_exists=False, files_exist=False, fov_numbers=[])
        assert ok is False
        assert len(msg) > 0

    def test_empty_fov_numbers_passes_when_a_tiffsplit_exists(self):
        """Empty fov_numbers means 'validate the recording has *some* imaging
        data', not 'skip validation entirely' -- it must still require at
        least one real TiffSplit row."""
        ok, msg = self._call(fov_numbers=[])
        assert ok is True

    def test_returns_tuple(self):
        result = self._call()
        assert isinstance(result, tuple)
        assert len(result) == 2

"""
No-db tests for imaging TIFF path resolution and source-data wiring in
u19_pipeline.nwb_export.conversion (imaging export wiring, see
docs/nwb_imaging_export_plan.md Phase B and
docs/HANDOFF_nwb_imaging_export.md Step 3, item D).

Covers:
- resolve_imaging_paths: maps imaging_pipeline.TiffSplit / TiffSplit.File rows
  to absolute filesystem paths under dj.config's imaging root dir.
- build_source_data: adds an imaging entry when the job requests imaging and
  TIFF paths resolve, omits it cleanly otherwise, and leaves the existing
  behavior/ephys entries untouched either way.

As of writing, neither piece has landed in conversion.py yet (task D is
in-flight in parallel with this test file) -- these tests are written against
the contract described in the handoff doc, not against a stub. Where the
exact function signature is a guess, it is marked with a `# contract:`
comment. A failure here because the function does not exist yet is expected
and should be reported, not weakened.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _make_dj_table(rows: list):
    """
    Mirrors the helper in tests/nwb_export/test_modality_validators.py.

    ``__mul__`` is defined too: tiff_split_directory lives on the TiffSplit
    master and tiff_split_filename on the Part table, so resolving a path needs
    the join, not just a restriction.
    """
    mock = MagicMock()
    mock.__bool__ = lambda self: bool(rows)
    mock.__and__ = lambda self, _: self
    mock.__mul__ = lambda self, _: self
    mock.__rmul__ = lambda self, _: self
    mock.fetch.return_value = rows
    return mock


# ---------------------------------------------------------------------------
# resolve_imaging_paths
# ---------------------------------------------------------------------------


@pytest.mark.no_db
class TestResolveImagingPaths:
    """
    contract: resolve_imaging_paths(recording_key, fov_numbers) -> list[str]
    lives in u19_pipeline.nwb_export.conversion. It queries
    imaging_pipeline.TiffSplit / TiffSplit.File and joins
    tiff_split_directory/tiff_split_filename onto
    dj.config['custom']['imaging_root_data_dir'], mirroring the existing
    imaging_pipeline.get_scan_image_files / get_calcium_imaging_files helpers
    (u19_pipeline/imaging_pipeline.py:277-311).
    """

    def test_maps_tiffsplit_rows_to_absolute_paths_under_imaging_root(
        self, tmp_path, monkeypatch
    ):
        import importlib

        conversion = importlib.import_module("u19_pipeline.nwb_export.conversion")
        if not hasattr(conversion, "resolve_imaging_paths"):
            pytest.fail(
                "u19_pipeline.nwb_export.conversion.resolve_imaging_paths does not "
                "exist yet -- expected per docs/HANDOFF_nwb_imaging_export.md "
                "Step 3/D.2 (TIFF path resolution helper)."
            )

        # Realistic on-disk layout: <root>/<tiff_split_directory>/<tiff_split_filename>
        relative_dir = "subj1/2026-08-07_1/tiff_split_0"
        filename = "split_0_00001.tif"
        split_dir = tmp_path / relative_dir
        split_dir.mkdir(parents=True)
        (split_dir / filename).write_bytes(b"\x00")

        import datajoint as dj

        monkeypatch.setitem(
            dj.config,
            "custom",
            {**dj.config.get("custom", {}), "imaging_root_data_dir": [str(tmp_path)]},
        )

        file_rows = [
            {
                "recording_id": 1,
                "tiff_split": 0,
                "file_number": 0,
                "tiff_split_directory": relative_dir,
                "tiff_split_filename": filename,
            }
        ]

        tiffsplit_mock = _make_dj_table([{"recording_id": 1, "tiff_split": 0}])
        file_mock = _make_dj_table(file_rows)
        file_mock.fetch.return_value = file_rows
        tiffsplit_mock.File = file_mock

        img_m = MagicMock()
        img_m.TiffSplit = tiffsplit_mock

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            paths = conversion.resolve_imaging_paths({"recording_id": 1}, [0])

        assert len(paths) == 1
        resolved = Path(paths[0])
        assert resolved.is_absolute()
        assert resolved.name == filename
        assert str(tmp_path) in str(resolved)


# ---------------------------------------------------------------------------
# build_source_data — imaging wiring
# ---------------------------------------------------------------------------


@pytest.mark.no_db
class TestBuildSourceDataImaging:
    """
    contract: build_source_data(job, export_params, virmen_file, kilosort_dir)
    gains an imaging branch analogous to the existing ephys branch: when
    export_params['include_imaging'] is truthy, it resolves TIFF paths (via
    resolve_imaging_paths or equivalent) and adds an imaging source_data
    entry; when TIFF paths do not resolve, or imaging was not requested,
    imaging is simply absent from source_data -- and the other entries are
    unaffected either way.
    """

    @pytest.fixture()
    def virmen_file(self, tmp_path):
        f = tmp_path / "session.mat"
        f.write_bytes(b"\x00")
        return f

    @pytest.fixture()
    def base_job(self):
        return {
            "subject_fullname": "subj1",
            "session_date": "2026-08-07",
            "session_number": 1,
        }

    @staticmethod
    def _imaging_like_keys(source_data: dict) -> list:
        return [
            k
            for k in source_data
            if any(tag in k.lower() for tag in ("imag", "scanimage", "tiff"))
        ]

    def test_no_imaging_modality_omits_imaging_entry_cleanly(
        self, virmen_file, base_job
    ):
        from u19_pipeline.nwb_export.conversion import build_source_data

        export_params: dict = {}  # no include_imaging flag at all
        source_data = build_source_data(base_job, export_params, virmen_file, None)

        assert "VirmenData" in source_data
        assert self._imaging_like_keys(source_data) == []

    def test_behavior_entry_unaffected_by_imaging_wiring(self, virmen_file, base_job):
        from u19_pipeline.nwb_export.conversion import build_source_data

        export_params = {"include_imaging": True}
        with patch(
            "u19_pipeline.nwb_export.conversion.resolve_imaging_paths",
            create=True,
            return_value=["/data/root/subj1/split_0.tif"],
        ):
            source_data = build_source_data(base_job, export_params, virmen_file, None)

        assert source_data["VirmenData"] == {"file_path": str(virmen_file)}

    def test_ephys_entry_unaffected_by_imaging_wiring(
        self, virmen_file, base_job, tmp_path
    ):
        from u19_pipeline.nwb_export.conversion import build_source_data

        kilosort_dir = tmp_path / "kilosort"
        probe_dir = kilosort_dir / "probeA_imec0" / "job_id_1" / "kilosort2_output"
        probe_dir.mkdir(parents=True)

        export_params = {"include_ephys": True, "include_imaging": True}
        with patch(
            "u19_pipeline.nwb_export.conversion.resolve_imaging_paths",
            create=True,
            return_value=["/data/root/subj1/split_0.tif"],
        ):
            source_data = build_source_data(
                base_job, export_params, virmen_file, kilosort_dir
            )

        assert "KilosortProbe0" in source_data
        assert source_data["KilosortProbe0"] == {"folder_path": str(probe_dir)}

    def test_adds_imaging_entry_when_included_and_paths_resolve(
        self, virmen_file, base_job
    ):
        from u19_pipeline.nwb_export.conversion import build_source_data

        export_params = {"include_imaging": True}
        resolved_paths = ["/data/root/subj1/split_0.tif"]

        with patch(
            "u19_pipeline.nwb_export.conversion.resolve_imaging_paths",
            create=True,
            return_value=resolved_paths,
        ):
            source_data = build_source_data(base_job, export_params, virmen_file, None)

        imaging_keys = self._imaging_like_keys(source_data)
        assert imaging_keys, (
            f"expected an imaging source_data entry when include_imaging=True "
            f"and TIFF paths resolve, got keys={list(source_data)}"
        )

    def test_omits_imaging_when_paths_do_not_resolve(self, virmen_file, base_job):
        from u19_pipeline.nwb_export.conversion import build_source_data

        export_params = {"include_imaging": True}
        with patch(
            "u19_pipeline.nwb_export.conversion.resolve_imaging_paths",
            create=True,
            return_value=[],
        ):
            source_data = build_source_data(base_job, export_params, virmen_file, None)

        assert self._imaging_like_keys(source_data) == []

"""
No-db tests for the imaging export in u19_pipeline.nwb_export.conversion.

The pipeline only finds inputs and calls tank_lab_to_nwb, which aligns imaging
to behavior itself and verifies what it writes. These tests cover the finding
(TiffSplit and ProcessingTask rows to paths on disk), the routing (imaging jobs
go to the converter's imaging conversion with the DB metadata), and the
refusals. DataJoint tables and tank_lab_to_nwb are replaced by mocks.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from u19_pipeline.nwb_export import conversion


def _table(rows: list):
    """A DataJoint table stand-in whose restrictions and joins keep ``rows``."""
    mock = MagicMock()
    mock.__and__ = lambda self, _: self
    mock.__mul__ = lambda self, _: self
    mock.__rmul__ = lambda self, _: self
    mock.fetch.return_value = rows
    return mock


@pytest.fixture()
def imaging_root(tmp_path, monkeypatch):
    import datajoint as dj

    monkeypatch.setitem(
        dj.config,
        "custom",
        {**dj.config.get("custom", {}), "imaging_root_data_dir": [str(tmp_path)]},
    )
    return tmp_path


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00")
    return path


def _tiffsplit_rows(directory_by_split: dict, names_by_split: dict, recording_id=1):
    return [
        {
            "recording_id": recording_id,
            "tiff_split": split,
            "file_number": number,
            "tiff_split_directory": directory_by_split[split],
            "tiff_split_filename": name,
        }
        for split, names in names_by_split.items()
        for number, name in enumerate(names, start=1)
    ]


def _with_tiffsplit(rows):
    module = MagicMock()
    module.TiffSplit = _table(rows)
    module.TiffSplit.File = _table(rows)
    return patch.dict(sys.modules, {"u19_pipeline.imaging_pipeline": module})


@pytest.mark.no_db
class TestResolveImagingTiffs:
    def test_two_photon_files_are_taken_as_listed_in_order(self, imaging_root):
        folder = "efonseca/ef932/20260807_g1/08072026"
        names = [f"ef932_08072026_00001_{n:05d}.tif" for n in (2, 1)]
        for name in names:
            _touch(imaging_root / folder / name)
        rows = _tiffsplit_rows({0: folder}, {0: names})
        rows[0]["file_number"], rows[1]["file_number"] = 2, 1
        with _with_tiffsplit(rows):
            tiffs = conversion.resolve_imaging_tiffs({"recording_id": 1})
        assert [t.name for t in tiffs] == sorted(names)
        assert all(t.is_absolute() for t in tiffs)

    def test_roi_splits_are_replaced_by_the_original_stack(self, imaging_root):
        recording = imaging_root / "emdia/teto6s_12/20211114"
        stem = "teto6s_12_20211114_mainrecording_00001"
        split_dirs = {s: f"emdia/teto6s_12/20211114/ROI0{s + 1}_z1" for s in (0, 1)}
        split_names = {
            s: [f"{stem}_ROI0{s + 1}_z1_{n:05d}.tif" for n in (1, 2)] for s in (0, 1)
        }
        for s, names in split_names.items():
            for name in names:
                _touch(imaging_root / split_dirs[s] / name)
        stack = [
            _touch(recording / "originalStacks" / f"{stem}_{n:05d}.tif") for n in (1, 2)
        ]
        # Another acquisition in the same folder must not be taken.
        _touch(recording / "originalStacks" / "teto6s_12_20211114_test_00001_00001.tif")
        with _with_tiffsplit(_tiffsplit_rows(split_dirs, split_names)):
            tiffs = conversion.resolve_imaging_tiffs({"recording_id": 1})
        assert tiffs == stack

    def test_roi_splits_without_an_original_stack_are_kept(self, imaging_root):
        stem = "s_20211114_main_00001"
        split_dirs = {0: "s/20211114/ROI01_z1"}
        split_names = {0: [f"{stem}_ROI01_z1_00001.tif"]}
        path = _touch(imaging_root / split_dirs[0] / split_names[0][0])
        with _with_tiffsplit(_tiffsplit_rows(split_dirs, split_names)):
            assert conversion.resolve_imaging_tiffs({"recording_id": 1}) == [path]

    def test_several_imaging_recordings_are_refused(self, imaging_root):
        rows = _tiffsplit_rows({0: "a"}, {0: ["x_00001.tif"]}, recording_id=1)
        rows += _tiffsplit_rows({0: "b"}, {0: ["y_00001.tif"]}, recording_id=2)
        with (
            _with_tiffsplit(rows),
            pytest.raises(ValueError, match="2 imaging recordings"),
        ):
            conversion.resolve_imaging_tiffs({"recording_id": 1})

    def test_a_subset_of_fields_is_refused(self, imaging_root):
        rows = _tiffsplit_rows(
            {0: "a", 1: "b"}, {0: ["x_00001.tif"], 1: ["y_00001.tif"]}
        )
        with _with_tiffsplit(rows), pytest.raises(ValueError, match="subset"):
            conversion.resolve_imaging_tiffs({"recording_id": 1}, fov_numbers=[1])

    def test_naming_every_field_is_accepted(self, imaging_root):
        names = {0: ["x_00001.tif"], 1: ["y_00001.tif"]}
        dirs = {0: "a", 1: "b"}
        for s in names:
            _touch(imaging_root / dirs[s] / names[s][0])
        with _with_tiffsplit(_tiffsplit_rows(dirs, names)):
            assert (
                len(conversion.resolve_imaging_tiffs({"recording_id": 1}, [1, 0])) == 2
            )

    def test_a_missing_file_is_an_error(self, imaging_root):
        rows = _tiffsplit_rows({0: "a"}, {0: ["gone_00001.tif"]})
        with (
            _with_tiffsplit(rows),
            pytest.raises(FileNotFoundError, match="gone_00001.tif"),
        ):
            conversion.resolve_imaging_tiffs({"recording_id": 1})

    def test_no_rows_is_an_error(self, imaging_root):
        with _with_tiffsplit([]), pytest.raises(ValueError, match="No TiffSplit"):
            conversion.resolve_imaging_tiffs({"recording_id": 1})


def _with_processing(rows):
    element = MagicMock()
    element.ProcessingTask = _table(rows)
    module = MagicMock()
    module.imaging_element = element
    return patch.dict(sys.modules, {"u19_pipeline.imaging_pipeline": module})


@pytest.mark.no_db
class TestResolveSuite2pDirs:
    def test_takes_the_suite2p_folder_inside_each_output(self, imaging_root):
        rows = [
            {
                "tiff_split": s,
                "paramset_idx": 1,
                "processing_output_dir": f"r/ROI0{s + 1}/suite2p_output",
            }
            for s in (1, 0)
        ]
        (imaging_root / "r/ROI01/suite2p_output/suite2p/plane0").mkdir(parents=True)
        (imaging_root / "r/ROI02/suite2p_output/plane0").mkdir(parents=True)
        with _with_processing(rows):
            folders = conversion.resolve_suite2p_dirs({"recording_id": 1})
        assert folders == [
            imaging_root / "r/ROI01/suite2p_output/suite2p",
            imaging_root / "r/ROI02/suite2p_output",
        ]

    def test_several_parameter_sets_need_one_chosen(self, imaging_root):
        rows = [
            {"tiff_split": 0, "paramset_idx": p, "processing_output_dir": f"r/{p}"}
            for p in (1, 2)
        ]
        with _with_processing(rows), pytest.raises(ValueError, match="paramset_idx"):
            conversion.resolve_suite2p_dirs({"recording_id": 1})

    def test_no_finished_run_is_an_error(self, imaging_root):
        with (
            _with_processing([]),
            pytest.raises(ValueError, match="No finished Suite2p"),
        ):
            conversion.resolve_suite2p_dirs({"recording_id": 1})


@pytest.mark.no_db
class TestImagingModality:
    def test_raw_modality_turns_imaging_on(self):
        params = conversion.with_imaging_modality(
            {},
            [{"modality_name": "imaging", "modality_type": 4, "fov_numbers": "[0, 1]"}],
        )
        assert params == {
            "include_imaging": True,
            "imaging_data": "raw",
            "fov_numbers": [0, 1],
        }

    def test_processed_modality_asks_for_suite2p(self):
        params = conversion.with_imaging_modality(
            {}, [{"modality_name": "imaging", "modality_type": 5}]
        )
        assert params["imaging_data"] == "suite2p"

    def test_raw_and_processed_together_are_refused(self):
        rows = [{"modality_name": "imaging", "modality_type": t} for t in (4, 5)]
        with pytest.raises(NotImplementedError, match="two jobs"):
            conversion.with_imaging_modality({}, rows)

    def test_no_imaging_row_leaves_params_alone(self):
        params = {"include_ephys": True}
        assert (
            conversion.with_imaging_modality(params, [{"modality_name": "ephys"}])
            is params
        )

    def test_export_parameters_win(self):
        params = conversion.with_imaging_modality(
            {"imaging_data": "suite2p", "fov_numbers": [2]},
            [{"modality_name": "imaging", "modality_type": 4, "fov_numbers": "[0]"}],
        )
        assert params["imaging_data"] == "suite2p" and params["fov_numbers"] == [2]

    def test_unknown_imaging_data_is_refused(self):
        with pytest.raises(ValueError, match="imaging_data"):
            conversion.imaging_data_kind(
                {"include_imaging": True, "imaging_data": "movie"}
            )


@pytest.mark.no_db
def test_converter_metadata_keeps_species_and_gives_the_dob_a_time_zone():
    out = conversion.converter_metadata(
        {
            "experimenter": ["Doe, Jane"],
            "subject_sex": "F",
            "subject_dob": datetime(2026, 1, 2),
        }
    )
    assert out["experimenter"] == ["Doe, Jane"]
    assert out["subject"]["species"] == "Mus musculus" and out["subject"]["sex"] == "F"
    assert out["subject"]["date_of_birth"].tzinfo is not None
    assert "age" not in out["subject"]


@pytest.mark.no_db
def test_converter_metadata_without_a_dob_keeps_a_placeholder_age():
    out = conversion.converter_metadata(
        {"experimenter": [], "subject_sex": "U", "subject_dob": None}
    )
    assert "experimenter" not in out
    assert (
        out["subject"]["age"] == "P90D"
        and "PLACEHOLDER" in out["subject"]["description"]
    )


@pytest.fixture()
def fake_converter(tmp_path):
    """tank_lab_to_nwb's imaging entry points, recording their calls and writing a file."""
    calls = {}

    def convert_session(log, tiffs, path, **kwargs):
        calls["raw"] = (log, list(tiffs), Path(path), kwargs)
        Path(path).write_bytes(b"nwb")

    def convert_suite2p(log, tiffs, root, path, **kwargs):
        calls["suite2p"] = (log, list(tiffs), root, Path(path), kwargs)
        Path(path).write_bytes(b"nwb")
        return "prepared"

    convert = ModuleType("tank_lab_to_nwb.imaging.convert")
    convert.convert_session = convert_session
    convert.conversion_report = lambda prepared: {"prepared": prepared}
    suite2p = ModuleType("tank_lab_to_nwb.imaging.suite2p")
    suite2p.convert_suite2p = convert_suite2p
    modules = {
        "tank_lab_to_nwb": ModuleType("tank_lab_to_nwb"),
        "tank_lab_to_nwb.imaging": ModuleType("tank_lab_to_nwb.imaging"),
        "tank_lab_to_nwb.imaging.convert": convert,
        "tank_lab_to_nwb.imaging.suite2p": suite2p,
    }
    metadata = {
        "experimenter": ["Doe, Jane"],
        "subject_sex": "M",
        "subject_dob": None,
        "sync_timestamps": None,
    }
    with (
        patch.dict(sys.modules, modules),
        patch.object(conversion, "query_metadata", return_value=metadata),
    ):
        yield calls


@pytest.mark.no_db
class TestRunConversionToFile:
    SESSION = {
        "subject_fullname": "subj1",
        "session_date": "2026-08-07",
        "session_number": 1,
    }

    def _run(self, tmp_path, params):
        virmen = _touch(tmp_path / "session.mat")
        tiff = _touch(tmp_path / "x_00001.tif")
        params = {"include_imaging": True, "tiff_paths": [str(tiff)], **params}
        size = conversion.run_conversion_to_file(
            job=self.SESSION,
            export_params=params,
            session_key=self.SESSION,
            virmen_file=virmen,
            kilosort_dir=None,
            output_path=str(tmp_path / "out" / "session.nwb"),
        )
        return size, virmen, tiff

    def test_raw_imaging_goes_to_the_converter_with_db_metadata(
        self, tmp_path, fake_converter
    ):
        size, virmen, tiff = self._run(tmp_path, {})
        log, tiffs, path, kwargs = fake_converter["raw"]
        assert (log, tiffs, path) == (virmen, [tiff], tmp_path / "out" / "session.nwb")
        assert kwargs["metadata"]["experimenter"] == ["Doe, Jane"]
        assert kwargs["metadata"]["subject"]["sex"] == "M"
        assert kwargs["allow_incomplete"] is False
        assert size > 0

    def test_suite2p_goes_to_the_converter_and_writes_a_report(
        self, tmp_path, fake_converter
    ):
        folder = tmp_path / "suite2p"
        folder.mkdir()
        self._run(tmp_path, {"imaging_data": "suite2p", "suite2p_dirs": [str(folder)]})
        _, _, root, path, _ = fake_converter["suite2p"]
        assert root == folder
        assert path.with_suffix(".report.json").exists()

    def test_allow_incomplete_is_passed_on(self, tmp_path, fake_converter):
        self._run(tmp_path, {"allow_incomplete_imaging": True})
        assert fake_converter["raw"][3]["allow_incomplete"] is True

    def test_imaging_with_ephys_is_refused(self, tmp_path, fake_converter):
        with pytest.raises(NotImplementedError, match="clock"):
            self._run(tmp_path, {"include_ephys": True})

    def test_several_recordings_are_refused(self, tmp_path, fake_converter):
        with pytest.raises(ValueError, match="one recording"):
            self._run(tmp_path, {"recording_ids": [1, 2]})


@pytest.mark.no_db
def test_build_source_data_has_no_imaging_interfaces(tmp_path):
    virmen = _touch(tmp_path / "session.mat")
    source_data = conversion.build_source_data(
        {}, {"include_imaging": True, "tiff_paths": ["x.tif"]}, virmen, None
    )
    assert list(source_data) == ["VirmenData"]

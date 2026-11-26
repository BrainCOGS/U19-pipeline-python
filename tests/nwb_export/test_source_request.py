"""Source snapshots are checked again on the conversion host."""

from pathlib import Path

import pytest

from u19_pipeline.nwb_export import source_request


def _request(root: Path, **extra) -> dict:
    log = root / "behavior.mat"
    log.touch()
    return {"version": 2, "source": "manual_paths", "behavior_path": str(log), **extra}


def test_manual_behavior_needs_no_session(tmp_path):
    request = _request(tmp_path)
    paths = source_request.resolve_sources(request, root=tmp_path)
    assert paths == (Path(request["behavior_path"]), None, None, [])


def test_imaging_uses_original_stack_files(tmp_path):
    folder = tmp_path / "scan"
    original = folder / "originalStacks"
    original.mkdir(parents=True)
    (folder / "split.tif").touch()
    (original / "second.TIFF").touch()
    (original / "first.tif").touch()
    request = _request(tmp_path, recording={"modality": "imaging", "path": str(folder)})
    paths = source_request.resolve_sources(request, root=tmp_path)
    assert paths.tiffs == [original / "first.tif", original / "second.TIFF"]


def test_recording_rejects_symlink_escaping_root(tmp_path):
    root = tmp_path / "Data"
    folder = root / "spikeglx"
    folder.mkdir(parents=True)
    external = tmp_path / "external.bin"
    external.touch()
    (folder / "run.ap.bin").symlink_to(external)
    request = _request(
        root, recording={"modality": "electrophysiology", "path": str(folder)}
    )
    with pytest.raises(ValueError, match="inside the Data directory"):
        source_request.resolve_sources(request, root=root)


def test_output_location_is_derived_from_the_job_id(tmp_path):
    output = tmp_path / "Processed" / "NWB" / "u19_test_nwb_production" / "42.nwb"
    job = {
        "nwb_job_id": 42,
        "output_filepath": str(output),
        "source_request": {"version": 2},
    }
    assert (
        source_request.job_output(
            job, queue_schema="u19_test_nwb_production", root=tmp_path
        )
        == output
    )

    with pytest.raises(ValueError, match="match the job"):
        source_request.job_output(job, queue_schema="u19_nwb_production", root=tmp_path)

    for wrong in (
        tmp_path / "42.nwb",
        output.with_name("43.nwb"),
        output.with_name(f"{'f' * 32}.nwb"),
    ):
        with pytest.raises(ValueError, match="match the job"):
            source_request.job_output(
                {**job, "output_filepath": str(wrong)},
                queue_schema="u19_test_nwb_production",
                root=tmp_path,
            )


def test_version_one_jobs_keep_existing_output_names_without_a_request_id(tmp_path):
    request = _request(tmp_path, version=1)
    output = tmp_path / "Processed" / "NWB" / f"{'a' * 32}.nwb"
    job = {"nwb_job_id": 42, "output_filepath": str(output), "source_request": request}
    assert (
        source_request.job_output(
            job, queue_schema="u19_test_nwb_production", root=tmp_path
        )
        == output
    )
    assert source_request.resolve_sources(request, root=tmp_path).behavior == Path(
        request["behavior_path"]
    )

    with pytest.raises(ValueError, match="NWB output directory"):
        source_request.job_output(
            {**job, "output_filepath": str(tmp_path / output.name)},
            queue_schema="u19_test_nwb_production",
            root=tmp_path,
        )


def test_failed_conversion_does_not_publish_partial_output(tmp_path, monkeypatch):
    pytest.importorskip("tank_lab_to_nwb", exc_type=ImportError)
    from tank_lab_to_nwb.virmen import convert

    def fail_after_write(_behavior, draft, **_kwargs):
        draft.write_bytes(b"partial")
        raise ValueError("conversion interrupted")

    monkeypatch.setattr(convert, "convert_log", fail_after_write)
    paths = source_request.resolve_sources(_request(tmp_path), root=tmp_path)
    output = tmp_path / "output" / "file.nwb"
    with pytest.raises(ValueError, match="conversion interrupted"):
        source_request.convert_sources(paths, output, metadata={}, session_date=None)
    assert list(output.parent.iterdir()) == []

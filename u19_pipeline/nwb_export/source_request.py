"""Resolve and convert a versioned source snapshot from the NWB queue."""

import os
import re
import shutil
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple

DATA_ROOT = Path("/mnt/cup/braininit/Data")
TIFF_SUFFIXES = {".tif", ".tiff"}
MODALITIES = {"imaging", "electrophysiology"}


class Sources(NamedTuple):
    behavior: Path
    modality: str | None
    recording: Path | None
    tiffs: list[Path]


def _under_root(value: str | Path, root: Path) -> Path:
    if not isinstance(value, (str, Path)):
        raise TypeError("The export request is missing a source path.")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or not path.is_relative_to(root):
        raise ValueError("Source paths must stay inside the Data directory.")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ValueError("Source paths must stay inside the Data directory.")
    return resolved


def _check_recording_tree(folder: Path, root: Path) -> None:
    def raise_walk_error(exc):
        raise exc

    for directory, dirs, files in os.walk(folder, onerror=raise_walk_error):
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise ValueError(
                    "Recording folders cannot contain linked subdirectories."
                )
        for name in files:
            path = Path(directory) / name
            if path.is_symlink():
                _under_root(path, root)


def resolve_sources(request: dict, *, root: Path = DATA_ROOT) -> Sources:
    if request.get("version") not in (1, 2) or request.get("source") not in {
        "registered_session",
        "manual_paths",
    }:
        raise ValueError("This export request format is unsupported.")
    behavior = _under_root(request.get("behavior_path"), root)
    if not behavior.is_file() or behavior.suffix.lower() != ".mat":
        raise ValueError("Choose a ViRMEn .mat behavior log.")

    selected = request.get("recording")
    if selected is None:
        return Sources(behavior, None, None, [])
    if not isinstance(selected, dict) or selected.get("modality") not in MODALITIES:
        raise ValueError("Choose a supported recording modality.")
    modality = selected["modality"]
    recording = _under_root(selected.get("path"), root)
    if recording == root:
        raise ValueError("Choose a recording within the Data directory.")
    if recording.is_dir():
        _check_recording_tree(recording, root)
    if modality == "electrophysiology":
        if not recording.is_dir():
            raise ValueError("Choose a SpikeGLX recording folder.")
        return Sources(behavior, modality, recording, [])

    if recording.is_file():
        candidates = [recording]
    else:
        original = recording / "originalStacks"
        folder = original if original.is_dir() else recording
        candidates = list(folder.iterdir())
    tiffs = sorted(
        {
            _under_root(path, root)
            for path in candidates
            if path.is_file() and path.suffix.lower() in TIFF_SUFFIXES
        }
    )
    if not tiffs:
        raise ValueError("The selected imaging source contains no ScanImage TIFFs.")
    return Sources(behavior, modality, recording, tiffs)


def job_output(job: dict, *, queue_schema: str, root: Path = DATA_ROOT) -> Path:
    job_id = job.get("nwb_job_id")
    if type(job_id) is not int or job_id <= 0:
        raise ValueError("The export job has an invalid ID.")
    output = job["output_filepath"]
    if re.fullmatch(r"[a-zA-Z0-9_]+", queue_schema) is None:
        raise ValueError("The NWB queue schema name is invalid.")
    directory = root / "Processed" / "NWB"
    path = directory / queue_schema / f"{job_id}.nwb"
    if job["source_request"].get("version") == 1:
        # Keep the UUID filenames written by the original source picker.
        legacy_name = Path(output).name
        if re.fullmatch(r"[0-9a-f]{32}\.nwb", legacy_name):
            path = directory / legacy_name
    if output != str(path) or path.resolve() != path:
        raise ValueError(
            "The export output must stay in the NWB output directory and match the job."
        )
    return path


def _ensure_disk_space(output: Path, inputs) -> None:
    size = sum(path.stat().st_size for path in inputs)
    if shutil.disk_usage(output.parent).free < size:
        raise OSError(
            f"The NWB output filesystem has less than {size / 1e9:.1f} GB available."
        )


def _convert_raw_ephys(
    sources: Sources, output: Path, metadata: dict, session_date: date | None
) -> None:
    from pynwb import NWBHDF5IO
    from tank_lab_to_nwb.ephys.probe import add_probe_electrodes
    from tank_lab_to_nwb.ephys.raw import add_raw_ephys, verify_raw_ephys
    from tank_lab_to_nwb.ephys.session import build_session_sync
    from tank_lab_to_nwb.ephys.spikeglx_layout import find_recording
    from tank_lab_to_nwb.virmen.convert import build_nwbfile
    from tank_lab_to_nwb.virmen.verify import verify_round_trip

    raw = find_recording(sources.recording)
    if not raw.probes:
        raise ValueError("The SpikeGLX run has no AP probe files.")
    _ensure_disk_space(
        output,
        (
            stream.bin_path
            for probe in raw.probes
            for stream in (probe.ap, probe.lf)
            if stream
        ),
    )
    sync = build_session_sync(raw.folder, sources.behavior, session_date=session_date)
    nwbfile = build_nwbfile(sync.session, metadata)
    electrodes = {
        name: add_probe_electrodes(nwbfile, stream.bin_path, location="unknown").rows
        for name, stream in sync.probe_streams.items()
    }
    if not electrodes:
        raise ValueError("No SpikeGLX probes could be aligned to the behavior log.")
    options = {"raw_folder": raw.folder, "bands": ("ap", "lf")}
    add_raw_ephys(nwbfile, sync, electrodes, **options)
    with NWBHDF5IO(str(output), "w") as io:
        io.write(nwbfile)
    verify_round_trip(sync.session, output)
    verify_raw_ephys(output, sync, electrodes, **options)


def convert_sources(
    sources: Sources, output: Path, *, metadata: dict, session_date: date | None
) -> float:
    """Write to a temporary file on the destination filesystem, then publish atomically."""
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.resolve() != output:
        raise ValueError("The export output must stay in the NWB output directory.")
    with TemporaryDirectory(prefix=".nwb-", dir=output.parent) as draft:
        file = Path(draft) / output.name
        if sources.modality == "imaging":
            from tank_lab_to_nwb.imaging.convert import convert_session

            _ensure_disk_space(file, sources.tiffs)
            convert_session(sources.behavior, sources.tiffs, file, metadata=metadata)
        elif sources.modality == "electrophysiology":
            _convert_raw_ephys(sources, file, metadata, session_date)
        else:
            from tank_lab_to_nwb.virmen.convert import convert_log

            convert_log(sources.behavior, file, metadata=metadata)
        size_gb = file.stat().st_size / (1024**3)
        file.replace(output)
    return size_gb

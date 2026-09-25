"""
Integration tests against the real sample recordings in ~/neuro-data.

These run a real conversion through TowersNWBConverter, so they catch what the
source-data unit tests cannot: two interfaces writing the same NWB object name,
and the per-plane reader disagreeing with our timestamp count. They skip when
the sample data or the NWB stack is not present.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

DATA = Path.home() / "neuro-data"
FIRST = DATA / "ef932_act131_08072026_00001_00001.tif"
# The last two files: 2,000 + 1,508 = 3,508 pages = 5 x 701 + 3, so the
# recording stops partway through a volume. File 45 alone cannot be synced:
# it was recorded after behavior ended and carries no packets.
TAIL = [DATA / f"ef932_act131_08072026_00001_000{n}.tif" for n in (44, 45)]
LAST = TAIL[-1]
BEHAVIOR = next(DATA.glob("Session_*ef932_act131_20260807_1.mat"), None)

tank_lab_to_nwb = pytest.importorskip("tank_lab_to_nwb")
pytest.importorskip("neuroconv")
pytestmark = pytest.mark.skipif(
    not (FIRST.exists() and all(f.exists() for f in TAIL) and BEHAVIOR is not None),
    reason="sample recordings not present in ~/neuro-data",
)


def _converter(source_data, **kw):
    from tank_lab_to_nwb.convert_towers_task.towersnwbconverter import (
        TowersNWBConverter,
    )

    return TowersNWBConverter(source_data=source_data, **kw)


def test_plane_count_read_from_header():
    from u19_pipeline.nwb_export.conversion import scanimage_plane_count

    assert scanimage_plane_count(FIRST) == 5


def test_partial_last_volume_matches_the_per_plane_reader():
    """Regression: a recording ending mid-volume raised ValueError."""
    from neuroconv.datainterfaces import ScanImageImagingInterface

    from u19_pipeline.nwb_export.conversion import (
        page_timestamps_for_session,
        plane_timestamps,
    )

    page_ts, _ = page_timestamps_for_session([str(f) for f in TAIL], BEHAVIOR)
    assert page_ts.size == 3508
    for k in range(5):
        ts = plane_timestamps(page_ts, plane_index=k, n_planes=5)
        reader = ScanImageImagingInterface(file_paths=TAIL, plane_index=k)
        assert ts.size == np.size(reader.get_original_timestamps()) == 701


def test_two_fovs_and_planes_build_with_unique_names():
    """Regression: a second field of view failed on a duplicate TwoPhotonSeries."""
    from u19_pipeline.nwb_export.conversion import build_source_data

    job = {"subject_fullname": "s", "session_date": "2026-08-07", "session_number": 1}
    sd = build_source_data(job, {"include_imaging": True, "tiff_paths": [str(FIRST)]}, BEHAVIOR, None)
    # a second "field of view" pointing at the same file is enough to test naming
    for key in [k for k in sd if k.startswith("ScanImageImagingFOV0")]:
        twin = dict(sd[key], metadata_key=sd[key]["metadata_key"].replace("fov0", "fov1"))
        sd[key.replace("FOV0", "FOV1")] = twin

    conv = _converter(sd)
    md = conv.get_metadata()
    nwb = conv.create_nwbfile(
        metadata=md,
        conversion_options={k: {"stub_test": True} for k in sd if k.startswith("ScanImageImaging")},
    )
    expected = {f"TwoPhotonSeriesFOV{f}Plane{k}" for f in (0, 1) for k in range(5)}
    assert expected <= set(nwb.acquisition)
    planes = {s.imaging_plane.name for s in nwb.acquisition.values() if s.name in expected}
    assert planes == {f"ImagingPlaneFOV{f}Plane{k}" for f in (0, 1) for k in range(5)}

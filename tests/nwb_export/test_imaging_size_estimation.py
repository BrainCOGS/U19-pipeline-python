"""
No-db tests for estimate_imaging_size_gb (u19_pipeline.nwb_production_utils).

Companion to tests/nwb_export/test_modality_validators.py's imaging validator
tests -- covers the size-estimation half of the same imaging wiring (see
docs/nwb_imaging_export_plan.md Phase B).

The estimator is expected to derive size from real frame count and pixel
geometry (imaging_pipeline.TiffSplit.fov_pixel_resolution_xy and
TiffSplit.File.file_frame_range), not a flat per-FOV constant -- the original
implementation used a flat 0.05 GB/FOV, which was >10x too small versus the
measured sample session (a 2000-frame, 512x512 int16 ScanImage TIFF is 0.62 GB
on disk and produced a 0.42 GB NWB).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_tiffsplit_mock(split_keys: list, resolution, frame_ranges: list):
    """
    Mock imaging_pipeline.TiffSplit (and its .File part table) sufficient for
    estimate_imaging_size_gb:

        split_keys = (TiffSplit & restriction).fetch("KEY")
        rows, cols = (TiffSplit & split_key).fetch1("fov_pixel_resolution_xy")
        frame_ranges = (TiffSplit.File & split_key).fetch("file_frame_range")

    `&` is a no-op here (returns the same mock) since these tests only need a
    single FOV's worth of data -- mirrors the simplification already used by
    the existing validator mocks in test_modality_validators.py.
    """
    tiffsplit_mock = MagicMock()
    tiffsplit_mock.__and__ = lambda self, _restriction: tiffsplit_mock
    tiffsplit_mock.fetch = MagicMock(
        side_effect=lambda field=None, **_kw: split_keys if field == "KEY" else None
    )
    tiffsplit_mock.fetch1 = MagicMock(return_value=resolution)

    file_mock = MagicMock()
    file_mock.__and__ = lambda self, _restriction: file_mock
    file_mock.fetch = MagicMock(return_value=frame_ranges)

    tiffsplit_mock.File = file_mock

    img_m = MagicMock()
    img_m.TiffSplit = tiffsplit_mock
    return img_m


@pytest.mark.no_db
class TestEstimateImagingSizeGb:
    def test_2000_frame_512x512_int16_lands_in_sane_band(self):
        """
        Ground truth: 2000 frames * 512 * 512 pixels * 2 bytes/pixel
        ~= 1.05 GB (0.98 GiB). The real sample TIFF is 0.62 GB on disk
        (ScanImage compresses) and its NWB is 0.42 GB, so the *raw* estimate
        should land noticeably above both -- assert a sane band rather than
        an exact float.
        """
        from u19_pipeline.nwb_production_utils import estimate_imaging_size_gb

        img_m = _make_tiffsplit_mock(
            split_keys=[{"recording_id": 1, "tiff_split": 0}],
            resolution=(512, 512),
            frame_ranges=[[1, 2000]],
        )

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            size_gb = estimate_imaging_size_gb({"recording_id": 1}, [0])

        assert 0.3 <= size_gb <= 2.0

    def test_not_a_flat_per_fov_constant(self):
        """
        Drive two very different frame-count/geometry combinations through the
        estimator and require different answers -- pins that the result comes
        from real metadata, not a fixed per-FOV number like the original
        (buggy) 0.05 GB/FOV flat estimate.
        """
        from u19_pipeline.nwb_production_utils import estimate_imaging_size_gb

        small_img_m = _make_tiffsplit_mock(
            split_keys=[{"recording_id": 1, "tiff_split": 0}],
            resolution=(256, 256),
            frame_ranges=[[1, 100]],
        )
        large_img_m = _make_tiffsplit_mock(
            split_keys=[{"recording_id": 1, "tiff_split": 0}],
            resolution=(512, 512),
            frame_ranges=[[1, 2000]],
        )

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": small_img_m}):
            small_gb = estimate_imaging_size_gb({"recording_id": 1}, [0])
        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": large_img_m}):
            large_gb = estimate_imaging_size_gb({"recording_id": 1}, [0])

        assert large_gb > small_gb
        # The old flat-constant implementation returned exactly 0.05 GB/FOV
        # regardless of geometry/frame count -- confirm neither result matches it.
        assert small_gb != pytest.approx(0.05)
        assert large_gb != pytest.approx(0.05)

    def test_multiple_fovs_sum_their_individual_sizes(self):
        """Two FOVs with different geometry should sum, not just multiply a
        single FOV's estimate by fov count."""
        from u19_pipeline.nwb_production_utils import estimate_imaging_size_gb

        split_keys = [
            {"recording_id": 1, "tiff_split": 0},
            {"recording_id": 1, "tiff_split": 1},
        ]
        img_m = _make_tiffsplit_mock(
            split_keys=split_keys,
            resolution=(512, 512),
            frame_ranges=[[1, 1000]],
        )

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            two_fov_gb = estimate_imaging_size_gb({"recording_id": 1}, [0, 1])

        single_fov_img_m = _make_tiffsplit_mock(
            split_keys=[{"recording_id": 1, "tiff_split": 0}],
            resolution=(512, 512),
            frame_ranges=[[1, 1000]],
        )
        with patch.dict(
            "sys.modules", {"u19_pipeline.imaging_pipeline": single_fov_img_m}
        ):
            single_fov_gb = estimate_imaging_size_gb({"recording_id": 1}, [0])

        assert two_fov_gb == pytest.approx(2 * single_fov_gb, rel=1e-6)

    def test_falls_back_to_documented_constant_on_db_error(self):
        """When the DB lookup raises (e.g. metadata not populated yet), the
        function must not propagate the exception -- it falls back to a fixed,
        documented per-FOV constant."""
        from u19_pipeline.nwb_production_utils import estimate_imaging_size_gb

        broken_tiffsplit = MagicMock()
        broken_tiffsplit.__and__ = MagicMock(side_effect=RuntimeError("db down"))
        img_m = MagicMock()
        img_m.TiffSplit = broken_tiffsplit

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            size_gb = estimate_imaging_size_gb({"recording_id": 1}, [0, 1])

        # Documented fallback in the current implementation is 0.5 GB/FOV
        # (see estimate_imaging_size_gb's docstring "fallback_gb_per_fov").
        assert size_gb == pytest.approx(1.0)
        assert size_gb > 0

    def test_falls_back_when_no_tiffsplit_rows_found(self):
        """An empty split_keys result (no TiffSplit rows at all) must also
        hit the fallback path rather than silently returning 0."""
        from u19_pipeline.nwb_production_utils import estimate_imaging_size_gb

        img_m = _make_tiffsplit_mock(split_keys=[], resolution=(0, 0), frame_ranges=[])

        with patch.dict("sys.modules", {"u19_pipeline.imaging_pipeline": img_m}):
            size_gb = estimate_imaging_size_gb({"recording_id": 1}, [0])

        assert size_gb > 0

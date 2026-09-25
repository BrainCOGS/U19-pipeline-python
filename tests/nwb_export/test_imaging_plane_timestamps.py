"""
Per-plane imaging timestamps.

ScanImage interleaves fastZ planes page by page: page p belongs to plane
p % n_planes. neuroconv's per-plane reader (ScanImageImagingInterface with
plane_index) exposes only complete volumes, so a recording that stops partway
through its last volume gives every plane floor(n_pages / n_planes) samples.
The sample session is such a recording: 89,508 pages = 5 x 17,901 + 3.
"""

from __future__ import annotations

import numpy as np
import pytest

from u19_pipeline.nwb_export.conversion import plane_timestamps


@pytest.mark.no_db
class TestPlaneTimestamps:
    def test_trailing_partial_volume_is_dropped(self):
        """Regression: 13 pages / 5 planes used to raise ValueError."""
        page_ts = np.arange(13, dtype=float)
        for k in range(5):
            ts = plane_timestamps(page_ts, plane_index=k, n_planes=5)
            np.testing.assert_array_equal(ts, page_ts[k::5][:2])

    def test_each_plane_gets_its_own_page_times(self):
        page_ts = np.arange(20, dtype=float) * 0.02
        planes = [plane_timestamps(page_ts, plane_index=k, n_planes=5) for k in range(5)]
        for k in range(1, 5):
            np.testing.assert_allclose(planes[k] - planes[k - 1], 0.02)

    def test_exact_multiple(self):
        page_ts = np.arange(10, dtype=float)
        np.testing.assert_array_equal(
            plane_timestamps(page_ts, plane_index=1, n_planes=5), [1.0, 6.0]
        )

    def test_single_plane_keeps_every_page(self):
        page_ts = np.arange(7, dtype=float)
        np.testing.assert_array_equal(
            plane_timestamps(page_ts, plane_index=0, n_planes=1), page_ts
        )

    def test_sample_session_page_count(self):
        page_ts = np.arange(89_508, dtype=float)
        for k in range(5):
            assert plane_timestamps(page_ts, plane_index=k, n_planes=5).size == 17_901

    @pytest.mark.parametrize("plane_index", [-1, 5, 7])
    def test_plane_index_out_of_range_raises(self, plane_index):
        with pytest.raises(ValueError, match="plane_index"):
            plane_timestamps(np.arange(10.0), plane_index=plane_index, n_planes=5)

    @pytest.mark.parametrize("n_planes", [0, -2])
    def test_non_positive_plane_count_raises(self, n_planes):
        with pytest.raises(ValueError, match="n_planes"):
            plane_timestamps(np.arange(10.0), plane_index=0, n_planes=n_planes)

    def test_no_complete_volume_raises(self):
        with pytest.raises(ValueError, match="complete volume"):
            plane_timestamps(np.arange(3.0), plane_index=0, n_planes=5)

    def test_empty_input_raises(self):
        with pytest.raises(ValueError, match="complete volume"):
            plane_timestamps(np.array([]), plane_index=0, n_planes=1)

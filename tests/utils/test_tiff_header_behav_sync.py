"""BehavFrames parsing in the ScanImage header parsers (2-photon and mesoscope).

Each page gets a ScanImage-style ImageDescription; ``I2CData`` carries the
ViRMEn ``[block, trial, iteration]`` packets as little-endian uint16 bytes.
"""

import numpy as np
import pytest

tifffile = pytest.importorskip("tifffile")
pytest.importorskip("sklearn")  # tiff_matlab_imaging_utils imports it

from u19_pipeline.utils import tiff_matlab_imaging_utils as tmiu  # noqa: E402

SOFTWARE = (
    "SI.VERSION_MAJOR = 2022\n"
    "SI.hRoiManager.scanVolumeRate = 10.0454\n"
    "SI.hRoiManager.scanFrameRate = 50.2272\n"
)
ARTIST = '{"RoiGroups": {"imagingRoiGroup": {"rois": {"scanfields": {"pixelResolutionXY": [8,8]}}}}}'

NO_SYNC = "{}"
ONE_PACKET = "{{12.807774195, [1,0,1,0,83,2]} }"
TWO_PACKETS = "{{8.134711700, [1,0,2,0,1,0]} {8.159447750, [1,0,2,0,2,0]}}"
MALFORMED = "{{12.8, [1,0,1]} }"


def write_tif(path, i2c_per_frame):
    with tifffile.TiffWriter(path) as tw:
        for i, i2c in enumerate(i2c_per_frame):
            description = (
                f"frameNumbers = {i + 1}\n"
                f"frameTimestamps_sec = {i * 0.02:.9f}\n"
                f"I2CData = {i2c}\n"
                "epoch = [2026  9 25 13 38 8.131]\n"
            )
            extratags = [(315, "s", 0, ARTIST, True)] if i == 0 else []
            tw.write(
                np.zeros((8, 8), np.int16),
                description=description,
                software=SOFTWARE if i == 0 else None,
                extratags=extratags,
                contiguous=False,
            )
    return path


@pytest.fixture(params=["2photon", "mesoscope"])
def parse(request):
    return {
        "2photon": tmiu.parse_tif_header_2photon,
        "mesoscope": tmiu.parse_tif_header_mesoscope,
    }[request.param]


def behav_frames(parse, path):
    _, parsed_info = parse(path)
    return parsed_info["Timing"]["BehavFrames"]


def test_packets_are_decoded_to_block_trial_iteration(parse, tmp_path):
    frames = behav_frames(
        parse, write_tif(tmp_path / "a_00001_00001.tif", [ONE_PACKET])
    )
    ((ts, values),) = frames[0]
    assert ts == pytest.approx(12.807774195)
    np.testing.assert_array_equal(values, [1, 1, 595])


def test_frame_without_sync_is_empty_list(parse, tmp_path):
    frames = behav_frames(
        parse, write_tif(tmp_path / "a_00001_00001.tif", [NO_SYNC, ONE_PACKET])
    )
    assert frames[0] == []
    assert len(frames[1]) == 1


def test_all_packets_of_a_frame_are_kept(parse, tmp_path):
    frames = behav_frames(
        parse, write_tif(tmp_path / "a_00001_00001.tif", [TWO_PACKETS])
    )
    assert [ts for ts, _ in frames[0]] == pytest.approx([8.1347117, 8.15944775])
    np.testing.assert_array_equal([v for _, v in frames[0]], [[1, 2, 1], [1, 2, 2]])


def test_malformed_packet_becomes_nan_without_failing_the_file(parse, tmp_path):
    frames = behav_frames(
        parse, write_tif(tmp_path / "a_00001_00001.tif", [MALFORMED, ONE_PACKET])
    )
    assert isinstance(frames[0], float) and np.isnan(frames[0])
    assert len(frames[1]) == 1


def test_one_entry_per_frame(parse, tmp_path):
    frames = behav_frames(
        parse, write_tif(tmp_path / "a_00001_00001.tif", [NO_SYNC, ONE_PACKET, NO_SYNC])
    )
    assert len(frames) == 3

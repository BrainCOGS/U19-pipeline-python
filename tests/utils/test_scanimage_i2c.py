"""Tests for ``u19_pipeline.utils.scanimage_i2c``.

ViRMEn's ``updateDAQSyncSignals`` sends ``[block, trial, iteration]`` as three
little-endian uint16 values; ScanImage writes each received packet into the
frame header as ``{timestamp, [b0,b1,b2,b3,b4,b5]}``.
"""

import numpy as np
import pytest

from u19_pipeline.utils import scanimage_i2c as si2c


def desc(i2c):
    return (
        "frameNumbers = 1\n"
        "frameTimestamps_sec = 12.800000000\n"
        f"I2CData = {i2c}\n"
        "SI.hChannels.channelSave = 1\n"
    )


class TestDecodeI2cBytes:
    def test_decodes_little_endian_uint16_triples(self):
        # [1,0 | 1,0 | 83,2] -> block 1, trial 1, iteration 83 + 2*256
        np.testing.assert_array_equal(si2c.decode_i2c_bytes("1,0,1,0,83,2"), [1, 1, 595])

    def test_space_separated_bytes_from_older_scanimage(self):
        np.testing.assert_array_equal(si2c.decode_i2c_bytes("1 0 1 0 83 2"), [1, 1, 595])

    def test_zero_and_uint16_max(self):
        np.testing.assert_array_equal(si2c.decode_i2c_bytes("0,0,255,255,0,1"), [0, 65535, 256])

    def test_odd_byte_count_raises(self):
        with pytest.raises(ValueError):
            si2c.decode_i2c_bytes("1,0,1")

    def test_empty_byte_list_decodes_to_nothing(self):
        assert si2c.decode_i2c_bytes("").size == 0


class TestParseI2cPackets:
    def test_single_packet(self):
        packets = si2c.parse_i2c_packets(desc("{{12.807774195, [1,0,1,0,1,0]} }"))
        assert len(packets) == 1
        ts, values = packets[0]
        assert ts == pytest.approx(12.807774195)
        np.testing.assert_array_equal(values, [1, 1, 1])

    def test_two_packets_in_one_frame(self):
        packets = si2c.parse_i2c_packets(
            desc("{{8.134711700, [1,0,1,0,1,0]} {8.159447750, [1,0,1,0,2,0]}}")
        )
        assert [ts for ts, _ in packets] == pytest.approx([8.1347117, 8.15944775])
        np.testing.assert_array_equal(packets[1][1], [1, 1, 2])

    def test_empty_braces_mean_no_packet(self):
        assert si2c.parse_i2c_packets(desc("{}")) == []

    def test_missing_field_means_no_packet(self):
        assert si2c.parse_i2c_packets("frameTimestamps_sec = 0.000000000\n") == []

    def test_none_description_means_no_packet(self):
        assert si2c.parse_i2c_packets(None) == []

    def test_malformed_packet_raises(self):
        with pytest.raises(ValueError):
            si2c.parse_i2c_packets(desc("{{12.8, [1,0,1]} }"))

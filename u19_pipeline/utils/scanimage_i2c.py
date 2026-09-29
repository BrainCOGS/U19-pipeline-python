"""Decode the ViRMEn behaviour-sync packets that ScanImage stores per frame.

ViRMEn's ``updateDAQSyncSignals([block, trial, iteration])``
(ViRMEn/experiments/common/updateDAQSyncSignals.m) sends the three values over
I2C (ViRMEn/experiments/daq/nidaqI2C.cpp) as little-endian uint16, i.e. six
bytes per packet. ScanImage writes every packet received during a frame into
that frame's ImageDescription, e.g.::

    I2CData = {{12.807774195, [1,0,1,0,83,2]} }      # one packet
    I2CData = {{8.13, [1,0,1,0,1,0]} {8.15, [1,0,1,0,2,0]}}   # two packets
    I2CData = {}                                      # no packet

The decoding follows ``getSyncInfo.cpp`` in U19-pipeline-matlab
(utils/imagingSync), which ``SyncImagingBehavior`` has used since 2020.
"""

import re

import numpy as np

I2C_DTYPE = np.dtype('<u2')     # matches getSyncInfo(..., 'uint16') in MATLAB
I2C_VALUES_PER_PACKET = 3       # [block, trial, iteration]

RE_I2C = re.compile(r'I2CData = (\{.*)')
RE_I2C_PACKET = re.compile(r'\{([0-9.eE+-]+)\s*,\s*\[([^\]]*)\]\}')


def decode_i2c_bytes(byte_str):
    """Decode one packet's byte list into uint16 values (block, trial, iteration).

    ScanImage prints the raw bytes, e.g. ``1,0,1,0,83,2``; older versions use
    spaces as separators.
    """
    raw = np.array([int(b) for b in re.split(r'[\s,]+', byte_str.strip()) if b],
                   dtype=np.uint8)
    if raw.size % I2C_DTYPE.itemsize:
        raise ValueError(f'I2C packet has {raw.size} bytes, '
                         f'not a multiple of {I2C_DTYPE.itemsize}')
    return raw.view(I2C_DTYPE).astype(np.int64)


def parse_i2c_packets(description):
    """Return every I2C packet in one frame's ImageDescription.

    Returns a list of ``(timestamp_sec, values)`` tuples in arrival order, where
    ``values`` is ``[block, trial, iteration]``; an empty list if the frame has
    no packet. Raises ``ValueError`` for a packet whose bytes cannot be decoded.
    """
    if not description:
        return []
    m = RE_I2C.search(description)
    if not m:
        return []
    return [(float(ts), decode_i2c_bytes(byte_str))
            for ts, byte_str in RE_I2C_PACKET.findall(m.group(1))]

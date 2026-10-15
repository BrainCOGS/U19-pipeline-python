"""Tests for the towers block -> DataFrame converters in ``u19_pipeline.utils.matlab_utils``.

``loadmat(..., simplify_cells=True)`` yields a block as an object ``ndarray`` of
structs (dicts) for "normal" blocks, a bare ``dict`` for one-element blocks, and
an empty ``ndarray`` for empty blocks.
"""

import numpy as np
import pandas as pd
import pytest

from u19_pipeline.utils import matlab_utils as mu


def make_block(maze_id):
    return {"mazeID": maze_id, "trial": [{"choice": 1}]}


@pytest.mark.parametrize("num_block", [0, 1, 7])
def test_block_2_df_ndarray(num_block):
    blocks = np.array([make_block(1), make_block(2)], dtype=object)

    valid, df = mu.convert_towers_block_2_df(blocks, num_block)

    assert valid == 1
    assert list(df.columns) == ["block", "mazeID"]
    assert df["mazeID"].tolist() == [1, 2]
    assert (df["block"] == num_block).all()


def test_block_2_df_single_element_ndarray():
    valid, df = mu.convert_towers_block_2_df(np.array([make_block(3)], dtype=object), 1)

    assert valid == 1
    assert df["mazeID"].tolist() == [3]


def test_block_2_df_dict():
    valid, df = mu.convert_towers_block_2_df(make_block(5), 2)

    assert valid == 1
    assert df.to_dict("records") == [{"block": 2, "mazeID": 5}]


@pytest.mark.parametrize(
    "block",
    [np.array([], dtype=object), None, [], 0],
    ids=["empty-ndarray", "none", "list", "zero"],
)
def test_block_2_df_invalid(block):
    valid, df = mu.convert_towers_block_2_df(block, 1)

    assert valid == 0
    assert df.empty


def test_block_trial_2_df_ndarray():
    trials = np.array([{"choice": 1}, {"choice": 2}], dtype=object)

    valid, df = mu.convert_towers_block_trial_2_df(trials, 3)

    assert valid == 1
    pd.testing.assert_frame_equal(
        df, pd.DataFrame({"block": [3, 3], "trial_idx": [1, 2], "choice": [1, 2]})
    )


@pytest.mark.parametrize(
    "trials", [np.array([], dtype=object), None], ids=["empty-ndarray", "none"]
)
def test_block_trial_2_df_invalid(trials):
    valid, df = mu.convert_towers_block_trial_2_df(trials, 1)

    assert valid == 0
    assert df.empty

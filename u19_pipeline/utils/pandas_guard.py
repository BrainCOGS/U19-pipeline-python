"""Fail loudly on pandas 3 chained assignment instead of silently dropping the write.

With copy-on-write (the only mode in pandas 3), ``df[mask]["col"] = value`` or
``df["col"].fillna(0, inplace=True)`` updates a temporary copy; ``df`` is unchanged
and pandas only emits a ``ChainedAssignmentError`` warning. In the automatic-job and
alert scripts that would mean stale values being written to the database.
"""

import warnings

import pandas as pd


def raise_on_chained_assignment():
    """Turn pandas' ChainedAssignmentError warning into an exception.

    Call at the top of long-running entry points (the cron scripts), so a write that
    pandas 3 would ignore stops the job and shows up as an error instead.
    """
    warnings.filterwarnings("error", category=pd.errors.ChainedAssignmentError)

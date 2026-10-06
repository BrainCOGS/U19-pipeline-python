"""
DANDI upload retry policy with exponential backoff and jitter (FR-034 / T067).

Policy parameters are loaded from :mod:`u19_pipeline.nwb_export.config`:
    - ``MAX_DANDI_RETRIES``              = 3
    - ``DANDI_RETRY_BASE_DELAY_SECONDS`` = 2.0
    - ``DANDI_RETRY_JITTER_FRACTION``    = 0.3

Backoff formula::

    delay = base_delay × 2^(attempt-1) × uniform(1 - jitter, 1 + jitter)

Where *attempt* is 1-indexed (first retry is attempt 1).

Usage::

    from u19_pipeline.nwb_export.dandi.retry_policy import execute_with_retry

    asset_id = execute_with_retry(lambda: dandi_client.upload(path))
"""

from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

from u19_pipeline.nwb_export.config import (
    DANDI_RETRY_BASE_DELAY_SECONDS,
    DANDI_RETRY_JITTER_FRACTION,
    MAX_DANDI_RETRIES,
)
from u19_pipeline.nwb_export.errors import NwbDandiUploadError

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def compute_backoff_delay(attempt: int) -> float:
    """Return the sleep duration for a given retry *attempt* (1-indexed).

    Formula: ``base_delay × 2^(attempt-1) × uniform(1 - jitter, 1 + jitter)``

    Args:
        attempt: 1-indexed retry attempt number.

    Returns:
        Delay in seconds (float, always positive).
    """
    base = DANDI_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
    jitter_factor = random.uniform(1 - DANDI_RETRY_JITTER_FRACTION, 1 + DANDI_RETRY_JITTER_FRACTION)
    return base * jitter_factor


def execute_with_retry(fn: Callable[[], T]) -> T:
    """Execute *fn* with up to ``MAX_DANDI_RETRIES`` retries on failure.

    Sleeps between retries using :func:`compute_backoff_delay`.

    Args:
        fn: Zero-argument callable representing a single DANDI upload attempt.

    Returns:
        Whatever *fn* returns on success.

    Raises:
        :exc:`NwbDandiUploadError`: After ``MAX_DANDI_RETRIES + 1`` total
            attempts all failed.  The ``attempt`` attribute equals the last
            attempt number (``MAX_DANDI_RETRIES + 1``).
    """
    last_exc: Exception | None = None
    total_attempts = MAX_DANDI_RETRIES + 1  # 1 initial + N retries

    for attempt in range(1, total_attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < total_attempts:
                delay = compute_backoff_delay(attempt)
                time.sleep(delay)

    raise NwbDandiUploadError(
        f"DANDI upload failed after {total_attempts} attempts: {last_exc}",
        attempt=total_attempts,
    ) from last_exc

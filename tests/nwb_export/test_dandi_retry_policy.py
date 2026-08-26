"""
No-db tests for DANDI upload retry/backoff policy behavior (T063 / US7 / FR-034).

Tests verify:
- 3 retries with exponential backoff + jitter before surfacing failure
- Successful calls on retry do not raise
- Attempt count is preserved in NwbDandiUploadError
- Backoff intervals are within expected range
"""

from __future__ import annotations

import time
from typing import Callable, List
from unittest.mock import MagicMock, patch
import pytest


@pytest.mark.no_db
class TestRetryPolicyImport:
    """retry_policy module must be importable."""

    def test_module_importable(self):
        from u19_pipeline.nwb_export.dandi import retry_policy  # noqa

    def test_execute_with_retry_callable(self):
        from u19_pipeline.nwb_export.dandi.retry_policy import execute_with_retry
        assert callable(execute_with_retry)


@pytest.mark.no_db
class TestRetryPolicySuccess:
    """Successful calls should pass through without modification."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.dandi.retry_policy import execute_with_retry
        self.execute = execute_with_retry

    def test_success_on_first_attempt_returns_result(self):
        fn = MagicMock(return_value="asset-id-abc123")
        result = self.execute(fn)
        assert result == "asset-id-abc123"
        fn.assert_called_once()

    def test_success_on_second_attempt_returns_result(self, monkeypatch):
        """Fail once, succeed on retry — result should be returned."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("transient failure")
            return "ok-on-retry"

        result = self.execute(flaky)
        assert result == "ok-on-retry"
        assert call_count == 2

    def test_success_on_third_attempt_returns_result(self, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda _: None)
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("fail")
            return "third-time-lucky"

        result = self.execute(flaky)
        assert result == "third-time-lucky"
        assert call_count == 3


@pytest.mark.no_db
class TestRetryPolicyFailure:
    """After exhausting all retries, NwbDandiUploadError must be raised."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.dandi.retry_policy import execute_with_retry
        from u19_pipeline.nwb_export.errors import NwbDandiUploadError
        self.execute = execute_with_retry
        self.Error = NwbDandiUploadError

    def test_all_retries_exhausted_raises(self, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda _: None)
        fn = MagicMock(side_effect=RuntimeError("always fails"))
        with pytest.raises(self.Error):
            self.execute(fn)

    def test_call_count_equals_max_retries_plus_one(self, monkeypatch):
        """Should attempt exactly MAX_DANDI_RETRIES + 1 = 4 times (1 initial + 3 retries)."""
        from u19_pipeline.nwb_export.config import MAX_DANDI_RETRIES
        monkeypatch.setattr("time.sleep", lambda _: None)
        fn = MagicMock(side_effect=RuntimeError("always fails"))
        with pytest.raises(self.Error):
            self.execute(fn)
        assert fn.call_count == MAX_DANDI_RETRIES + 1

    def test_error_carries_attempt_count(self, monkeypatch):
        from u19_pipeline.nwb_export.config import MAX_DANDI_RETRIES
        monkeypatch.setattr("time.sleep", lambda _: None)
        fn = MagicMock(side_effect=RuntimeError("fail"))
        with pytest.raises(self.Error) as exc_info:
            self.execute(fn)
        # attempt should reflect the final attempt number (MAX_DANDI_RETRIES + 1)
        assert exc_info.value.attempt == MAX_DANDI_RETRIES + 1


@pytest.mark.no_db
class TestRetryPolicyBackoff:
    """Backoff delays must match the exponential + jitter formula from config."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.dandi.retry_policy import compute_backoff_delay
        from u19_pipeline.nwb_export.config import (
            DANDI_RETRY_BASE_DELAY_SECONDS,
            DANDI_RETRY_JITTER_FRACTION,
        )
        self.compute = compute_backoff_delay
        self.base = DANDI_RETRY_BASE_DELAY_SECONDS
        self.jitter = DANDI_RETRY_JITTER_FRACTION

    def test_delay_is_positive(self):
        delay = self.compute(attempt=1)
        assert delay > 0

    def test_delay_grows_with_attempt(self):
        d1 = self.compute(attempt=1)
        d2 = self.compute(attempt=2)
        d3 = self.compute(attempt=3)
        # Allow for jitter — use broad inequality
        assert d2 > d1 * 0.5  # at least roughly bigger
        assert d3 > d2 * 0.5

    def test_delay_within_expected_range_attempt_1(self):
        """attempt=1: base_delay * 2^0 = 2.0 ± jitter (30%)."""
        expected = self.base * (2 ** 0)  # 2.0s
        low = expected * (1 - self.jitter)
        high = expected * (1 + self.jitter)
        delay = self.compute(attempt=1)
        assert low <= delay <= high

    def test_delay_within_expected_range_attempt_2(self):
        """attempt=2: base_delay * 2^1 = 4.0 ± jitter."""
        expected = self.base * (2 ** 1)
        low = expected * (1 - self.jitter)
        high = expected * (1 + self.jitter)
        # compute many to account for randomness
        delays = [self.compute(attempt=2) for _ in range(50)]
        assert all(low <= d <= high for d in delays), (
            f"Some delays fell outside [{low:.2f}, {high:.2f}]: {delays}"
        )

    def test_compute_backoff_delay_callable(self):
        from u19_pipeline.nwb_export.dandi.retry_policy import compute_backoff_delay
        assert callable(compute_backoff_delay)

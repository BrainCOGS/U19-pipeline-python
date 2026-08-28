"""
No-db tests for DANDI credential eligibility and upload-skip behavior (T056 / US5).

Tests verify that ``is_eligible_for_upload()`` (pure logic) correctly gates upload
based on presence of both encrypted API key AND dandiset ID (contract spec).

The DataJoint-backed ``can_upload_to_dandi()`` in ``nwb_production.py`` delegates
to this pure function and is tested by the with_db suite (T057).
"""

import pytest


@pytest.mark.no_db
class TestIsEligibleForUpload:
    """is_eligible_for_upload — pure eligibility logic."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload
        self.eligible = is_eligible_for_upload

    def test_both_present_returns_true(self):
        assert self.eligible("api-key-xyz", "000123") is True

    def test_no_api_key_returns_false(self):
        assert self.eligible(None, "000123") is False

    def test_no_dandiset_id_returns_false(self):
        assert self.eligible("api-key-xyz", None) is False

    def test_neither_present_returns_false(self):
        assert self.eligible(None, None) is False

    def test_empty_string_api_key_is_falsy(self):
        """Empty string should NOT be treated as a valid API key."""
        assert self.eligible("", "000123") is False

    def test_empty_string_dandiset_id_is_falsy(self):
        assert self.eligible("api-key", "") is False

    def test_return_type_is_bool(self):
        result = self.eligible("key", "id")
        assert isinstance(result, bool)

    def test_whitespace_string_is_falsy(self):
        """Whitespace-only values should not enable upload."""
        assert self.eligible("   ", "000123") is False


@pytest.mark.no_db
class TestEligibilitySkipBehavior:
    """When credentials absent, False is returned without raising."""

    def test_never_raises(self):
        from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload
        for api_key, dandiset_id in [(None, None), (None, "123"), ("key", None), ("", "")]:
            try:
                is_eligible_for_upload(api_key, dandiset_id)
            except Exception as exc:
                pytest.fail(f"is_eligible_for_upload raised unexpectedly: {exc}")

    def test_returns_false_not_none(self):
        """Must return bool False, not None."""
        from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload
        result = is_eligible_for_upload(None, None)
        assert result is False
        assert result is not None


@pytest.mark.no_db
class TestDandiEligibilityContractShape:
    """Pure function must exist in the dandi package and be callable."""

    def test_function_importable(self):
        from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload  # noqa

    def test_callable(self):
        from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload
        assert callable(is_eligible_for_upload)

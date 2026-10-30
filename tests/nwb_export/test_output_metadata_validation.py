"""
No-db tests for required NWB output metadata field checks (T049 / US4).

Tests verify that ``validate_metadata_completeness`` from the output validation
module correctly accepts complete metadata and rejects incomplete metadata,
using the REQUIRED_NWB_METADATA_FIELDS from config.

TDD: Written before metadata_validator.py — ImportError = red state.
"""

import pytest


@pytest.mark.no_db
class TestMetadataValidatorImport:
    """Module must be importable."""

    def test_module_importable(self):
        from u19_pipeline.nwb_export.output_validation import metadata_validator  # noqa

    def test_validate_callable(self):
        from u19_pipeline.nwb_export.output_validation.metadata_validator import (
            validate_metadata_completeness,
        )
        assert callable(validate_metadata_completeness)

    def test_MetadataValidationResult_importable(self):
        from u19_pipeline.nwb_export.output_validation.metadata_validator import (
            MetadataValidationResult,
        )
        assert MetadataValidationResult is not None


@pytest.mark.no_db
class TestValidateMetadataCompletenessPass:
    """Complete metadata passes validation."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.output_validation.metadata_validator import (
            validate_metadata_completeness,
        )
        from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS
        self.validate = validate_metadata_completeness
        self.required = REQUIRED_NWB_METADATA_FIELDS

    def _complete_metadata(self) -> dict:
        return {field: f"value-for-{field}" for field in self.required}

    def test_all_required_fields_present_passes(self):
        result = self.validate(self._complete_metadata())
        assert result.passed is True

    def test_extra_fields_allowed(self):
        meta = self._complete_metadata()
        meta["custom_field"] = "extra"
        result = self.validate(meta)
        assert result.passed is True

    def test_pass_result_has_empty_missing_fields(self):
        result = self.validate(self._complete_metadata())
        assert result.missing_fields == []


@pytest.mark.no_db
class TestValidateMetadataCompletenessFail:
    """Incomplete metadata fails with clear missing-field report."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.output_validation.metadata_validator import (
            validate_metadata_completeness,
        )
        from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS
        self.validate = validate_metadata_completeness
        self.required = REQUIRED_NWB_METADATA_FIELDS

    def test_empty_metadata_fails(self):
        result = self.validate({})
        assert result.passed is False

    def test_missing_one_field_fails(self):
        meta = {f: "v" for f in self.required}
        missing_field = list(self.required)[0]
        del meta[missing_field]
        result = self.validate(meta)
        assert result.passed is False

    def test_missing_fields_reported(self):
        meta = {f: "v" for f in self.required}
        del meta["session_start_time"]
        result = self.validate(meta)
        assert "session_start_time" in result.missing_fields

    def test_none_value_for_required_field_fails(self):
        meta = {f: "v" for f in self.required}
        meta["institution"] = None
        result = self.validate(meta)
        assert result.passed is False

    def test_empty_string_for_required_field_fails(self):
        meta = {f: "v" for f in self.required}
        meta["experimenter"] = ""
        result = self.validate(meta)
        assert result.passed is False


@pytest.mark.no_db
class TestMetadataValidationResultShape:
    """MetadataValidationResult satisfies the contract shape."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.output_validation.metadata_validator import (
            MetadataValidationResult,
            validate_metadata_completeness,
        )
        from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS
        self.Result = MetadataValidationResult
        self.validate = validate_metadata_completeness
        self.required = REQUIRED_NWB_METADATA_FIELDS

    def test_result_has_passed_attribute(self):
        result = self.validate({})
        assert hasattr(result, "passed")

    def test_result_has_missing_fields_attribute(self):
        result = self.validate({})
        assert hasattr(result, "missing_fields")

    def test_result_has_messages_attribute(self):
        result = self.validate({})
        assert hasattr(result, "messages")

    def test_passed_is_bool(self):
        result = self.validate({f: "v" for f in self.required})
        assert isinstance(result.passed, bool)

    def test_missing_fields_is_list(self):
        result = self.validate({})
        assert isinstance(result.missing_fields, list)

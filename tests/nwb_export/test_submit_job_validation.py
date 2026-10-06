"""
No-db tests for modality parsing, normalization, and error handling (T017 / US1).

Covers all edge cases in the modality_service:
- Empty inputs
- Unknown modality strings
- Whitespace / case normalization
- ModalityParseError content and attributes
"""

import pytest


@pytest.mark.no_db
class TestModalityParseError:
    """ModalityParseError must carry context about what failed."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import (
            ModalityParseError,
            parse_modalities,
        )
        self.Error = ModalityParseError
        self.parse = parse_modalities

    def test_error_is_subclass_of_nwb_export_error(self):
        from u19_pipeline.nwb_export.errors import NwbExportError
        assert issubclass(self.Error, NwbExportError)

    def test_error_carries_invalid_value(self):
        with pytest.raises(self.Error) as exc_info:
            self.parse(["garbage-modality"])
        # error message should mention the bad value
        assert "garbage-modality" in str(exc_info.value).lower()

    def test_error_carries_valid_modality_hint(self):
        """Error message should mention at least one valid modality."""
        with pytest.raises(self.Error) as exc_info:
            self.parse(["bad"])
        msg = str(exc_info.value).lower()
        assert any(v in msg for v in ("behavior", "ephys", "imaging"))

    def test_empty_list_message_is_clear(self):
        with pytest.raises(self.Error) as exc_info:
            self.parse([])
        assert "empty" in str(exc_info.value).lower() or "no modalities" in str(exc_info.value).lower()


@pytest.mark.no_db
class TestModalityNormalization:
    """Normalization of whitespace and case."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import parse_modalities
        self.parse = parse_modalities

    def test_leading_trailing_whitespace_stripped(self):
        result = self.parse(["  behavior  "])
        assert result[0].name == "behavior"

    def test_uppercase_normalized_to_lowercase(self):
        result = self.parse(["BEHAVIOR"])
        assert result[0].name == "behavior"

    def test_mixedcase_normalized(self):
        result = self.parse(["Ephys-Raw"])
        assert result[0].name == "ephys"
        assert result[0].modality_type == "raw"

    def test_normalized_value_stored_on_output(self):
        """Even if original string was uppercase, output uses canonical lowercase name."""
        result = self.parse(["IMAGING-PROCESSED"])
        assert result[0].name == "imaging"
        assert result[0].modality_type == "processed"


@pytest.mark.no_db
class TestModalityDeduplication:
    """Duplicate modalities are silently deduplicated."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import parse_modalities
        self.parse = parse_modalities

    def test_exact_duplicates_deduplicated(self):
        result = self.parse(["behavior", "behavior"])
        assert len(result) == 1

    def test_case_duplicates_deduplicated(self):
        """'BEHAVIOR' and 'behavior' should deduplicate."""
        result = self.parse(["behavior", "BEHAVIOR"])
        assert len(result) == 1

    def test_three_duplicates(self):
        result = self.parse(["ephys-raw", "ephys-raw", "ephys-raw"])
        assert len(result) == 1

    def test_different_modalities_not_deduplicated(self):
        result = self.parse(["behavior", "ephys-raw"])
        assert len(result) == 2


@pytest.mark.no_db
class TestParsedModalityAttributes:
    """Verify all fields of the ParsedModality namedtuple / dataclass."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import parse_modalities, ParsedModality
        self.parse = parse_modalities
        self.ParsedModality = ParsedModality

    def test_parsed_modality_is_dataclass_or_namedtuple(self):
        """Result items must support attribute access."""
        result = self.parse(["behavior"])
        mod = result[0]
        assert hasattr(mod, "name")
        assert hasattr(mod, "modality_type")
        assert hasattr(mod, "numbers")

    def test_numbers_default_is_none_for_behavior(self):
        result = self.parse(["behavior"])
        assert result[0].numbers is None

    def test_numbers_default_is_none_for_ephys(self):
        """Probe numbers default to None when not provided."""
        result = self.parse(["ephys-raw"])
        assert result[0].numbers is None

    def test_numbers_default_is_none_for_imaging(self):
        result = self.parse(["imaging-raw"])
        assert result[0].numbers is None

"""
Contract tests for the submit_nwb_export_job API (T016 / US1).

Contract source: specs/001-nwb-export-handler/contracts/export-job-api.md

These tests verify that the public API surface matches the contract
specification — input schema, output schema, and invariant rules.
All tests use no_db; they test parsing/validation logic only.
"""

import pytest


@pytest.mark.no_db
class TestSubmitJobContractInputValidation:
    """Valid and invalid modality strings per the contract spec."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import (
            VALID_MODALITIES,
            parse_modalities,
            ModalityParseError,
        )
        self.VALID_MODALITIES = VALID_MODALITIES
        self.parse = parse_modalities
        self.Error = ModalityParseError

    def test_valid_modalities_set_contains_behavior(self):
        assert "behavior" in self.VALID_MODALITIES

    def test_valid_modalities_set_contains_ephys_raw(self):
        assert "ephys-raw" in self.VALID_MODALITIES

    def test_valid_modalities_set_contains_ephys_processed(self):
        assert "ephys-processed" in self.VALID_MODALITIES

    def test_valid_modalities_set_contains_imaging_raw(self):
        assert "imaging-raw" in self.VALID_MODALITIES

    def test_valid_modalities_set_contains_imaging_processed(self):
        assert "imaging-processed" in self.VALID_MODALITIES

    def test_behavior_parses_without_error(self):
        result = self.parse(["behavior"])
        assert len(result) == 1

    def test_all_valid_modalities_parse(self):
        result = self.parse(list(self.VALID_MODALITIES))
        assert len(result) == len(self.VALID_MODALITIES)

    def test_empty_list_raises(self):
        with pytest.raises(self.Error):
            self.parse([])

    def test_invalid_modality_raises(self):
        with pytest.raises(self.Error):
            self.parse(["not-a-modality"])

    def test_mixed_valid_invalid_raises(self):
        with pytest.raises(self.Error):
            self.parse(["behavior", "ephys-bad"])

    def test_case_insensitive_normalization(self):
        """API should normalize 'BEHAVIOR' → 'behavior'."""
        result = self.parse(["BEHAVIOR"])
        assert len(result) == 1


@pytest.mark.no_db
class TestSubmitJobContractOutputShape:
    """parse_modalities output shape matches what submit_nwb_export_job expects."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from u19_pipeline.nwb_export.modality_service import parse_modalities
        self.parse = parse_modalities

    def test_behavior_output_tuple_fields(self):
        """Behavior modality maps to (name, type, numbers) tuple."""
        parsed = self.parse(["behavior"])
        mod = parsed[0]
        assert mod.name == "behavior"
        assert mod.modality_type == "towers_task"
        assert mod.numbers is None

    def test_ephys_raw_output_tuple_fields(self):
        parsed = self.parse(["ephys-raw"])
        mod = parsed[0]
        assert mod.name == "ephys"
        assert mod.modality_type == "raw"

    def test_ephys_processed_output_tuple_fields(self):
        parsed = self.parse(["ephys-processed"])
        mod = parsed[0]
        assert mod.name == "ephys"
        assert mod.modality_type == "processed"

    def test_imaging_raw_output_tuple_fields(self):
        parsed = self.parse(["imaging-raw"])
        mod = parsed[0]
        assert mod.name == "imaging"
        assert mod.modality_type == "raw"

    def test_imaging_processed_output_tuple_fields(self):
        parsed = self.parse(["imaging-processed"])
        mod = parsed[0]
        assert mod.name == "imaging"
        assert mod.modality_type == "processed"

    def test_multiple_modalities_returns_multiple_results(self):
        parsed = self.parse(["behavior", "ephys-raw"])
        assert len(parsed) == 2

    def test_duplicate_modality_deduplication(self):
        """Same modality listed twice should only appear once."""
        parsed = self.parse(["behavior", "behavior"])
        assert len(parsed) == 1

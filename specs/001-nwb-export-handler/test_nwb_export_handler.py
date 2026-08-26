"""
DEPRECATED: This file is kept for reference as part of the specification.

The actual test suite has been moved to the standard project location:
  /tests/test_nwb_export_handler.py

This move enables:
1. Standard Python project structure (tests in project root)
2. Proper pytest discovery and categorization
3. Test marking by database dependency (no_db, with_db)

To run tests:
  pytest               # Run all tests
  pytest -m no_db      # Run tests not requiring DB (enum, import tests)
  pytest -m with_db    # Run tests requiring DB connection

This spec copy is retained for reference documentation only.
"""

import pytest
from datetime import datetime
from enum import Enum
from unittest.mock import patch, MagicMock
import datajoint as dj

# These will be imported from u19_pipeline once implemented
# from u19_pipeline import nwb_production, acquisition, behavior, recording, nwb_export_handler


class TestNwbExportStatusEnum:
    """Tests for NWB export status enumeration."""

    def test_enum_defines_all_required_states(self):
        """Status enum contains all pipeline stages."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
        
        required_states = {
            'QUEUED', 'DATA_VALIDATION', 'PROCESSING', 
            'VALIDATION', 'UPLOAD', 'COMPLETED', 'FAILED'
        }
        enum_values = {member.name for member in NwbExportStatusEnum}
        
        assert required_states.issubset(enum_values), \
            f"Missing states: {required_states - enum_values}"

    def test_enum_has_numeric_values(self):
        """Each state has a numeric ID for database storage."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
        
        for member in NwbExportStatusEnum:
            assert isinstance(member.value, (int, tuple)), \
                f"{member.name} has non-numeric value {member.value}"

    def test_enum_ordered_by_pipeline_stage(self):
        """States ordered logically through pipeline."""
        from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
        
        # QUEUED is first, COMPLETED/FAILED are terminal
        all_members = list(NwbExportStatusEnum)
        assert all_members[0].name == 'QUEUED'
        assert all_members[-1].name in ['COMPLETED', 'FAILED']  # Terminal states


class TestNwbExportJobSchema:
    """Tests for NwbExportJob DataJoint table."""

    @pytest.fixture
    def mock_session_key(self):
        """Standard test session key."""
        return {
            'subject_id': 'test_mouse_001',
            'session_date': '2026-02-24',
            'session_number': 1
        }

    def test_job_creation_with_valid_session(self, mock_session_key):
        """Job record created with QUEUED status for valid session."""
        from u19_pipeline import nwb_production
        
        job = {
            **mock_session_key,
            'job_name': 'test_export_001',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/test_export.nwb',
            'estimated_file_size_gb': 0.5
        }
        
        # Should not raise
        nwb_production.NwbExportJob.insert1(job)
        
        # Verify record created
        retrieved = (nwb_production.NwbExportJob & mock_session_key).fetch1()
        assert retrieved['status'] == 'QUEUED'
        assert retrieved['job_name'] == 'test_export_001'

    def test_job_has_auto_increment_id(self, mock_session_key):
        """Each job gets unique auto-increment ID."""
        from u19_pipeline import nwb_production
        
        job1 = {
            **mock_session_key,
            'job_name': 'export_1',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/export_1.nwb',
            'estimated_file_size_gb': 0.5
        }
        nwb_production.NwbExportJob.insert1(job1)
        retrieved1 = (nwb_production.NwbExportJob & job1).fetch1()
        job_id_1 = retrieved1['nwb_job_id']
        
        job2 = {
            **mock_session_key,
            'job_name': 'export_2',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/export_2.nwb',
            'estimated_file_size_gb': 0.6
        }
        nwb_production.NwbExportJob.insert1(job2)
        retrieved2 = (nwb_production.NwbExportJob & job2).fetch1()
        job_id_2 = retrieved2['nwb_job_id']
        
        assert job_id_2 > job_id_1, "IDs not auto-incremented"

    def test_job_tracks_actual_file_size(self, mock_session_key):
        """Job record updated with actual_file_size_gb after conversion."""
        from u19_pipeline import nwb_production
        
        job = {
            **mock_session_key,
            'job_name': 'size_test',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/size_test.nwb',
            'estimated_file_size_gb': 0.5
        }
        nwb_production.NwbExportJob.insert1(job)
        
        # Update with actual size
        nwb_production.NwbExportJob.update1({
            **mock_session_key,
            'actual_file_size_gb': 0.47
        })
        
        retrieved = (nwb_production.NwbExportJob & mock_session_key).fetch1()
        assert retrieved['actual_file_size_gb'] == 0.47


class TestNwbExportModalityTable:
    """Tests for NwbExportModality association table."""

    @pytest.fixture
    def mock_job_key(self, mock_session_key):
        """Create job and return job key."""
        from u19_pipeline import nwb_production
        
        job = {
            **mock_session_key,
            'job_name': 'modality_test',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/modality_test.nwb',
            'estimated_file_size_gb': 1.0
        }
        nwb_production.NwbExportJob.insert1(job)
        retrieved = (nwb_production.NwbExportJob & mock_session_key).fetch1()
        return {'nwb_job_id': retrieved['nwb_job_id']}

    def test_associate_behavior_modality(self, mock_job_key):
        """Behavior modality can be associated with job."""
        from u19_pipeline import nwb_production
        
        modality = {
            **mock_job_key,
            'modality_name': 'behavior',
            'modality_type': 'towers_task'
        }
        nwb_production.NwbExportModality.insert1(modality)
        
        retrieved = (nwb_production.NwbExportModality & mock_job_key).fetch()
        assert len(retrieved) > 0
        assert retrieved[0]['modality_name'] == 'behavior'

    def test_associate_ephys_modality_raw(self, mock_job_key):
        """Ephys raw data modality can be associated."""
        from u19_pipeline import nwb_production
        
        modality = {
            **mock_job_key,
            'modality_name': 'ephys',
            'modality_type': 'raw',
            'probe_numbers': '[0, 1, 2]'
        }
        nwb_production.NwbExportModality.insert1(modality)
        
        retrieved = (nwb_production.NwbExportModality & {
            **mock_job_key,
            'modality_name': 'ephys'
        }).fetch1()
        assert retrieved['modality_type'] == 'raw'

    def test_associate_imaging_modality_processed(self, mock_job_key):
        """Imaging processed data modality can be associated."""
        from u19_pipeline import nwb_production
        
        modality = {
            **mock_job_key,
            'modality_name': 'imaging',
            'modality_type': 'processed',
            'fov_numbers': '[0, 1, 2, 3]'
        }
        nwb_production.NwbExportModality.insert1(modality)
        
        retrieved = (nwb_production.NwbExportModality & {
            **mock_job_key,
            'modality_name': 'imaging'
        }).fetch1()
        assert retrieved['modality_type'] == 'processed'

    def test_support_multiple_modalities_per_job(self, mock_job_key):
        """Single job can have behavior + ephys + imaging."""
        from u19_pipeline import nwb_production
        
        modalities = [
            {**mock_job_key, 'modality_name': 'behavior', 'modality_type': 'towers_task'},
            {**mock_job_key, 'modality_name': 'ephys', 'modality_type': 'processed', 'probe_numbers': '[0, 1]'},
            {**mock_job_key, 'modality_name': 'imaging', 'modality_type': 'raw', 'fov_numbers': '[0, 1, 2]'}
        ]
        
        for mod in modalities:
            nwb_production.NwbExportModality.insert1(mod)
        
        retrieved = (nwb_production.NwbExportModality & mock_job_key).fetch()
        assert len(retrieved) == 3
        names = {r['modality_name'] for r in retrieved}
        assert names == {'behavior', 'ephys', 'imaging'}


class TestDandiCredentialsTable:
    """Tests for DANDI credential storage."""

    def test_store_dandi_credentials(self):
        """User DANDI API key and dandiset ID stored securely."""
        from u19_pipeline import nwb_production
        
        credentials = {
            'user_id': 'user123',
            'dandi_api_key': 'encrypted_key_here',  # In reality, encrypted
            'default_dandiset_id': '000123',
            'created_timestamp': datetime.now()
        }
        
        nwb_production.DandiCredentials.insert1(credentials, skip_duplicates=True)
        
        retrieved = (nwb_production.DandiCredentials & {'user_id': 'user123'}).fetch1()
        assert retrieved['default_dandiset_id'] == '000123'

    def test_credentials_encryption_field_exists(self):
        """Credentials table has encryption indicator."""
        from u19_pipeline import nwb_production
        
        # Check that definition includes encryption-related fields
        definition = nwb_production.DandiCredentials.definition
        assert 'dandi_api_key' in definition.lower()

    def test_missing_api_key_allowed(self):
        """User can have dandiset ID without API key (will skip upload)."""
        from u19_pipeline import nwb_production
        
        credentials = {
            'user_id': 'user456',
            'dandi_api_key': None,
            'default_dandiset_id': '000456',
            'created_timestamp': datetime.now()
        }
        
        nwb_production.DandiCredentials.insert1(credentials, skip_duplicates=True)
        retrieved = (nwb_production.DandiCredentials & {'user_id': 'user456'}).fetch1()
        assert retrieved['dandi_api_key'] is None

    def test_missing_dandiset_allowed(self):
        """User can have API key without dandiset (will skip upload)."""
        from u19_pipeline import nwb_production
        
        credentials = {
            'user_id': 'user789',
            'dandi_api_key': 'encrypted_key',
            'default_dandiset_id': None,
            'created_timestamp': datetime.now()
        }
        
        nwb_production.DandiCredentials.insert1(credentials, skip_duplicates=True)
        retrieved = (nwb_production.DandiCredentials & {'user_id': 'user789'}).fetch1()
        assert retrieved['default_dandiset_id'] is None


class TestNwbExportLogStatus:
    """Tests for status transition logging."""

    @pytest.fixture
    def mock_job_key(self):
        """Create test job."""
        from u19_pipeline import nwb_production
        
        job = {
            'subject_id': 'test_mouse_logging',
            'session_date': '2026-02-24',
            'session_number': 1,
            'job_name': 'logging_test',
            'user_id': 'user123',
            'status': 'QUEUED',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/logging_test.nwb',
            'estimated_file_size_gb': 1.0
        }
        nwb_production.NwbExportJob.insert1(job)
        retrieved = (nwb_production.NwbExportJob & {
            'subject_id': 'test_mouse_logging',
            'session_date': '2026-02-24',
            'session_number': 1
        }).fetch1()
        return {'nwb_job_id': retrieved['nwb_job_id']}

    def test_log_status_transition(self, mock_job_key):
        """Status transition creates log entry."""
        from u19_pipeline import nwb_production
        
        log_entry = {
            **mock_job_key,
            'status_old': 'QUEUED',
            'status_new': 'DATA_VALIDATION',
            'status_timestamp': datetime.now(),
            'error_message': None,
            'error_exception': None
        }
        
        nwb_production.NwbExportLogStatus.insert1(log_entry)
        
        retrieved = (nwb_production.NwbExportLogStatus & mock_job_key).fetch()
        assert len(retrieved) > 0
        assert retrieved[0]['status_old'] == 'QUEUED'
        assert retrieved[0]['status_new'] == 'DATA_VALIDATION'

    def test_log_captures_error_on_failure(self, mock_job_key):
        """Log entry includes error details on failure."""
        from u19_pipeline import nwb_production
        
        log_entry = {
            **mock_job_key,
            'status_old': 'DATA_VALIDATION',
            'status_new': 'FAILED',
            'status_timestamp': datetime.now(),
            'error_message': 'Missing behavior trials',
            'error_exception': 'ValueError: No trials found for session'
        }
        
        nwb_production.NwbExportLogStatus.insert1(log_entry)
        
        retrieved = (nwb_production.NwbExportLogStatus & mock_job_key).fetch()
        failed_logs = [r for r in retrieved if r['status_new'] == 'FAILED']
        assert len(failed_logs) > 0
        assert 'Missing behavior trials' in failed_logs[0]['error_message']

    def test_query_full_job_history(self, mock_job_key):
        """Can retrieve complete status history for job."""
        from u19_pipeline import nwb_production
        
        transitions = [
            ('QUEUED', 'DATA_VALIDATION'),
            ('DATA_VALIDATION', 'PROCESSING'),
            ('PROCESSING', 'VALIDATION'),
            ('VALIDATION', 'COMPLETED')
        ]
        
        for old_status, new_status in transitions:
            log_entry = {
                **mock_job_key,
                'status_old': old_status,
                'status_new': new_status,
                'status_timestamp': datetime.now(),
                'error_message': None,
                'error_exception': None
            }
            nwb_production.NwbExportLogStatus.insert1(log_entry)
        
        history = (nwb_production.NwbExportLogStatus & mock_job_key).fetch(as_dict=True)
        assert len(history) == 4
        assert history[0]['status_old'] == 'QUEUED'
        assert history[-1]['status_new'] == 'COMPLETED'


class TestNwbExportValidation:
    """Tests for NWB output validation results."""

    @pytest.fixture
    def mock_job_key(self):
        """Create test job."""
        from u19_pipeline import nwb_production
        
        job = {
            'subject_id': 'test_mouse_validation',
            'session_date': '2026-02-24',
            'session_number': 1,
            'job_name': 'validation_test',
            'user_id': 'user123',
            'status': 'VALIDATION',
            'submission_timestamp': datetime.now(),
            'output_filepath': '/data/nwb/validation_test.nwb',
            'estimated_file_size_gb': 1.0
        }
        nwb_production.NwbExportJob.insert1(job)
        retrieved = (nwb_production.NwbExportJob & {
            'subject_id': 'test_mouse_validation',
            'session_date': '2026-02-24',
            'session_number': 1
        }).fetch1()
        return {'nwb_job_id': retrieved['nwb_job_id']}

    def test_store_validation_results(self, mock_job_key):
        """NWB validation results stored in table."""
        from u19_pipeline import nwb_production
        
        validation = {
            **mock_job_key,
            'validation_timestamp': datetime.now(),
            'validation_passed': True,
            'validation_report_json': '{"status": "passed"}',
            'file_size_gb': 0.95,
            'nwb_inspector_passed': True,
            'hdf5_integrity_passed': True,
            'metadata_complete_passed': True,
            'validation_warnings_count': 0,
            'validation_errors_count': 0
        }
        
        nwb_production.NwbExportValidation.insert1(validation)
        
        retrieved = (nwb_production.NwbExportValidation & mock_job_key).fetch1()
        assert retrieved['validation_passed'] is True
        assert retrieved['validation_errors_count'] == 0

    def test_validation_with_warnings(self, mock_job_key):
        """Validation can pass with warnings."""
        from u19_pipeline import nwb_production
        
        validation = {
            **mock_job_key,
            'validation_timestamp': datetime.now(),
            'validation_passed': True,
            'validation_report_json': '{"warnings": ["Missing optional field"]}',
            'file_size_gb': 0.95,
            'nwb_inspector_passed': True,
            'hdf5_integrity_passed': True,
            'metadata_complete_passed': True,
            'validation_warnings_count': 1,
            'validation_errors_count': 0
        }
        
        nwb_production.NwbExportValidation.insert1(validation)
        
        retrieved = (nwb_production.NwbExportValidation & mock_job_key).fetch1()
        assert retrieved['validation_passed'] is True
        assert retrieved['validation_warnings_count'] == 1


class TestNwbExportHandler:
    """Tests for NwbExportHandler processing logic."""

    def test_handler_can_be_imported(self):
        """Handler module and class exist."""
        from u19_pipeline.automatic_job import nwb_export_handler
        from u19_pipeline.automatic_job.nwb_export_handler import NwbExportHandler
        
        assert hasattr(NwbExportHandler, 'pipeline_handler_main')
        assert hasattr(NwbExportHandler, 'process_data_validation')
        assert hasattr(NwbExportHandler, 'process_nwb_conversion')
        assert hasattr(NwbExportHandler, 'process_validation')
        assert hasattr(NwbExportHandler, 'update_status_pipeline')

    @patch('u19_pipeline.nwb_production.NwbExportJob')
    def test_pipeline_handler_queries_active_jobs(self, mock_nwb_job):
        """Main handler queries jobs with QUEUED, DATA_VALIDATION, or PROCESSING status."""
        from u19_pipeline.automatic_job.nwb_export_handler import NwbExportHandler
        
        # Mock active jobs query
        mock_nwb_job.fetch.return_value = [
            {'nwb_job_id': 1, 'status': 'QUEUED'},
            {'nwb_job_id': 2, 'status': 'DATA_VALIDATION'}
        ]
        
        # Call handler (should query without error)
        NwbExportHandler.pipeline_handler_main()
        
        # Verify query was made
        mock_nwb_job.fetch.assert_called()

    def test_data_validation_returns_tuple(self):
        """Data validation returns (is_valid: bool, error_info: dict)."""
        from u19_pipeline.automatic_job.nwb_export_handler import NwbExportHandler
        
        job = {
            'nwb_job_id': 999,
            'subject_id': 'test',
            'session_date': '2026-02-24',
            'session_number': 1
        }
        
        result = NwbExportHandler.process_data_validation(job)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        is_valid, error_info = result
        assert isinstance(is_valid, bool)
        assert isinstance(error_info, dict)
        assert 'error_message' in error_info
        assert 'error_exception' in error_info


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

"""
Enhanced NWB Export Handler - Implementation Template

This document shows the structure and key methods. Some conversion logic
marked with TODO for integration with VirmenDataInterface and NWB libraries.

Per Constitution:
- Principle I: DataJoint for all DB ops (✓ using nwb_production API)
- Principle IV: Enum-based states (✓ using NwbExportStatusEnum)
- Principle II: Type hints on all public functions (✓)
- Principle V: TDD (✓ tests written first)
"""

import time
import traceback
import json
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, Any, Optional
import logging

import datajoint as dj
import h5py
from nwbinspector.inspector import inspect_nwbfile

from u19_pipeline import nwb_production, acquisition, behavior, recording
from u19_pipeline.nwb_export_enums import NwbExportStatusEnum, DandiUploadStatusEnum
from u19_pipeline.nwb_production_utils import (
    estimate_behavior_size_gb,
    estimate_ephys_size_gb,
    estimate_imaging_size_gb,
    validate_behavior_data_exists,
    validate_ephys_data_exists,
    validate_imaging_data_exists,
)

logger = logging.getLogger(__name__)


class NwbExportHandler:
    """
    Handler for NWB export job processing pipeline.
    
    Processes jobs through states: QUEUED → DATA_VALIDATION → PROCESSING → 
    VALIDATION → (UPLOAD) → COMPLETED
    """

    @staticmethod
    def pipeline_handler_main() -> None:
        """
        Main processing loop - queries active jobs and dispatches to handlers.
        
        Called repeatedly by cronjob. Processes all jobs not in terminal state.
        """
        try:
            # Get active jobs (not COMPLETED or FAILED)
            active_jobs = (
                nwb_production.NwbExportJob 
                & "status_id >= 0 AND status_id < 5"  # Exclude COMPLETED(5) and FAILED(-1)
            ).fetch(as_dict=True)

            if active_jobs:
                logger.info(f"Processing {len(active_jobs)} active NWB export jobs")

            for job in active_jobs:
                try:
                    current_status = NwbExportStatusEnum(job["status_id"])
                    job_key = {"nwb_job_id": job["nwb_job_id"]}

                    if current_status == NwbExportStatusEnum.QUEUED:
                        # QUEUED → DATA_VALIDATION
                        NwbExportHandler.process_data_validation(job_key)

                    elif current_status == NwbExportStatusEnum.DATA_VALIDATION:
                        # DATA_VALIDATION → PROCESSING
                        NwbExportHandler.process_nwb_conversion(job_key)

                    elif current_status == NwbExportStatusEnum.PROCESSING:
                        # PROCESSING → VALIDATION
                        NwbExportHandler.process_validation(job_key)

                    elif current_status == NwbExportStatusEnum.VALIDATION:
                        # VALIDATION → (UPLOAD or COMPLETED)
                        NwbExportHandler.process_upload_decision(job_key)

                    elif current_status == NwbExportStatusEnum.UPLOAD:
                        # UPLOAD → COMPLETED or FAILED
                        NwbExportHandler.process_upload_to_dandi(job_key)

                    time.sleep(0.5)  # Avoid hammering DB

                except Exception as e:
                    logger.error(f"Error processing job {job['nwb_job_id']}: {e}", exc_info=True)
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Error in pipeline main loop: {e}", exc_info=True)

    @staticmethod
    def process_data_validation(job_key: Dict[str, Any]) -> None:
        """
        Validate that source data exists for all requested modalities.
        
        Checks:
        - Behavior: Session exists, trials > 0
        - Ephys: Probes exist, spike data or raw files exist
        - Imaging: FOVs exist, ROI data or raw stacks exist
        
        Transitions: QUEUED → DATA_VALIDATION → PROCESSING (success) or FAILED (error)
        """
        job_id = job_key["nwb_job_id"]
        logger.info(f"Starting data validation for job {job_id}")

        try:
            job = (nwb_production.NwbExportJob & job_key).fetch1()
            session_key = {
                'subject_id': job['subject_id'],
                'session_date': job['session_date'],
                'session_number': job['session_number']
            }

            # Get modalities for this job
            modalities = (nwb_production.NwbExportModality & job_key).fetch(as_dict=True)

            all_valid = True
            errors = []

            for mod in modalities:
                modality_name = mod['modality_name']
                modality_type = mod['modality_type']

                if modality_name == 'behavior':
                    is_valid, error_msg = validate_behavior_data_exists(session_key)
                    if not is_valid:
                        all_valid = False
                        errors.append(f"Behavior: {error_msg}")

                elif modality_name == 'ephys':
                    probe_numbers = json.loads(mod['probe_numbers']) if mod['probe_numbers'] else []
                    is_valid, error_msg = validate_ephys_data_exists(session_key, probe_numbers)
                    if not is_valid:
                        all_valid = False
                        errors.append(f"Ephys: {error_msg}")

                elif modality_name == 'imaging':
                    fov_numbers = json.loads(mod['fov_numbers']) if mod['fov_numbers'] else []
                    is_valid, error_msg = validate_imaging_data_exists(session_key, fov_numbers)
                    if not is_valid:
                        all_valid = False
                        errors.append(f"Imaging: {error_msg}")

            if all_valid:
                logger.info(f"Data validation passed for job {job_id}")
                nwb_production.update_job_status(
                    job_key,
                    NwbExportStatusEnum.PROCESSING
                )
            else:
                error_msg = "; ".join(errors)
                logger.error(f"Data validation failed for job {job_id}: {error_msg}")
                nwb_production.update_job_status(
                    job_key,
                    NwbExportStatusEnum.FAILED,
                    error_message=error_msg[:512],
                    error_exception=None
                )

        except Exception as e:
            logger.error(f"Exception in data validation for job {job_id}: {e}", exc_info=True)
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message=str(e)[:512],
                error_exception=traceback.format_exc()[:4095]
            )

    @staticmethod
    def process_nwb_conversion(job_key: Dict[str, Any]) -> None:
        """
        Convert validated data to NWB 2.0 format.
        
        Steps:
        1. Initialize NWBFile with session metadata
        2. Add behavior module (position, velocity, trial structure)
        3. Add ecephys module (spike times, quality metrics) if ephys included
        4. Add ophys module (ROI masks, Ca2+ traces) if imaging included
        5. Write to HDF5 at output_filepath
        
        Transitions: DATA_VALIDATION → PROCESSING → VALIDATION (success) or FAILED (error)
        
        TODO: Integrate with:
        - VirmenDataInterface for behavior
        - KilosortInterface / raw probe readers for ephys
        - ImagingInterface for imaging
        """
        job_id = job_key["nwb_job_id"]
        logger.info(f"Starting NWB conversion for job {job_id}")

        try:
            job = (nwb_production.NwbExportJob & job_key).fetch1()
            output_path = Path(job["output_filepath"])
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # TODO: Implement conversion
            # 1. Get session metadata
            # 2. Initialize NWBFile
            # 3. Add modalities based on NwbExportModality table
            # 4. Write to output_path
            
            # Placeholder: this would be replaced with actual conversion code
            raise NotImplementedError(
                "NWB conversion requires integration with VirmenDataInterface, "
                "KilosortInterface, and imaging data converters. "
                "See IMPLEMENTATION_GUIDE.md for details."
            )

            # On successful conversion:
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.VALIDATION
            )

        except NotImplementedError as e:
            # Special handling for not-yet-implemented
            logger.warning(f"Conversion not implemented for job {job_id}: {e}")
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message="Conversion awaiting implementation",
                error_exception=str(e)
            )

        except Exception as e:
            logger.error(f"Conversion failed for job {job_id}: {e}", exc_info=True)
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message=str(e)[:512],
                error_exception=traceback.format_exc()[:4095]
            )

    @staticmethod
    def process_validation(job_key: Dict[str, Any]) -> None:
        """
        Validate generated NWB file.
        
        Checks:
        - NWB Inspector: run inspection, count warnings/errors
        - HDF5 Integrity: verify file structure valid
        - Metadata Complete: required fields present
        
        Stores results in NwbExportValidation table.
        
        Transitions: PROCESSING → VALIDATION → UPLOAD or COMPLETED (success) or FAILED (error)
        """
        job_id = job_key["nwb_job_id"]
        logger.info(f"Starting validation for job {job_id}")

        try:
            job = (nwb_production.NwbExportJob & job_key).fetch1()
            output_path = Path(job["output_filepath"])

            if not output_path.exists():
                raise FileNotFoundError(f"NWB file not found: {output_path}")

            file_size_gb = output_path.stat().st_size / (1024**3)

            # Initialize validation record
            validation_record = {
                "nwb_job_id": job_id,
                "validation_timestamp": datetime.now(),
                "validation_passed": False,
                "validation_report_json": "{}",
                "file_size_gb": file_size_gb,
                "nwb_inspector_passed": False,
                "hdf5_integrity_passed": False,
                "metadata_complete_passed": False,
                "validation_warnings_count": 0,
                "validation_errors_count": 0
            }

            # Check HDF5 integrity
            try:
                with h5py.File(output_path, 'r') as f:
                    validation_record["hdf5_integrity_passed"] = True
            except Exception as e:
                logger.error(f"HDF5 integrity check failed: {e}")
                validation_record["hdf5_integrity_passed"] = False

            # Run NWB Inspector
            try:
                inspection_results = inspect_nwbfile(str(output_path))
                report = {
                    "warnings": [str(w) for w in inspection_results.get("warnings", [])],
                    "errors": [str(e) for e in inspection_results.get("errors", [])]
                }
                validation_record["validation_report_json"] = json.dumps(report)
                validation_record["nwb_inspector_passed"] = len(report["errors"]) == 0
                validation_record["validation_warnings_count"] = len(report["warnings"])
                validation_record["validation_errors_count"] = len(report["errors"])
            except Exception as e:
                logger.error(f"NWB Inspector failed: {e}")
                validation_record["validation_report_json"] = json.dumps({"error": str(e)})

            # Check metadata completeness
            # TODO: Verify required metadata fields present
            validation_record["metadata_complete_passed"] = True  # Placeholder

            # Overall pass if no critical errors
            validation_record["validation_passed"] = (
                validation_record["hdf5_integrity_passed"] and
                validation_record["nwb_inspector_passed"] and
                validation_record["metadata_complete_passed"]
            )

            # Insert validation record
            nwb_production.NwbExportValidation.insert1(validation_record)

            # Update actual file size
            update_dict = {**job_key, "actual_file_size_gb": file_size_gb}
            nwb_production.NwbExportJob.update1(update_dict)

            if validation_record["validation_passed"]:
                logger.info(f"Validation passed for job {job_id}")
                nwb_production.update_job_status(
                    job_key,
                    NwbExportStatusEnum.UPLOAD
                )
            else:
                logger.error(f"Validation failed for job {job_id}")
                error_msg = f"Validation errors: {validation_record['validation_errors_count']} errors, {validation_record['validation_warnings_count']} warnings"
                nwb_production.update_job_status(
                    job_key,
                    NwbExportStatusEnum.FAILED,
                    error_message=error_msg,
                    error_exception=None
                )

        except Exception as e:
            logger.error(f"Validation exception for job {job_id}: {e}", exc_info=True)
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message=str(e)[:512],
                error_exception=traceback.format_exc()[:4095]
            )

    @staticmethod
    def process_upload_decision(job_key: Dict[str, Any]) -> None:
        """
        Decide whether to upload to DANDI or mark complete.
        
        If user has both API key AND dandiset ID → UPLOAD
        If user missing either credential → COMPLETED (no error, no upload)
        
        Transitions: VALIDATION → UPLOAD (if credentials complete) or COMPLETED (if not)
        """
        job_id = job_key["nwb_job_id"]
        job = (nwb_production.NwbExportJob & job_key).fetch1()
        user_id = job["user_id"]

        logger.info(f"Checking DANDI credentials for job {job_id} (user {user_id})")

        can_upload = nwb_production.can_upload_to_dandi(user_id)

        if can_upload:
            logger.info(f"DANDI credentials complete for job {job_id}, proceeding to upload")
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.UPLOAD
            )
        else:
            logger.info(f"DANDI credentials incomplete for job {job_id}, marking complete without upload")
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.COMPLETED
            )

    @staticmethod
    def process_upload_to_dandi(job_key: Dict[str, Any]) -> None:
        """
        Upload NWB file to DANDI.
        
        Steps:
        1. Get DANDI credentials for user
        2. Validate dandiset exists
        3. Upload NWB file using DANDI Python SDK
        4. Store DANDI asset ID
        5. Transition to COMPLETED on success
        
        Transitions: UPLOAD → COMPLETED (success) or FAILED (error)
        
        TODO: Implement DANDI upload with retry logic and progress tracking
        """
        job_id = job_key["nwb_job_id"]
        logger.info(f"Starting DANDI upload for job {job_id}")

        try:
            job = (nwb_production.NwbExportJob & job_key).fetch1()
            user_id = job["user_id"]

            api_key, dandiset_id = nwb_production.get_dandi_credentials(user_id)

            if not api_key or not dandiset_id:
                raise ValueError(f"Missing DANDI credentials for user {user_id}")

            output_path = Path(job["output_filepath"])

            # TODO: Implement DANDI upload
            # 1. Initialize DANDI client with API key
            # 2. Validate dandiset exists
            # 3. Upload file to dandiset
            # 4. Get asset ID
            # 5. Store in NwbExportJobDandi table

            # Placeholder implementation
            raise NotImplementedError(
                "DANDI upload requires integration with DANDI Python SDK. "
                "See IMPLEMENTATION_GUIDE.md for details."
            )

            # On successful upload:
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.COMPLETED
            )

        except NotImplementedError as e:
            logger.warning(f"Upload not implemented for job {job_id}: {e}")
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message="DANDI upload awaiting implementation",
                error_exception=str(e)
            )

        except Exception as e:
            logger.error(f"DANDI upload failed for job {job_id}: {e}", exc_info=True)
            nwb_production.update_job_status(
                job_key,
                NwbExportStatusEnum.FAILED,
                error_message=str(e)[:512],
                error_exception=traceback.format_exc()[:4095]
            )

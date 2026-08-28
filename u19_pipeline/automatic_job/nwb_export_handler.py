"""
Background processor for NWB export jobs.

This module handles the automated processing of NWB export jobs through
the pipeline stages: data validation, NWB conversion, and validation.
"""

import time
import traceback
from datetime import datetime
from pathlib import Path

import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.utils.slack_utils as slack_utils
from u19_pipeline import nwb_production, recording
from u19_pipeline.imaging_pipeline import imaging_element
from u19_pipeline.nwb_production_utils import (
    validate_behavior_data_exists,
    validate_ephys_data_exists,
    validate_imaging_data_exists,
)


class NwbExportHandler:
    """Handler for NWB export job processing pipeline."""

    @staticmethod
    def pipeline_handler_main():
        """
        Main processing loop - queries active jobs and dispatches to handlers.

        This function is called repeatedly by the cronjob to process all active
        NWB export jobs through their pipeline stages.
        """
        # Get active jobs (status < COMPLETED and not FAILED)
        active_jobs = (nwb_production.NwbExportJob & "status_nwb_id >= 0 AND status_nwb_id < 3").fetch(as_dict=True)

        print(f"Processing {len(active_jobs)} active NWB export jobs...")

        for job in active_jobs:
            current_status = job["status_nwb_id"]

            try:
                # Dispatch to appropriate handler based on current status
                if current_status == 0:  # QUEUED -> DATA_VALIDATION
                    success, error_info = NwbExportHandler.process_data_validation(job)
                    next_status = 1 if success else -1

                elif current_status == 1:  # DATA_VALIDATION -> PROCESSING
                    success, error_info = NwbExportHandler.process_nwb_conversion(job)
                    next_status = 2 if success else -1

                elif current_status == 2:  # PROCESSING -> COMPLETED
                    success, error_info = NwbExportHandler.process_validation(job)
                    next_status = 3 if success else -1

                else:
                    continue  # Unknown status, skip

                # Update status if changed
                if next_status != current_status:
                    NwbExportHandler.update_status_pipeline(
                        {"nwb_job_id": job["nwb_job_id"]}, current_status, next_status, error_info
                    )

                    # Send Slack notifications for completion/failure
                    if next_status == 3:  # COMPLETED
                        slack_message = f"NWB export completed: Job #{job['nwb_job_id']}"
                        try:
                            slack_utils.send_slack_update_notification(
                                config.slack_webhooks_dict.get("nwb_export_notification"), slack_message, job
                            )
                        except Exception as e:
                            print(f"Failed to send Slack notification: {e}")

                    elif next_status == -1:  # FAILED
                        try:
                            slack_utils.send_slack_error_notification(
                                config.slack_webhooks_dict.get("nwb_export_notification"), error_info, job
                            )
                        except Exception as e:
                            print(f"Failed to send Slack error notification: {e}")

            except Exception as e:
                # Log unexpected errors
                print(f"Error processing job {job['nwb_job_id']}: {e}")
                traceback.print_exc()

                error_info = {"error_message": str(e)[:255], "error_exception": traceback.format_exc()[:4095]}
                NwbExportHandler.update_status_pipeline(
                    {"nwb_job_id": job["nwb_job_id"]},
                    current_status,
                    -1,  # FAILED
                    error_info,
                )

            time.sleep(1)  # Brief pause between jobs

    @staticmethod
    def process_data_validation(job: dict) -> tuple[bool, dict]:
        """
        Validate that source data exists for all modalities.

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        error_info = {"error_message": None, "error_exception": None}

        try:
            print(f"Validating data for job {job['nwb_job_id']}...")

            # Check behavior data
            if nwb_production.NwbExportJob.BehaviorExport & {"nwb_job_id": job["nwb_job_id"]}:
                session_key = (nwb_production.NwbExportJob.BehaviorExport & {"nwb_job_id": job["nwb_job_id"]}).fetch1(
                    "KEY"
                )

                valid, error_msg = validate_behavior_data_exists(session_key)
                if not valid:
                    raise ValueError(f"Behavior validation failed: {error_msg}")

            # Check ephys data
            if nwb_production.NwbExportJob.EphysExport & {"nwb_job_id": job["nwb_job_id"]}:
                ephys_record = (nwb_production.NwbExportJob.EphysExport & {"nwb_job_id": job["nwb_job_id"]}).fetch1()
                recording_key = {k: ephys_record[k] for k in recording.Recording.primary_key if k in ephys_record}
                probe_numbers = list(ephys_record["probe_numbers"])

                valid, error_msg = validate_ephys_data_exists(recording_key, probe_numbers)
                if not valid:
                    raise ValueError(f"Ephys validation failed: {error_msg}")

            # Check imaging data
            if nwb_production.NwbExportJob.ImagingExport & {"nwb_job_id": job["nwb_job_id"]}:
                imaging_record = (
                    nwb_production.NwbExportJob.ImagingExport & {"nwb_job_id": job["nwb_job_id"]}
                ).fetch1()
                scan_key = {k: imaging_record[k] for k in imaging_element.Scan.primary_key if k in imaging_record}
                fov_numbers = list(imaging_record["fov_numbers"])

                valid, error_msg = validate_imaging_data_exists(scan_key, fov_numbers)
                if not valid:
                    raise ValueError(f"Imaging validation failed: {error_msg}")

            print(f"Data validation passed for job {job['nwb_job_id']}")
            return True, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"Data validation failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def process_nwb_conversion(job: dict) -> tuple[bool, dict]:
        """
        Convert data to NWB format.

        This is a placeholder implementation. In production, this would:
        1. Initialize NWB file with metadata
        2. Add behavior data using VirmenDataInterface
        3. Add ephys data from Kilosort outputs
        4. Add imaging data from ROI traces
        5. Write to output_filepath

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        error_info = {"error_message": None, "error_exception": None}

        try:
            print(f"Converting data to NWB for job {job['nwb_job_id']}...")

            # Ensure output directory exists
            output_path = Path(job["output_filepath"])
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # TODO: Implement actual NWB conversion
            # This would involve:
            # - from pynwb import NWBFile, NWBHDF5IO
            # - Creating NWBFile with session metadata
            # - Adding behavior module with VirmenDataInterface
            # - Adding ecephys module with Units tables
            # - Adding ophys module with ImageSegmentation + Fluorescence
            # - Writing to HDF5 file

            # For now, create a placeholder
            # In production, replace this with actual NWB conversion code
            raise NotImplementedError(
                "NWB conversion not yet implemented. "
                "This requires integration with VirmenDataInterface and NWB conversion tools."
            )

            print(f"NWB conversion completed for job {job['nwb_job_id']}")
            return True, error_info

        except NotImplementedError as e:
            # Special handling for not-implemented - this is expected
            error_info["error_message"] = "NWB conversion awaiting implementation"
            error_info["error_exception"] = str(e)[:4095]
            print(f"NWB conversion not implemented for job {job['nwb_job_id']}")
            return False, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"NWB conversion failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def process_validation(job: dict) -> tuple[bool, dict]:
        """
        Validate NWB file and insert validation record.

        This would run NWB Inspector and other validation checks.

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        error_info = {"error_message": None, "error_exception": None}

        try:
            print(f"Validating NWB file for job {job['nwb_job_id']}...")

            output_path = Path(job["output_filepath"])

            # Check file exists
            if not output_path.exists():
                raise FileNotFoundError(f"NWB file not found: {output_path}")

            # Get actual file size
            file_size_gb = output_path.stat().st_size / (1024**3)

            # TODO: Implement actual NWB validation
            # This would involve:
            # - Running NWB Inspector: nwbinspector.inspect_nwbfile(filepath)
            # - Checking HDF5 integrity: h5py.File(filepath, 'r')
            # - Verifying required metadata present
            # - Counting warnings and errors

            # For now, create a placeholder validation record
            validation_record = {
                "nwb_job_id": job["nwb_job_id"],
                "validation_timestamp": datetime.now(),
                "validation_passed": False,  # Set to False until implemented
                "validation_report_json": {},  # Would contain Inspector output
                "file_size_gb": file_size_gb,
                "nwb_inspector_passed": False,
                "hdf5_integrity_passed": False,
                "metadata_complete_passed": False,
                "validation_warnings_count": 0,
                "validation_errors_count": 1,  # Count "not implemented" as error
            }

            nwb_production.NwbExportValidation.insert1(validation_record)

            # Update actual file size in main job record
            (nwb_production.NwbExportJob & {"nwb_job_id": job["nwb_job_id"]}).update1(
                {"actual_file_size_gb": file_size_gb}
            )

            raise NotImplementedError("NWB validation not yet implemented")

        except NotImplementedError as e:
            error_info["error_message"] = "NWB validation awaiting implementation"
            error_info["error_exception"] = str(e)[:4095]
            print(f"NWB validation not implemented for job {job['nwb_job_id']}")
            return False, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"NWB validation failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def update_status_pipeline(job_key: dict, old_status: int, new_status: int, error_info: dict):
        """
        Update job status and log transition.

        Args:
            job_key: Job identifier dictionary
            old_status: Previous status ID
            new_status: New status ID
            error_info: Dictionary with error_message and error_exception
        """
        print(f"Updating job {job_key['nwb_job_id']}: status {old_status} -> {new_status}")

        # Update job status
        (nwb_production.NwbExportJob & job_key).update1({"status_nwb_id": new_status})

        # Set completion timestamp if completed
        if new_status == 3:  # COMPLETED
            (nwb_production.NwbExportJob & job_key).update1({"completion_timestamp": datetime.now()})

        # Log status change
        log_entry = {
            **job_key,
            "status_nwb_id_old": old_status,
            "status_nwb_id_new": new_status,
            "status_timestamp": datetime.now(),
        }

        if error_info.get("error_message"):
            log_entry["error_message"] = error_info["error_message"]
        if error_info.get("error_exception"):
            log_entry["error_exception"] = error_info["error_exception"]

        nwb_production.NwbExportLogStatus.insert1(log_entry)

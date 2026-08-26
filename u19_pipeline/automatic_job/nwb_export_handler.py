"""
Background processor for NWB export jobs.

This module handles the automated processing of NWB export jobs through
the pipeline stages: data validation, NWB conversion, and validation.
"""

import ast
import json
import time
import traceback
from datetime import datetime
from pathlib import Path

import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.utils.slack_utils as slack_utils
from u19_pipeline import acquisition, nwb_production
from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
from u19_pipeline.nwb_production_utils import (
    recording_ids_for_session,
    validate_behavior_data_exists,
    validate_ephys_data_exists,
)


def _parse_number_list(raw) -> list:
    """
    Parse a probe_numbers / fov_numbers value into a list of ints.

    NwbExportModality stores these as a JSON-array string (e.g. "[0, 1, 2]"),
    but the value may already be a list/None. Returns an empty list for NULL.
    """
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    try:
        return list(json.loads(raw))
    except (ValueError, TypeError):
        try:
            return list(ast.literal_eval(raw))
        except (ValueError, SyntaxError, TypeError):
            return []


def _parse_export_params(raw) -> dict:
    """
    Parse a job's export_parameters value into a dict.

    Stored as a JSON string (or a Python-repr string in early versions); may also
    already be a dict or None. Returns an empty dict on failure.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return dict(json.loads(raw))
    except (ValueError, TypeError):
        try:
            return dict(ast.literal_eval(raw))
        except (ValueError, SyntaxError, TypeError):
            return {}


class NwbExportHandler:
    """Handler for NWB export job processing pipeline."""

    @staticmethod
    def pipeline_handler_main():
        """
        Main processing loop - queries active jobs and dispatches to handlers.

        This function is called repeatedly by the cronjob to process all active
        NWB export jobs through their pipeline stages.
        """
        # Active jobs = non-terminal: status in [QUEUED .. UPLOADED], i.e.
        # status_id >= 0 AND status_id < COMPLETED(6) AND status_id != FAILED(-1).
        completed = int(NwbExportStatusEnum.COMPLETED)
        failed = int(NwbExportStatusEnum.FAILED)
        restriction = (
            f"status_id >= 0 AND status_id < {completed} AND status_id != {failed}"
        )
        active_jobs = (nwb_production.NwbExportJob & restriction).fetch(as_dict=True)

        print(f"Processing {len(active_jobs)} active NWB export jobs...")

        for job in active_jobs:
            current_status = job["status_id"]

            try:
                # Dispatch to the appropriate handler based on current status,
                # using NwbExportStatusEnum values consistently.
                if current_status == int(NwbExportStatusEnum.QUEUED):
                    success, error_info = NwbExportHandler.process_data_validation(job)
                    next_status = (
                        int(NwbExportStatusEnum.DATA_VALIDATION) if success else failed
                    )

                elif current_status == int(NwbExportStatusEnum.DATA_VALIDATION):
                    success, error_info = NwbExportHandler.process_nwb_conversion(job)
                    next_status = (
                        int(NwbExportStatusEnum.PROCESSING) if success else failed
                    )

                elif current_status == int(NwbExportStatusEnum.PROCESSING):
                    success, error_info = NwbExportHandler.process_validation(job)
                    next_status = (
                        int(NwbExportStatusEnum.VALIDATION) if success else failed
                    )

                elif current_status == int(NwbExportStatusEnum.VALIDATION):
                    success, error_info = NwbExportHandler.process_upload(job)
                    # process_upload returns the next status (UPLOAD or COMPLETED)
                    next_status = (
                        error_info.pop("_next_status", completed) if success else failed
                    )

                elif current_status == int(NwbExportStatusEnum.UPLOAD):
                    success, error_info = NwbExportHandler.process_upload(job)
                    next_status = (
                        error_info.pop("_next_status", completed) if success else failed
                    )

                elif current_status == int(NwbExportStatusEnum.UPLOADED):
                    # Upload finished previously; finalize.
                    success, error_info = (
                        True,
                        {"error_message": None, "error_exception": None},
                    )
                    next_status = completed

                else:
                    continue  # Unknown status, skip

                # Update status if changed
                if next_status != current_status:
                    NwbExportHandler.update_status_pipeline(
                        {"nwb_job_id": job["nwb_job_id"]},
                        current_status,
                        next_status,
                        error_info,
                    )

                    if next_status == completed:
                        slack_message = (
                            f"NWB export completed: Job #{job['nwb_job_id']}"
                        )
                        try:
                            slack_utils.send_slack_update_notification(
                                config.slack_webhooks_dict.get(
                                    "nwb_export_notification"
                                ),
                                slack_message,
                                job,
                            )
                        except Exception as e:
                            print(f"Failed to send Slack notification: {e}")

                    elif next_status == failed:
                        try:
                            slack_utils.send_slack_error_notification(
                                config.slack_webhooks_dict.get(
                                    "nwb_export_notification"
                                ),
                                error_info,
                                job,
                            )
                        except Exception as e:
                            print(f"Failed to send Slack error notification: {e}")

            except Exception as e:
                # Log unexpected errors
                print(f"Error processing job {job['nwb_job_id']}: {e}")
                traceback.print_exc()

                error_info = {
                    "error_message": str(e)[:255],
                    "error_exception": traceback.format_exc()[:4095],
                }
                NwbExportHandler.update_status_pipeline(
                    {"nwb_job_id": job["nwb_job_id"]},
                    current_status,
                    failed,
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

            # The session that this job exports is the acquisition.Session referenced
            # by the NwbExportJob primary key. Derive its key from the job record.
            session_key = {
                k: job[k] for k in acquisition.Session.primary_key if k in job
            }

            # Modalities to export are recorded in NwbExportModality (one row per
            # modality_name). Branch on modality_name to run the right validation.
            modalities = (
                nwb_production.NwbExportModality & {"nwb_job_id": job["nwb_job_id"]}
            ).fetch(as_dict=True)

            for modality in modalities:
                modality_name = modality["modality_name"]

                if modality_name == "behavior":
                    valid, error_msg = validate_behavior_data_exists(session_key)
                    if not valid:
                        raise ValueError(f"Behavior validation failed: {error_msg}")

                elif modality_name == "ephys":
                    # The job record carries only the acquisition.Session key, not
                    # recording_id. Resolve the recording(s) for this session via
                    # the BehaviorSession Part table, then validate each.
                    recording_ids = recording_ids_for_session(session_key)
                    if not recording_ids:
                        raise ValueError(
                            f"Ephys validation failed: no recording linked to "
                            f"session {session_key}"
                        )
                    probe_numbers = _parse_number_list(modality.get("probe_numbers"))
                    for rid in recording_ids:
                        recording_key = {"recording_id": rid}
                        valid, error_msg = validate_ephys_data_exists(
                            recording_key, probe_numbers
                        )
                        if not valid:
                            raise ValueError(
                                f"Ephys validation failed for recording {rid}: {error_msg}"
                            )

                elif modality_name == "imaging":
                    # TODO: the imaging_element.Scan <-> acquisition.Session linkage
                    # is not reliably known. Do NOT fabricate a scan key (an empty
                    # restriction matches all rows and passes vacuously). Fail loud
                    # until the linkage is confirmed and wired here.
                    raise ValueError(
                        f"Imaging validation failed: could not resolve imaging Scan "
                        f"for session {session_key}; imaging export not yet wired."
                    )

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
        Convert source data to an NWB file using the shared converter.

        Resolves input paths, runs TowersNWBConverter via the shared
        u19_pipeline.nwb_export.conversion module (same code path as the CLI in
        scripts/run_nwb_export.py), writes the NWB file and records the actual
        file size on the job.

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        from u19_pipeline.nwb_export.conversion import (
            resolve_input_paths,
            run_conversion_to_file,
        )

        error_info = {"error_message": None, "error_exception": None}

        try:
            print(f"Converting data to NWB for job {job['nwb_job_id']}...")

            export_params = _parse_export_params(job.get("export_parameters"))
            session_key = {
                "subject_fullname": job["subject_fullname"],
                "session_date": str(job["session_date"]),
                "session_number": int(job["session_number"]),
            }

            virmen_file, kilosort_dir = resolve_input_paths(job, export_params)

            output_path = Path(job["output_filepath"])
            output_path.parent.mkdir(parents=True, exist_ok=True)

            size_gb = run_conversion_to_file(
                job=job,
                export_params=export_params,
                session_key=session_key,
                virmen_file=virmen_file,
                kilosort_dir=kilosort_dir,
                output_path=str(output_path),
            )

            (nwb_production.NwbExportJob & {"nwb_job_id": job["nwb_job_id"]}).update1(
                {"actual_file_size_gb": size_gb}
            )

            print(f"NWB conversion completed for job {job['nwb_job_id']}")
            return True, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"NWB conversion failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def process_validation(job: dict) -> tuple[bool, dict]:
        """
        Validate the generated NWB file and record a validation result.

        Runs an HDF5 integrity check and, if nwbinspector is importable, the NWB
        Inspector. Inserts a single NwbExportValidation row (idempotent on retry)
        and updates actual_file_size_gb.

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        import json

        error_info = {"error_message": None, "error_exception": None}

        try:
            print(f"Validating NWB file for job {job['nwb_job_id']}...")

            output_path = Path(job["output_filepath"])
            if not output_path.exists():
                raise FileNotFoundError(f"NWB file not found: {output_path}")

            file_size_gb = output_path.stat().st_size / (1024**3)

            # ── HDF5 integrity: open and read top-level keys ──────────────────
            hdf5_ok = False
            top_keys: list = []
            try:
                import h5py

                with h5py.File(output_path, "r") as f:
                    top_keys = list(f.keys())
                hdf5_ok = bool(top_keys)
            except Exception as exc:  # noqa: BLE001
                hdf5_ok = False
                error_info["error_message"] = f"HDF5 integrity check failed: {exc}"[
                    :255
                ]

            # ── NWB Inspector (optional) ──────────────────────────────────────
            inspector_ran = False
            inspector_passed = True
            warnings_count = 0
            errors_count = 0
            report_messages: list = []
            try:
                import nwbinspector

                inspect_fn = getattr(nwbinspector, "inspect_nwbfile", None)
                if inspect_fn is None:
                    raise AttributeError("nwbinspector.inspect_nwbfile not available")
                for msg in inspect_fn(nwbfile_path=str(output_path)):
                    importance = str(getattr(msg, "importance", ""))
                    report_messages.append(
                        {
                            "importance": importance,
                            "message": str(getattr(msg, "message", "")),
                            "check": str(getattr(msg, "check_function_name", "")),
                        }
                    )
                    if "ERROR" in importance.upper():
                        errors_count += 1
                    else:
                        warnings_count += 1
                inspector_ran = True
                inspector_passed = errors_count == 0
            except ImportError:
                print(
                    "nwbinspector not importable; skipping NWB Inspector (not a failure)."
                )
            except Exception as exc:  # noqa: BLE001
                # Inspector itself errored; record but do not crash validation.
                print(f"NWB Inspector run failed: {exc}")
                report_messages.append({"inspector_error": str(exc)})

            validation_passed = bool(
                hdf5_ok and (not inspector_ran or inspector_passed)
            )

            report_json = json.dumps(
                {
                    "top_level_keys": top_keys,
                    "inspector_ran": inspector_ran,
                    "messages": report_messages,
                }
            )

            validation_record = {
                "nwb_job_id": job["nwb_job_id"],
                "validation_timestamp": datetime.now(),
                "validation_passed": validation_passed,
                "validation_report_json": report_json,
                "file_size_gb": file_size_gb,
                "nwb_inspector_passed": inspector_passed if inspector_ran else True,
                "hdf5_integrity_passed": hdf5_ok,
                "metadata_complete_passed": hdf5_ok,
                "validation_warnings_count": warnings_count,
                "validation_errors_count": errors_count,
            }

            # Insert ONCE. On a cronjob retry a row may already exist for this
            # nwb_job_id (primary key); replace it rather than crashing on a
            # duplicate-key insert.
            job_pk = {"nwb_job_id": job["nwb_job_id"]}
            if nwb_production.NwbExportValidation & job_pk:
                (nwb_production.NwbExportValidation & job_pk).delete_quick()
            nwb_production.NwbExportValidation.insert1(validation_record)

            (nwb_production.NwbExportJob & job_pk).update1(
                {"actual_file_size_gb": file_size_gb}
            )

            if not validation_passed:
                error_info["error_message"] = (
                    error_info["error_message"]
                    or f"NWB validation failed (hdf5_ok={hdf5_ok}, errors={errors_count})"
                )
                print(f"NWB validation failed for job {job['nwb_job_id']}")
                return False, error_info

            print(f"NWB validation passed for job {job['nwb_job_id']}")
            return True, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"NWB validation failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def process_upload(job: dict) -> tuple[bool, dict]:
        """
        Optional DANDI upload stage.

        If the job has no NwbExportJobDandi row, no upload was requested and the
        job transitions straight to COMPLETED. If a dandiset was selected, upload
        the NWB file to DANDI and record the upload status; on success transition
        to COMPLETED, on failure mark FAILED with an actionable error (a requested
        upload is never silently skipped).

        The status to advance to on success is returned via the ``_next_status``
        key of ``error_info`` (the dispatch loop pops it).

        Args:
            job: Job record dictionary

        Returns:
            Tuple of (success, error_info)
        """
        error_info = {"error_message": None, "error_exception": None}
        completed = int(NwbExportStatusEnum.COMPLETED)

        try:
            job_pk = {"nwb_job_id": job["nwb_job_id"]}

            dandi_rows = (nwb_production.NwbExportJobDandi & job_pk).fetch(as_dict=True)
            if not dandi_rows:
                # No DANDI upload requested for this job -> finalize.
                print(
                    f"No DANDI upload requested for job {job['nwb_job_id']}; finalizing."
                )
                error_info["_next_status"] = completed
                return True, error_info

            dandiset_id = dandi_rows[0]["dandiset_id"]
            user_id = job["user_id"]

            from u19_pipeline.nwb_production import (
                can_upload_to_dandi,
                get_dandi_credentials,
            )

            if not can_upload_to_dandi(user_id):
                raise RuntimeError(
                    f"DANDI upload requested (dandiset {dandiset_id}) but user "
                    f"'{user_id}' has no usable DANDI credentials configured."
                )

            api_key, _default_dandiset = get_dandi_credentials(user_id)
            if not api_key:
                raise RuntimeError(
                    f"DANDI upload requested but no API key available for user '{user_id}'."
                )

            from u19_pipeline.nwb_export.dandi.upload_client import DandiUploadClient

            output_path = job["output_filepath"]
            print(
                f"Uploading job {job['nwb_job_id']} to DANDI dandiset {dandiset_id}..."
            )
            client = DandiUploadClient(api_key=api_key, dandiset_id=dandiset_id)
            client.upload(output_path)

            # Record upload success. The neuroconv upload returns organised file
            # paths, not a DANDI asset id; leave dandi_asset_id NULL until a real
            # asset-id lookup is wired (TODO).
            (nwb_production.NwbExportJobDandi & job_pk).update1(
                {**job_pk, "upload_status": 1, "upload_timestamp": datetime.now()}
            )

            print(f"DANDI upload complete for job {job['nwb_job_id']}")
            error_info["_next_status"] = completed
            return True, error_info

        except Exception as e:
            error_info["error_message"] = str(e)[:255]
            error_info["error_exception"] = traceback.format_exc()[:4095]
            print(f"DANDI upload failed for job {job['nwb_job_id']}: {e}")
            return False, error_info

    @staticmethod
    def update_status_pipeline(
        job_key: dict, old_status: int, new_status: int, error_info: dict
    ):
        """
        Update job status and log transition.

        Args:
            job_key: Job identifier dictionary
            old_status: Previous status ID
            new_status: New status ID
            error_info: Dictionary with error_message and error_exception
        """
        print(
            f"Updating job {job_key['nwb_job_id']}: status {old_status} -> {new_status}"
        )

        # Update job status
        (nwb_production.NwbExportJob & job_key).update1({"status_id": new_status})

        # Set completion timestamp if completed
        if new_status == 3:  # COMPLETED
            (nwb_production.NwbExportJob & job_key).update1(
                {"completion_timestamp": datetime.now()}
            )

        # Log status change
        log_entry = {
            **job_key,
            "status_old": old_status,
            "status_new": new_status,
            "status_timestamp": datetime.now(),
        }

        if error_info.get("error_message"):
            log_entry["error_message"] = error_info["error_message"]
        if error_info.get("error_exception"):
            log_entry["error_exception"] = error_info["error_exception"]

        nwb_production.NwbExportLogStatus.insert1(log_entry)

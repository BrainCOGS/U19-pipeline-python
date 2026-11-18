"""
Resource estimation utilities for NWB export jobs.

Provides functions to estimate file sizes and validate data availability.
"""


def estimate_behavior_size_gb(session_key: dict) -> float:
    """
    Estimate NWB size for behavior data.

    Logic: Query behavior.TowersBlock.Trial count
           Assume ~50KB per trial for position/velocity timeseries

    Args:
        session_key: Dictionary with session identifiers

    Returns:
        Estimated size in GB
    """
    from u19_pipeline import behavior  # noqa: PLC0415

    try:
        trials = (behavior.TowersBlock.Trial & session_key).fetch("KEY")
        trial_count = len(trials)
        # 50KB per trial converted to GB
        size_gb = (trial_count * 50 * 1024) / (1024**3)
        return size_gb
    except Exception:
        # If we can't estimate, return a conservative estimate
        return 0.5  # 500MB default


def estimate_ephys_size_gb(recording_key: dict, probe_numbers: list) -> float:
    """
    Estimate NWB size for ephys data (Kilosort outputs only).

    Logic: ~200MB per probe for spike times + cluster info

    Args:
        recording_key: Dictionary with recording identifiers
        probe_numbers: List of probe/insertion numbers

    Returns:
        Estimated size in GB
    """
    probe_count = len(probe_numbers)
    # 200MB per probe
    size_gb = probe_count * 0.2
    return size_gb


def estimate_imaging_size_gb(scan_key: dict, fov_numbers: list) -> float:
    """
    Estimate NWB size for imaging data (ROI traces only).

    Logic: ~50MB per FOV for ROI masks + calcium traces

    Args:
        scan_key: Dictionary with scan identifiers
        fov_numbers: List of FOV numbers

    Returns:
        Estimated size in GB
    """
    fov_count = len(fov_numbers)
    # 50MB per FOV
    size_gb = fov_count * 0.05
    return size_gb


def estimate_total_size(nwb_job_key: dict) -> float:
    """
    Calculate total estimated size for a job.

    Queries modality part tables and sums estimates.

    Args:
        nwb_job_key: Dictionary with nwb_job_id

    Returns:
        Total estimated size in GB
    """
    from u19_pipeline import nwb_production

    total_gb = 0.0

    # Check behavior
    if nwb_production.NwbExportJob.BehaviorExport & nwb_job_key:
        session_key = (nwb_production.NwbExportJob.BehaviorExport & nwb_job_key).fetch1("KEY")
        total_gb += estimate_behavior_size_gb(session_key)

    # Check ephys
    if nwb_production.NwbExportJob.EphysExport & nwb_job_key:
        recording_key, probe_numbers = (nwb_production.NwbExportJob.EphysExport & nwb_job_key).fetch1(
            "KEY", "probe_numbers"
        )
        total_gb += estimate_ephys_size_gb(recording_key, probe_numbers)

    # Check imaging
    if nwb_production.NwbExportJob.ImagingExport & nwb_job_key:
        scan_key, fov_numbers = (nwb_production.NwbExportJob.ImagingExport & nwb_job_key).fetch1("KEY", "fov_numbers")
        total_gb += estimate_imaging_size_gb(scan_key, fov_numbers)

    return total_gb


def validate_behavior_data_exists(session_key: dict) -> tuple[bool, str]:
    """
    Validate that behavior data exists for a session.

    Args:
        session_key: Dictionary with session identifiers

    Returns:
        Tuple of (valid, error_message)
    """
    from u19_pipeline import acquisition, behavior  # noqa: PLC0415

    try:
        # Check if session exists
        if not (acquisition.Session & session_key):
            return False, "Session not found in database"

        # Check if trials exist
        trials = (behavior.TowersBlock.Trial & session_key).fetch("KEY")
        if not trials:
            return False, "No behavior trials found for session"

        return True, ""
    except Exception as e:
        return False, f"Error validating behavior data: {str(e)}"


def validate_ephys_data_exists(recording_key: dict, probe_numbers: list) -> tuple[bool, str]:
    """
    Validate that ephys data exists for specified probes.

    Args:
        recording_key: Dictionary with recording identifiers
        probe_numbers: List of probe/insertion numbers

    Returns:
        Tuple of (valid, error_message)
    """
    from u19_pipeline import recording  # noqa: PLC0415
    from u19_pipeline.ephys_pipeline import ephys_element  # noqa: PLC0415

    try:
        # Check if recording exists
        if not (recording.Recording & recording_key):
            return False, "Recording not found in database"

        # Check if probes exist
        for probe_num in probe_numbers:
            probe_key = {**recording_key, "insertion_number": probe_num}
            if not (ephys_element.ProbeInsertion & probe_key):
                return False, f"Probe {probe_num} not found"

        return True, ""
    except Exception as e:
        return False, f"Error validating ephys data: {str(e)}"


def validate_imaging_data_exists(scan_key: dict, fov_numbers: list) -> tuple[bool, str]:
    """
    Validate that imaging data exists for specified FOVs.

    Args:
        scan_key: Dictionary with scan identifiers
        fov_numbers: List of FOV numbers

    Returns:
        Tuple of (valid, error_message)
    """
    from u19_pipeline.imaging_pipeline import imaging_element  # noqa: PLC0415

    try:
        # Check if scan exists
        if not (imaging_element.Scan & scan_key):
            return False, "Scan not found in database"

        # Check if FOVs exist
        for fov_num in fov_numbers:
            fov_key = {**scan_key, "fov": fov_num}
            if not (imaging_element.FieldOfView & fov_key):
                return False, f"FOV {fov_num} not found"

        return True, ""
    except Exception as e:
        return False, f"Error validating imaging data: {str(e)}"

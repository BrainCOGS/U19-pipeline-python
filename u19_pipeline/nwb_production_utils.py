"""
Resource estimation utilities for NWB export jobs.

Provides functions to estimate file sizes and validate data availability.
"""

import logging

log = logging.getLogger(__name__)


def recording_ids_for_session(session_key: dict) -> list:
    """
    Return the recording_id(s) linked to an acquisition.Session.

    The NwbExportJob record only carries the acquisition.Session primary key
    (subject_fullname, session_date, session_number); it does NOT carry
    recording_id. The link lives in recording.Recording.BehaviorSession, a Part
    table whose secondary attribute is ``-> acquisition.Session``. This helper
    resolves the session key to the set of recording_ids for that session.

    Args:
        session_key: Dictionary with acquisition.Session identifiers.

    Returns:
        List of recording_id ints (empty if none linked).
    """
    from u19_pipeline import recording  # noqa: PLC0415

    return (
        (recording.Recording.BehaviorSession & session_key)
        .fetch("recording_id")
        .tolist()
    )


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


def estimate_imaging_size_gb(recording_key: dict, fov_numbers: list) -> float:
    """
    Estimate the on-disk size of raw ScanImage imaging data for a recording.

    Logic: for each requested FOV (an imaging_pipeline.TiffSplit.tiff_split
    number), sum the frame counts of its TiffSplit.File rows -- each
    file_frame_range is a [first last] frame-index pair, inclusive -- and
    multiply by the per-frame pixel count from
    TiffSplit.fov_pixel_resolution_xy and 2 bytes/pixel (ScanImage raw frames
    are int16). If fov_numbers is empty, all TiffSplits for the recording are
    used.

    Sanity check against the sample session: a 2000-frame, 512x512 int16
    ScanImage BigTIFF (5-slice fastZ, single FOV) works out to
        512 * 512 * 2000 frames * 2 bytes/pixel = 1,048,576,000 bytes
                                                  ~= 0.98 GiB
    by this formula. The actual TIFF measured 0.62 GB on disk (ScanImage
    applies TIFF compression) and the NWB file converted from it measured
    0.42 GB. This function deliberately estimates the *uncompressed raw*
    footprint rather than the compressed-on-disk or NWB-output size, so for
    this sample it comes out ~1.6-2.3x the true footprint -- overestimating
    is the safer failure mode for a size estimate used to provision space.

    Falls back to a flat per-FOV constant only if the DB lookup fails (e.g.
    frame-range/resolution metadata not yet populated for this recording).
    The fallback is calibrated off the same sample (0.62 GB observed for one
    FOV) and rounds down slightly to 0.5 GB/FOV as a documented,
    order-of-magnitude-correct default -- replacing the old flat 0.05 GB/FOV
    guess, which was more than 10x too small.

    Args:
        recording_key: Dictionary with recording identifiers (e.g.
            {"recording_id": rid}). Callers with only an acquisition.Session
            key should resolve recording_id(s) via recording_ids_for_session()
            first and call this once per recording_id.
        fov_numbers: List of FOV numbers (TiffSplit.tiff_split values). Empty
            means "all FOVs for this recording".

    Returns:
        Estimated size in GB.
    """
    from u19_pipeline import imaging_pipeline  # noqa: PLC0415

    bytes_per_pixel = 2  # ScanImage raw frames are int16
    fallback_gb_per_fov = 0.5  # see docstring for derivation

    try:
        if fov_numbers:
            restriction = [
                {**recording_key, "tiff_split": fov_num} for fov_num in fov_numbers
            ]
        else:
            restriction = recording_key

        split_keys = (imaging_pipeline.TiffSplit & restriction).fetch("KEY")
        if not split_keys:
            raise ValueError("No TiffSplit rows found for recording/FOVs")

        total_bytes = 0
        for split_key in split_keys:
            rows, cols = (imaging_pipeline.TiffSplit & split_key).fetch1(
                "fov_pixel_resolution_xy"
            )
            pixels_per_frame = int(rows) * int(cols)

            frame_ranges = (imaging_pipeline.TiffSplit.File & split_key).fetch(
                "file_frame_range"
            )
            n_frames = sum(int(fr[1]) - int(fr[0]) + 1 for fr in frame_ranges)

            total_bytes += pixels_per_frame * n_frames * bytes_per_pixel

        return total_bytes / (1024**3)
    except Exception:
        # Conservative fallback -- see docstring for the 0.5 GB/FOV derivation.
        fov_count = len(fov_numbers) if fov_numbers else 1
        return fov_count * fallback_gb_per_fov


def _parse_number_list(raw) -> list:
    """Parse a probe_numbers / fov_numbers value (JSON-array string) into a list."""
    import ast
    import json

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


def estimate_total_size(nwb_job_key: dict) -> float:
    """
    Calculate total estimated size for a job.

    Queries the NwbExportModality association table for the job and sums the
    per-modality estimates. The session/recording/scan keys are derived from the
    NwbExportJob record (which carries the acquisition.Session primary key).

    Args:
        nwb_job_key: Dictionary with nwb_job_id

    Returns:
        Total estimated size in GB
    """
    from u19_pipeline import acquisition, nwb_production

    total_gb = 0.0

    job = (nwb_production.NwbExportJob & nwb_job_key).fetch1()
    session_key = {k: job[k] for k in acquisition.Session.primary_key if k in job}
    modalities = (nwb_production.NwbExportModality & nwb_job_key).fetch(as_dict=True)

    for modality in modalities:
        modality_name = modality["modality_name"]

        if modality_name == "behavior":
            total_gb += estimate_behavior_size_gb(session_key)

        elif modality_name == "ephys":
            # The job carries the session key, not recording_id. Resolve the
            # recording_id(s) for the session via the BehaviorSession Part table.
            recording_ids = recording_ids_for_session(session_key)
            probe_numbers = _parse_number_list(modality.get("probe_numbers"))
            if not recording_ids:
                log.warning(
                    "estimate_total_size: no recording linked to session %s; "
                    "skipping ephys estimate.",
                    session_key,
                )
                continue
            for rid in recording_ids:
                total_gb += estimate_ephys_size_gb({"recording_id": rid}, probe_numbers)

        elif modality_name == "imaging":
            # The job carries the session key, not recording_id. Resolve the
            # recording_id(s) for the session via the BehaviorSession Part table,
            # exactly like the ephys branch above.
            recording_ids = recording_ids_for_session(session_key)
            fov_numbers = _parse_number_list(modality.get("fov_numbers"))
            if not recording_ids:
                log.warning(
                    "estimate_total_size: no recording linked to session %s; "
                    "skipping imaging estimate.",
                    session_key,
                )
                continue
            for rid in recording_ids:
                total_gb += estimate_imaging_size_gb({"recording_id": rid}, fov_numbers)

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


def validate_ephys_data_exists(
    recording_key: dict, probe_numbers: list
) -> tuple[bool, str]:
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


def validate_imaging_data_exists(
    recording_key: dict, fov_numbers: list
) -> tuple[bool, str]:
    """
    Validate that imaging data exists for specified FOVs (tiff splits).

    Validates through the real imaging tables in u19_pipeline.imaging_pipeline:
    ImagingPipelineSession (one row per imaging recording) -> AcquiredTiff (the
    raw tiff acquisition record) -> TiffSplit (one row per FOV, keyed by the
    tinyint `tiff_split` number that fov_numbers refers to) -> TiffSplit.File
    (the actual split files on disk).

    Args:
        recording_key: Dictionary with recording identifiers (must resolve to
            a recording.Recording primary key, e.g. {"recording_id": rid}).
            Callers with only an acquisition.Session key should resolve
            recording_id(s) via recording_ids_for_session() first and call
            this once per recording_id, the same way validate_ephys_data_exists
            is called.
        fov_numbers: List of FOV numbers (TiffSplit.tiff_split values). If
            empty, validates that at least one TiffSplit exists for the
            recording instead of vacuously passing (an empty DataJoint
            restriction matches everything).

    Returns:
        Tuple of (valid, error_message)
    """
    from u19_pipeline import imaging_pipeline  # noqa: PLC0415

    try:
        # Check if the imaging session exists for this recording
        if not (imaging_pipeline.ImagingPipelineSession & recording_key):
            return False, "Imaging session not found in database"

        # Check if the raw tiff acquisition record exists
        if not (imaging_pipeline.AcquiredTiff & recording_key):
            return False, "No acquired tiff found for imaging session"

        if not fov_numbers:
            # Guard against vacuously passing on an empty restriction: require
            # at least one tiff split to exist for this recording.
            if not (imaging_pipeline.TiffSplit & recording_key):
                return False, "No tiff splits found for imaging session"
            return True, ""

        # Check if each requested FOV (tiff split) exists and has files
        for fov_num in fov_numbers:
            split_key = {**recording_key, "tiff_split": fov_num}
            if not (imaging_pipeline.TiffSplit & split_key):
                return False, f"FOV (tiff split) {fov_num} not found"
            if not (imaging_pipeline.TiffSplit.File & split_key):
                return False, f"FOV (tiff split) {fov_num} has no files recorded"

        return True, ""
    except Exception as e:
        return False, f"Error validating imaging data: {str(e)}"

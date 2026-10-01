import os
import re

# Lines of an error log that most likely carry the actual reason of the failure
error_line_regex = re.compile(
    r"([\w\.]*(Error|Exception)\b|\b(error|failed|failure|fatal|traceback|"
    r"no such file|not found|permission denied|out of memory|oom|killed|"
    r"segmentation fault|cancelled|time limit)\b)",
    re.IGNORECASE,
)


def write_file(path, text):

    os.umask(0)
    descriptor = os.open(
        path=path,
        flags=(
            os.O_WRONLY  # access mode: write only
            | os.O_CREAT  # create if not exists
            | os.O_TRUNC  # truncate the file to zero
        ),
        mode=0o664,
    )

    with open(descriptor, "w") as fh:
        fh.write(text)


def summarize_error_log(error_log_data, max_length=200):
    """
    Get the most informative line of a cluster error log, to be reported directly
    in the DB record and in the slack notification (instead of "check LOG")
    Input:
    error_log_data (str) = full contents of the error log
    Returns:
    (str) = single line summarizing the error ('' if the log has no content)
    """
    if not error_log_data:
        return ""

    lines = [x.strip() for x in error_log_data.splitlines() if x.strip()]
    if not lines:
        return ""

    # Python/MATLAB tracebacks end with the actual exception, slurm & kilosort
    # write plain messages, so prefer the last error looking line and fall back
    # to the last line of the log
    error_lines = [x for x in lines if error_line_regex.search(x)]
    summary = error_lines[-1] if error_lines else lines[-1]

    if len(summary) > max_length:
        summary = summary[: max_length - 3] + "..."

    return summary


def build_error_message(
    base_message, error_log_data, log_location, max_length=255, summary=None
):
    """
    Build the error message reported to the user (DB & slack) out of the status
    message of the job, the actual error found in the log and the log location
    Input:
    base_message   (str) = message coming from the slurm job status check
    error_log_data (str) = full contents of the error log ('' if not available)
    log_location   (str) = path (local or user@host:path) of the log file
    summary        (str) = error to report instead of the one found in error_log_data
    Returns:
    (str) = error message, cropped to max_length keeping the log location
    """
    # Keep room for the message itself, cropping the beginning of the location
    # (the job id is at the end of it) if the location is unusually long
    max_location_length = max_length - 60
    log_location = str(log_location)
    if len(log_location) > max_location_length:
        log_location = "..." + log_location[-(max_location_length - 3) :]

    suffix = " (LOG: " + log_location + ")"
    if summary is None:
        summary = summarize_error_log(error_log_data)
    if not summary:
        summary = "no error detail found in log"

    message = str(base_message) + " - " + summary if base_message else summary
    available_length = max_length - len(suffix)
    if len(message) > available_length:
        message = message[: max(0, available_length - 3)] + "..."

    return message + suffix


def describe_missing_log(log_name, log_data):
    """
    Why a log has no error to report: it could not be copied (None) or it is empty
    """
    if log_data is None:
        return log_name + " log could not be copied"
    return log_name + " log is empty"


def build_job_error_info(
    slurm_message,
    error_log,
    error_log_location,
    output_log=None,
    output_log_location=None,
    max_message_length=255,
    max_exception_length=4095,
):
    """
    Error message & exception reported (DB & slack) when a cluster job failed.
    The error log (stderr) is used if it has content, otherwise the output log (stdout),
    where Kilosort/MATLAB and many tools write their errors. If neither has content the
    message says why (not copied/empty) next to what slurm reported about the job.
    Input:
    slurm_message       (str) = how the job ended according to slurm (may be '' or None)
    error_log           (str) = contents of the error log, None if it could not be copied
    error_log_location  (str) = path (local or user@host:path) of the error log
    output_log          (str) = contents of the output log, None if it could not be copied
    output_log_location (str) = path of the output log, None if there is no output log to report
    Returns:
    error_message   (str) = single line, at most max_message_length long
    error_exception (str) = notes on missing logs + log text (its tail), ending with the
                            slurm message so cropping from the left (as done for DB and
                            slack) keeps it, at most max_exception_length long
    """
    slurm_message = slurm_message or ""
    error_text = error_log if error_log and error_log.strip() else ""
    output_text = output_log if output_log and output_log.strip() else ""
    use_output_log = output_log_location is not None

    summary = None
    missing_logs = []
    notes = []
    log_location = error_log_location
    log_text = error_text
    if not error_text:
        missing_logs.append(describe_missing_log("error", error_log))
        notes.append(missing_logs[-1] + ": " + str(error_log_location))
        if use_output_log and output_text:
            summary = "stdout: " + summarize_error_log(output_text)
            log_location = output_log_location
            log_text = output_text
            notes.append("tail of output log " + str(output_log_location) + ":")
        elif use_output_log:
            missing_logs.append(describe_missing_log("output", output_log))
            notes.append(missing_logs[-1] + ": " + str(output_log_location))
        if summary is None:
            summary = ", ".join(missing_logs)

    error_message = build_error_message(
        slurm_message,
        error_text,
        log_location,
        max_length=max_message_length,
        summary=summary,
    )

    # Crop the log text from the left, keeping the notes and the slurm message
    header = "\n".join(notes) + "\n" if notes else ""
    footer = "\nslurm: " + slurm_message if slurm_message else ""
    error_exception = header + log_text + footer
    if len(error_exception) > max_exception_length:
        available_length = max_exception_length - len(header) - len(footer)
        if available_length > 3:
            error_exception = (
                header
                + "..."
                + log_text[len(log_text) - available_length + 3 :]
                + footer
            )
        else:
            error_exception = error_exception[-max_exception_length:]

    return error_message, error_exception

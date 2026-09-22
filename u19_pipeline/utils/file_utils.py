import os
import re

#Lines of an error log that most likely carry the actual reason of the failure
error_line_regex = re.compile(
    r'([\w\.]*(Error|Exception)\b|\b(error|failed|failure|fatal|traceback|'
    r'no such file|not found|permission denied|out of memory|oom|killed|'
    r'segmentation fault|cancelled|time limit)\b)', re.IGNORECASE)

def write_file(path, text):

    os.umask(0)
    descriptor = os.open(
    path=path,
    flags=(
        os.O_WRONLY  # access mode: write only
        | os.O_CREAT  # create if not exists
        | os.O_TRUNC  # truncate the file to zero
    ),
    mode=0o664
    )

    with open(descriptor, 'w') as fh:
        fh.write(text)


def summarize_error_log(error_log_data, max_length=200):
    '''
    Get the most informative line of a cluster error log, to be reported directly
    in the DB record and in the slack notification (instead of "check LOG")
    Input:
    error_log_data (str) = full contents of the error log
    Returns:
    (str) = single line summarizing the error ('' if the log has no content)
    '''
    if not error_log_data:
        return ''

    lines = [x.strip() for x in error_log_data.splitlines() if x.strip()]
    if not lines:
        return ''

    # Python/MATLAB tracebacks end with the actual exception, slurm & kilosort
    # write plain messages, so prefer the last error looking line and fall back
    # to the last line of the log
    error_lines = [x for x in lines if error_line_regex.search(x)]
    summary = error_lines[-1] if error_lines else lines[-1]

    if len(summary) > max_length:
        summary = summary[:max_length-3] + '...'

    return summary


def build_error_message(base_message, error_log_data, log_location, max_length=255):
    '''
    Build the error message reported to the user (DB & slack) out of the status
    message of the job, the actual error found in the log and the log location
    Input:
    base_message   (str) = message coming from the slurm job status check
    error_log_data (str) = full contents of the error log ('' if not available)
    log_location   (str) = path (local or user@host:path) of the log file
    Returns:
    (str) = error message, cropped to max_length keeping the log location
    '''
    # Keep room for the message itself, cropping the beginning of the location
    # (the job id is at the end of it) if the location is unusually long
    max_location_length = max_length - 60
    log_location = str(log_location)
    if len(log_location) > max_location_length:
        log_location = '...' + log_location[-(max_location_length-3):]

    suffix = ' (LOG: ' + log_location + ')'
    summary = summarize_error_log(error_log_data)
    if not summary:
        summary = 'no error detail found in log'

    message = str(base_message) + ' - ' + summary
    available_length = max_length - len(suffix)
    if len(message) > available_length:
        message = message[:max(0, available_length-3)] + '...'

    return message + suffix

from u19_pipeline.utils.file_utils import build_error_message, summarize_error_log

PYTHON_TRACEBACK_LOG = """Loading recording...
Traceback (most recent call last):
  File "/mnt/cup/braininit/run_kilosort.py", line 42, in <module>
    main()
MemoryError: Unable to allocate 30.0 GiB for an array
"""

KILOSORT_STDERR_LOG = """Time  35s. Computing whitening matrix..
ERROR: Out of memory on device. To continue use gpuDevice
finished, exiting
"""


class TestSummarizeErrorLog:

    def test_python_traceback_reports_the_exception_line(self):
        assert summarize_error_log(PYTHON_TRACEBACK_LOG) == 'MemoryError: Unable to allocate 30.0 GiB for an array'

    def test_error_line_wins_over_trailing_noise(self):
        # Kilosort keeps writing after the failure, the error line is not the last one
        assert summarize_error_log(KILOSORT_STDERR_LOG) == 'ERROR: Out of memory on device. To continue use gpuDevice'

    def test_log_without_error_keyword_falls_back_to_last_line(self):
        assert summarize_error_log('step 1 done\nstep 2 done\n') == 'step 2 done'

    def test_empty_log(self):
        assert summarize_error_log('') == ''
        assert summarize_error_log('\n  \n') == ''

    def test_long_line_is_cropped(self):
        summary = summarize_error_log('ValueError: ' + 'x' * 500)
        assert len(summary) == 200
        assert summary.endswith('...')

    def test_lines_joined_with_spaces_are_still_split(self):
        # get_error_log_str joins the log lines with ' ', keeping their newlines
        log = ' '.join(PYTHON_TRACEBACK_LOG.splitlines(keepends=True))
        assert summarize_error_log(log) == 'MemoryError: Unable to allocate 30.0 GiB for an array'


class TestBuildErrorMessage:

    def test_message_carries_error_and_log_path(self):
        message = build_error_message('Job failed', PYTHON_TRACEBACK_LOG, '/logs/job_id_123.log')

        assert message == ('Job failed - MemoryError: Unable to allocate 30.0 GiB for an array '
                           '(LOG: /logs/job_id_123.log)')

    def test_missing_log_still_reports_where_to_look(self):
        message = build_error_message('Job failed', '', 'u19prod@spock:/home/ErrorLog/job_id_123.log')

        assert 'no error detail found in log' in message
        assert message.endswith('(LOG: u19prod@spock:/home/ErrorLog/job_id_123.log)')

    def test_message_fits_in_the_db_column_keeping_the_log_path(self):
        message = build_error_message('Job failed', 'ValueError: ' + 'x' * 5000, '/logs/job_id_123.log')

        assert len(message) <= 255
        assert message.endswith('(LOG: /logs/job_id_123.log)')

    def test_unusually_long_log_path_is_cropped_from_the_left(self):
        log_location = '/' + 'very_long_dir/' * 40 + 'job_id_123.log'

        message = build_error_message('Job failed', PYTHON_TRACEBACK_LOG, log_location)

        assert len(message) <= 255
        assert message.endswith('job_id_123.log)')
        assert 'MemoryError' in message

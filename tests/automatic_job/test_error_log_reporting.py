from u19_pipeline.utils.file_utils import (
    build_error_message,
    build_job_error_info,
    summarize_error_log,
)

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
        assert (
            summarize_error_log(PYTHON_TRACEBACK_LOG)
            == "MemoryError: Unable to allocate 30.0 GiB for an array"
        )

    def test_error_line_wins_over_trailing_noise(self):
        # Kilosort keeps writing after the failure, the error line is not the last one
        assert (
            summarize_error_log(KILOSORT_STDERR_LOG)
            == "ERROR: Out of memory on device. To continue use gpuDevice"
        )

    def test_log_without_error_keyword_falls_back_to_last_line(self):
        assert summarize_error_log("step 1 done\nstep 2 done\n") == "step 2 done"

    def test_empty_log(self):
        assert summarize_error_log("") == ""
        assert summarize_error_log("\n  \n") == ""

    def test_long_line_is_cropped(self):
        summary = summarize_error_log("ValueError: " + "x" * 500)
        assert len(summary) == 200
        assert summary.endswith("...")

    def test_lines_joined_with_spaces_are_still_split(self):
        # get_error_log_str joins the log lines with ' ', keeping their newlines
        log = " ".join(PYTHON_TRACEBACK_LOG.splitlines(keepends=True))
        assert (
            summarize_error_log(log)
            == "MemoryError: Unable to allocate 30.0 GiB for an array"
        )


class TestBuildErrorMessage:
    def test_message_carries_error_and_log_path(self):
        message = build_error_message(
            "Job failed", PYTHON_TRACEBACK_LOG, "/logs/job_id_123.log"
        )

        assert message == (
            "Job failed - MemoryError: Unable to allocate 30.0 GiB for an array "
            "(LOG: /logs/job_id_123.log)"
        )

    def test_missing_log_still_reports_where_to_look(self):
        message = build_error_message(
            "Job failed", "", "u19prod@spock:/home/ErrorLog/job_id_123.log"
        )

        assert "no error detail found in log" in message
        assert message.endswith("(LOG: u19prod@spock:/home/ErrorLog/job_id_123.log)")

    def test_message_fits_in_the_db_column_keeping_the_log_path(self):
        message = build_error_message(
            "Job failed", "ValueError: " + "x" * 5000, "/logs/job_id_123.log"
        )

        assert len(message) <= 255
        assert message.endswith("(LOG: /logs/job_id_123.log)")

    def test_unusually_long_log_path_is_cropped_from_the_left(self):
        log_location = "/" + "very_long_dir/" * 40 + "job_id_123.log"

        message = build_error_message("Job failed", PYTHON_TRACEBACK_LOG, log_location)

        assert len(message) <= 255
        assert message.endswith("job_id_123.log)")
        assert "MemoryError" in message


class TestBuildJobErrorInfo:
    def test_error_log_wins_over_output_log(self):
        message, exception = build_job_error_info(
            "FAILED",
            PYTHON_TRACEBACK_LOG,
            "/err/job_id_1.log",
            "Error in output\n",
            "/out/job_id_1.log",
        )

        assert "MemoryError" in message
        assert message.endswith("(LOG: /err/job_id_1.log)")
        assert "Error in output" not in exception

    def test_whitespace_only_error_log_counts_as_empty(self):
        message, _ = build_job_error_info(
            "FAILED", " \n \n", "/err/1.log", KILOSORT_STDERR_LOG, "/out/1.log"
        )

        assert "stdout: ERROR: Out of memory on device" in message
        assert message.endswith("(LOG: /out/1.log)")

    def test_none_log_means_not_copied(self):
        message, exception = build_job_error_info(
            "FAILED", None, "u@h:/err/1.log", None, "u@h:/out/1.log"
        )

        assert "error log could not be copied" in message
        assert "output log could not be copied" in message
        assert "u@h:/out/1.log" in exception

    def test_without_output_log_it_is_not_mentioned(self):
        # pupillometry jobs only copy the error log
        message, exception = build_job_error_info("FAILED", "", "/err/1.log")

        assert "output" not in message
        assert "output" not in exception
        assert "error log is empty" in message

    def test_limits_keep_the_slurm_summary_at_the_end(self):
        # The handler crops error_exception keeping its end (4095 for the DB, 1023 for slack)
        huge = "line\n" * 10000 + "ValueError: " + "x" * 5000 + "\n"
        slurm_message = "OUT_OF_MEMORY (MaxRSS 30.0G, node spock-g3)"

        message, exception = build_job_error_info(
            slurm_message, "", "/err/1.log", huge, "/out/" + "d/" * 200 + "1.log"
        )

        assert len(message) <= 255
        assert len(exception) <= 4095
        assert exception.rstrip().endswith(slurm_message)
        assert slurm_message in exception[-1023:]
        assert "error log is empty" in exception

    def test_custom_limits(self):
        message, exception = build_job_error_info(
            "FAILED",
            "x" * 100,
            "/e.log",
            max_message_length=80,
            max_exception_length=50,
        )

        assert len(message) <= 80
        assert len(exception) <= 50
        assert exception.endswith("slurm: FAILED")

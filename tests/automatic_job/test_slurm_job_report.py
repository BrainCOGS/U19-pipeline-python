"""Tests for reporting why a slurm job failed from its ``sacct`` accounting record.

Jobs that are killed by slurm (out of memory, time limit, node failure, scancel) often
leave no text in their logs, so the job state, exit code, node, elapsed time and memory
are reported instead of only the state.
"""

from u19_pipeline.utils.slurm_utils import (
    describe_slurm_job,
    parse_memory,
    parse_sacct_output,
    sacct_format,
)

SACCT_HEADER = "|".join(sacct_format)


def sacct(*rows):
    return "\n".join([SACCT_HEADER, *rows]) + "\n"


# JobID|State|ExitCode|DerivedExitCode|Reason|Elapsed|Timelimit|MaxRSS|NodeList
OOM_JOB = sacct(
    "1415|OUT_OF_MEMORY|0:125|0:0|None|01:02:03|1-00:00:00||spock-g3",
    "1415.batch|OUT_OF_MEMORY|0:125|||01:02:03||31457280K|spock-g3",
    "1415.extern|COMPLETED|0:0|||01:02:03||1024K|spock-g3",
)
FAILED_JOB = sacct(
    "200|FAILED|1:0|0:0|None|00:00:05|02:00:00||spock-c1",
    "200.batch|FAILED|1:0|||00:00:05||1500M|spock-c1",
)
CANCELLED_JOB = sacct(
    "300|CANCELLED by 12345|0:0|0:0|None|00:10:00|02:00:00||spock-c2",
    "300.batch|CANCELLED|0:15|||00:10:00||2G|spock-c2",
)
TIMEOUT_JOB = sacct(
    "400|TIMEOUT|0:0|0:0|None|02:00:12|02:00:00||spock-g1",
    "400.batch|CANCELLED|0:15|||02:00:12||800M|spock-g1",
)
RUNNING_JOB = sacct("500|RUNNING|0:0|0:0|None|00:01:00|02:00:00||spock-g1")
PENDING_JOB = sacct("600|PENDING|0:0|0:0|Resources|00:00:00|02:00:00||None assigned")


class TestParseMemory:
    def test_units(self):
        assert parse_memory("1024K") == 1024**2
        assert parse_memory("1.5G") == 1.5 * 1024**3
        assert parse_memory("2T") == 2 * 1024**4
        assert parse_memory("512") == 512

    def test_empty_or_garbage_is_zero(self):
        assert parse_memory("") == 0
        assert parse_memory(None) == 0
        assert parse_memory("n/a") == 0


class TestParseSacctOutput:
    def test_main_row_with_max_memory_of_the_steps(self):
        info = parse_sacct_output(OOM_JOB, "1415")

        assert info["JobID"] == "1415"
        assert info["State"] == "OUT_OF_MEMORY"
        assert info["ExitCode"] == "0:125"
        assert info["NodeList"] == "spock-g3"
        # The main row has no MaxRSS, the steps do
        assert info["MaxRSS"] == "31457280K"

    def test_cancelled_by_keeps_the_uid(self):
        assert parse_sacct_output(CANCELLED_JOB, "300")["State"] == "CANCELLED by 12345"

    def test_no_output_means_job_not_found(self):
        assert parse_sacct_output("", "1") is None
        assert parse_sacct_output(None, "1") is None
        assert parse_sacct_output(SACCT_HEADER + "\n", "1") is None
        assert parse_sacct_output("\n\n", "1") is None

    def test_job_id_not_listed_falls_back_to_first_row(self):
        # e.g. array jobs are listed as <id>_<task>
        info = parse_sacct_output(
            sacct("700_1|FAILED|2:0|0:0|None|00:00:01|02:00:00||spock-c1"), "700"
        )
        assert info["State"] == "FAILED"

    def test_malformed_rows_are_skipped(self):
        stdout = sacct(
            "garbage line", "200|FAILED|1:0|0:0|None|00:00:05|02:00:00||spock-c1"
        )
        assert parse_sacct_output(stdout, "200")["State"] == "FAILED"

    def test_windows_line_endings_and_blank_lines(self):
        stdout = OOM_JOB.replace("\n", "\r\n") + "\r\n\r\n"
        assert parse_sacct_output(stdout, "1415")["State"] == "OUT_OF_MEMORY"

    def test_without_header(self):
        stdout = "200|FAILED|1:0|0:0|None|00:00:05|02:00:00||spock-c1\n"
        assert parse_sacct_output(stdout, "200")["ExitCode"] == "1:0"


class TestDescribeSlurmJob:
    def test_out_of_memory(self):
        description = describe_slurm_job(parse_sacct_output(OOM_JOB, "1415"))

        assert description.startswith("OUT_OF_MEMORY")
        assert "MaxRSS 30.0G" in description
        assert "node spock-g3" in description
        assert "elapsed 01:02:03" in description
        assert "ExitCode 0:125" in description

    def test_failed_reports_exit_code(self):
        description = describe_slurm_job(parse_sacct_output(FAILED_JOB, "200"))

        assert description.startswith("FAILED")
        assert "ExitCode 1:0" in description
        assert "MaxRSS 1.5G" in description

    def test_cancelled_reports_who(self):
        description = describe_slurm_job(parse_sacct_output(CANCELLED_JOB, "300"))

        assert description.startswith("CANCELLED by uid 12345")

    def test_timeout_reports_the_time_limit(self):
        description = describe_slurm_job(parse_sacct_output(TIMEOUT_JOB, "400"))

        assert description.startswith("TIMEOUT")
        assert "elapsed 02:00:12 of 02:00:00 limit" in description

    def test_signal_is_named(self):
        info = parse_sacct_output(FAILED_JOB, "200")
        info["ExitCode"] = "0:9"
        assert "ExitCode 0:9 (SIGKILL)" in describe_slurm_job(info)

    def test_derived_exit_code_reported_when_it_adds_information(self):
        info = parse_sacct_output(FAILED_JOB, "200")
        info["ExitCode"] = "0:0"
        info["DerivedExitCode"] = "137:0"
        description = describe_slurm_job(info)
        assert "ExitCode" not in description.replace("DerivedExitCode", "")
        assert "DerivedExitCode 137:0" in description

    def test_zero_exit_codes_reason_none_and_empty_fields_are_omitted(self):
        description = describe_slurm_job(parse_sacct_output(RUNNING_JOB, "500"))

        assert (
            description == "RUNNING (node spock-g1, elapsed 00:01:00 of 02:00:00 limit)"
        )

    def test_reason_and_unassigned_node(self):
        description = describe_slurm_job(parse_sacct_output(PENDING_JOB, "600"))

        assert "reason Resources" in description
        assert "node" not in description

    def test_no_info(self):
        assert describe_slurm_job(None) == ""
        assert describe_slurm_job({}) == ""

    def test_description_is_bounded(self):
        info = parse_sacct_output(OOM_JOB, "1415")
        info["NodeList"] = "spock-g[" + ",".join(str(x) for x in range(500)) + "]"

        description = describe_slurm_job(info)

        assert len(description) <= 150
        assert description.startswith("OUT_OF_MEMORY")

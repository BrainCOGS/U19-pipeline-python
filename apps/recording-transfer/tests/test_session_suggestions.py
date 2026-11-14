from datetime import datetime
from pathlib import Path

from u19_recording_transfer.recording_options import matching_sessions
from u19_recording_transfer.session_suggestions import rank_sessions, recording_start


def test_spikeglx_gate_is_not_a_behavior_session_number():
    sessions = [
        dict(
            subject_fullname="jyanar_ya070", session_date="2026-09-01", session_number=3
        )
    ]
    matches, filtered = matching_sessions(
        r"D:\NPX_DATA\jyanar\ya070_20260901_g0", sessions
    )
    assert filtered and matches == sessions
    assert matches[0]["session_number"] == 3


def test_earliest_meta_time_and_interval_membership_rank_suggestions(tmp_path):
    (tmp_path / "a.ap.meta").write_text("fileCreateTime=2026-09-01T12:10:00\n")
    (tmp_path / "z.ap.meta").write_text("fileCreateTime=2026-09-01T12:00:00.123\n")
    inside = dict(
        session_number=1,
        session_start_time="2026-09-01T11:55:00",
        session_end_time="2026-09-01T12:05:00",
    )
    nearby = dict(
        session_number=2,
        session_start_time="2026-09-01T12:01:00",
        session_end_time="2026-09-01T12:20:00",
    )
    suggestions, evidence = rank_sessions(tmp_path, [nearby, inside])
    assert suggestions[0]["session"] == inside
    assert suggestions[0]["inside"] and len(suggestions) == 2
    assert any("Multiple plausible" in line for line in evidence)
    assert recording_start(tmp_path)[0].isoformat() == "2026-09-01T12:00:00.123000"


def test_bad_meta_is_a_warning_and_other_meta_still_works(tmp_path):
    (tmp_path / "bad.ap.meta").write_text("fileCreateTime=not-a-timestamp\n")
    (tmp_path / "good.ap.meta").write_text("fileCreateTime=2026-09-01T12:00:00\n")
    start, evidence = recording_start(tmp_path)
    assert start is not None and any("Invalid" in item for item in evidence)


def test_timezone_mismatch_never_silently_matches_local_clock(tmp_path):
    (tmp_path / "data.ap.meta").write_text("fileCreateTime=2026-09-01T12:00:00+00:00\n")
    suggestions, evidence = rank_sessions(
        tmp_path, [dict(session_number=1, session_start_time="2026-09-01T12:00:00")]
    )
    assert suggestions == [] and any("timezone" in line for line in evidence)


def test_missing_meta_does_not_guess_a_session(tmp_path):
    (tmp_path / "data.ap.bin").write_bytes(b"recording")
    assert rank_sessions(tmp_path, [dict(session_number=1)])[0] == []

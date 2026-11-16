"""Explain SpikeGLX timestamp evidence without automatically associating sessions."""

from datetime import datetime, timedelta
from pathlib import Path


def recording_start(source):
    from .app import inventory

    files = inventory(Path(source))
    metas = sorted(name for name in files if name.endswith(".ap.meta"))
    metas = metas or sorted(name for name in files if name.endswith(".meta"))
    timestamps = []
    warnings = []
    for name in metas:
        for line in (
            (Path(source) / name)
            .read_text(encoding="utf-8", errors="strict")
            .splitlines()
        ):
            if line.startswith("fileCreateTime="):
                try:
                    timestamps.append(
                        (datetime.fromisoformat(line.split("=", 1)[1].strip()), name)
                    )
                except ValueError:
                    warnings.append(f"Invalid fileCreateTime in {name}")
                break
    if not timestamps:
        return None, warnings or ["No SpikeGLX fileCreateTime available"]
    aware = {stamp.tzinfo is not None for stamp, _ in timestamps}
    if len(aware) > 1:
        return None, [*warnings, "Mixed timezone-aware and local metadata timestamps"]
    timestamps.sort(key=lambda item: item[0])
    stamp, name = timestamps[0]
    return stamp, [
        *warnings,
        f"Earliest metadata timestamp: {stamp.isoformat()} ({name})",
    ]


def rank_sessions(source, sessions, tolerance_minutes=15):
    """Return scored candidate hints; never return an automatic selected session."""
    if tolerance_minutes < 0:
        raise ValueError("Tolerance must be nonnegative.")
    start, evidence = recording_start(source)
    if start is None:
        return [], evidence
    tolerance = timedelta(minutes=tolerance_minutes)
    suggestions = []
    for session in sessions:
        raw_start = session.get("session_start_time")
        if not raw_start:
            continue
        try:
            session_start = (
                raw_start
                if isinstance(raw_start, datetime)
                else datetime.fromisoformat(raw_start)
            )
            raw_end = session.get("session_end_time")
            session_end = (
                raw_end
                if isinstance(raw_end, datetime)
                else datetime.fromisoformat(raw_end)
                if raw_end
                else None
            )
            if (session_start.tzinfo is None) != (start.tzinfo is None):
                evidence.append(
                    "Recording/session timezone formats differ; synchronize clocks before time matching"
                )
                continue
            inside = session_end is not None and session_start <= start <= session_end
            delta = abs(start - session_start)
            if inside or delta <= tolerance:
                suggestions.append(
                    dict(
                        session=session,
                        inside=inside,
                        distance_seconds=delta.total_seconds(),
                        reason=(
                            "Recording starts inside this behavior session"
                            if inside
                            else f"Recording starts {delta.total_seconds() / 60:.1f} min from session start"
                        ),
                    )
                )
        except (ValueError, TypeError):
            evidence.append(
                f"Invalid session timing for session {session.get('session_number')}"
            )
    suggestions.sort(key=lambda item: (not item["inside"], item["distance_seconds"]))
    if len(suggestions) > 1:
        evidence.append("Multiple plausible sessions; confirm explicitly")
    return suggestions, evidence

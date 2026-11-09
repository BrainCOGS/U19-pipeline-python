"""DB-free helpers for scripts/backfill_reward_scale.py.

Kept separate from the script so the checks that decide whether a value is
safe to write can be tested without a database connection.
"""

import csv
import math
from datetime import date, datetime, timedelta
from pathlib import Path, PureWindowsPath

import numpy as np

# Verdicts for a single block.
OK = "ok"                    # db value is the rounded/truncated file value
UNCHANGED = "unchanged"      # file value already stored exactly, nothing to do
FALLBACK = "fallback"        # db holds 0 from the `catch` branch in TowersBlock.m:89
MISMATCH = "mismatch"        # db value is not derivable from the file value
NO_VALUE = "no_value"        # rewardScale absent or unusable in the log
NO_BLOCK = "no_block"        # log has no block at this index

# SessionStarted.session_start_time is written when ViRMEn starts, the log's
# session.start a moment later, so they can straddle a minute boundary.
START_TIME_TOLERANCE = timedelta(minutes=2)

PATH_MAP_COLUMNS = ("subject_fullname", "session_date", "session_number", "path")


def round_half_away(x):
    """Match how MATLAB int8() and MySQL both coerce a double to an integer."""
    return math.floor(x + 0.5) if x >= 0 else math.ceil(x - 0.5)


def as_list(value):
    """A 1-element MATLAB struct array is squeezed to a dict by loadmat."""
    if isinstance(value, dict):
        return [value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if value is None:
        return []
    return list(value)


def scalar(value):
    """Unwrap a numpy scalar/0-d array, returning None if it isn't one number."""
    if isinstance(value, np.ndarray):
        if value.size != 1:
            return None
        value = value.item()
    if value is None or isinstance(value, (str, bytes, dict)):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def read_block_values(log, block_num):
    """Return (reward_scale, first_trial) for 1-indexed block_num.

    Mirrors TowersBlock.m:87 -- the stored scale is the *first* trial's, because
    autoAdjustReward can change it mid-block.  Any other choice would make
    backfilled rows mean something different from rows the nightly cron writes.
    """
    blocks = as_list(log.get("block"))
    if block_num < 1 or block_num > len(blocks):
        return None, None
    block = blocks[block_num - 1]
    if not isinstance(block, dict):
        return None, None

    first_trial = scalar(block.get("firstTrial"))
    if first_trial is not None:
        first_trial = int(first_trial)

    trials = as_list(block.get("trial"))
    reward_scale = None
    if trials and isinstance(trials[0], dict):
        reward_scale = scalar(trials[0].get("rewardScale"))

    return reward_scale, first_trial


def classify(db_scale, db_first_trial, file_scale, file_first_trial):
    """Decide whether this block is safe to update."""
    if file_scale is None:
        return NO_VALUE

    # Independent check that we are looking at the same block the DB row came
    # from, using a field the ingestion copied verbatim (TowersBlock.m:80).
    if (file_first_trial is not None and db_first_trial is not None
            and int(file_first_trial) != int(db_first_trial)):
        return MISMATCH

    db_scale = float(db_scale)
    if math.isclose(db_scale, file_scale, rel_tol=0, abs_tol=1e-6):
        return UNCHANGED

    if db_scale in (round_half_away(file_scale), math.trunc(file_scale)):
        return OK

    # TowersBlock.m:89 stores 0 when the field is missing; the file having a
    # value now means the read, not the data, was the problem back then.
    if db_scale == 0:
        return FALLBACK

    return MISMATCH


def _log_subject(log):
    animal = log.get("animal")
    if not isinstance(animal, dict):
        return None
    name = animal.get("name")
    if isinstance(name, np.ndarray):
        name = name.item() if name.size == 1 else None
    return str(name) if isinstance(name, str) else None


def _log_start(log):
    session = log.get("session")
    if not isinstance(session, dict):
        return None
    start = session.get("start")
    if start is None:
        return None
    parts = np.asarray(start, dtype=float).ravel()
    if parts.size < 5 or not np.all(np.isfinite(parts[:5])):
        return None
    try:
        return datetime(*(int(p) for p in parts[:5]))
    except ValueError:
        return None


def check_identity(log, subject_fullname, session_start_time, require):
    """Check that a log belongs to the given session.

    Compares log.animal.name to the subject and log.session.start to the
    session's start time.  The start time is what separates two sessions of
    the same mouse on the same day, which a date check alone cannot.

    When require is False (the file came from the path recorded in the DB),
    fields missing from the log are tolerated, but fields that are present
    and disagree still fail.  When require is True (the file was found
    somewhere else), both fields must be present and match.

    Returns (ok, reason) where reason is None when ok.
    """
    name = _log_subject(log)
    if name is None:
        if require:
            return False, "log has no animal.name to confirm the subject"
    elif name != subject_fullname:
        return False, f"log is for {name!r}, not {subject_fullname!r}"

    if session_start_time is None:
        return True, None

    start = _log_start(log)
    if start is None:
        if require:
            return False, "log has no session.start to confirm the session"
    elif abs(start - session_start_time) > START_TIME_TOLERANCE:
        return False, f"log session.start is {start}, session started {session_start_time}"

    return True, None


def recorded_basename(path):
    """File name from a recorded path, which may use either separator."""
    # A trailing separator means a directory; PureWindowsPath would otherwise
    # return that directory's name as though it were the file.
    if not path or path.endswith(("/", "\\")):
        return None
    name = PureWindowsPath(path).name  # splits on both / and \
    return name or None


def build_basename_index(dirs):
    """Map .mat file name -> every path under dirs with that name."""
    index = {}
    for directory in dirs:
        directory = Path(directory)
        if not directory.is_dir():
            raise FileNotFoundError(f"search directory does not exist: {directory}")
        for path in directory.rglob("*.mat"):
            if path.is_file():
                index.setdefault(path.name, []).append(path)
    return index


def session_key(subject_fullname, session_date, session_number):
    if isinstance(session_date, (date, datetime)):
        session_date = session_date.strftime("%Y-%m-%d")
    return str(subject_fullname), str(session_date), int(session_number)


def load_path_map(csv_path):
    """Read explicit session -> file overrides.  Rows with a blank path are skipped."""
    mapping = {}
    with open(csv_path, newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [col for col in PATH_MAP_COLUMNS if col not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{csv_path} is missing column(s): {', '.join(missing)}")
        for line, row in enumerate(reader, start=2):
            path = (row["path"] or "").strip()
            if not path:
                continue
            key = session_key(row["subject_fullname"].strip(), row["session_date"].strip(),
                              row["session_number"].strip())
            if key in mapping:
                raise ValueError(f"{csv_path}:{line} lists session {key} more than once")
            mapping[key] = Path(path)
    return mapping


def write_missing_csv(csv_path, missing):
    """Write unresolved sessions in the format load_path_map reads back.

    missing is an iterable of (subject_fullname, session_date, session_number,
    recorded_path).  The path column is left blank for the user to fill in.
    """
    with open(csv_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([*PATH_MAP_COLUMNS, "recorded_path"])
        for subject_fullname, session_date, session_number, recorded_path in missing:
            key = session_key(subject_fullname, session_date, session_number)
            writer.writerow([*key, "", recorded_path or ""])

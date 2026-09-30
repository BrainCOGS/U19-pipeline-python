"""Guards for pandas 3 behaviour changes (no database needed).

pandas 3 made copy-on-write the only mode: chained assignment such as
``df[mask]["col"] = v`` or ``df["col"].fillna(0, inplace=True)`` now changes a
temporary copy and leaves ``df`` untouched (pandas only warns with
``ChainedAssignmentError``). In the automatic-job handlers that would mean status or
error values silently not being updated before they are written to the database.
pandas 3 also stores strings in a dedicated string dtype, so ``object``-dtype checks
no longer match string columns.

These tests scan the source for both hazards, and check that the cron entry points
turn ``ChainedAssignmentError`` into an exception.
"""

import ast
import pathlib
import warnings

import pandas as pd
import pytest

ROOT = pathlib.Path(__file__).parents[1]
SOURCES = [ROOT / "u19_pipeline", ROOT / "scripts"]
INDEXERS = {"loc", "iloc", "at", "iat"}

# Chained-assignment shapes that are not pandas: (file, root variable) -> why.
NOT_PANDAS = {
    ("u19_pipeline/automatic_job/pupillometry_handler.py", "update_value_dict"): "dict",
    ("u19_pipeline/automatic_job/recording_handler.py", "update_value_dict"): "dict",
    ("u19_pipeline/automatic_job/recording_process_handler.py", "update_value_dict"): "dict",
    ("u19_pipeline/automatic_job/recording_process_handler.py", "update_dict"): "dict",
    ("u19_pipeline/utility.py", "pci"): "numpy array (np.zeros((2, n)))",
}

# Cron entry points (see u19_pipeline/automatic_job/crontab_example and the call_*.sh).
ENTRY_POINTS = [
    "u19_pipeline/alert_system/cronjob_alert.py",
    "u19_pipeline/automatic_job/cronjob_automatic_job.py",
    "u19_pipeline/automatic_job/populate_missing_syncbehavior_ephys.py",
    "u19_pipeline/automatic_job/pupillometry_check_handler_script.py",
    "u19_pipeline/automatic_job/pupillometry_handler_script.py",
]


def _root(node):
    while isinstance(node, (ast.Subscript, ast.Attribute, ast.Call)):
        node = node.func if isinstance(node, ast.Call) else node.value
    return node.id if isinstance(node, ast.Name) else None


def _chained(target):
    """x[a][b], x.loc[a][b], x[a].loc[b], x.values[...], x.to_numpy()[...]."""
    if not isinstance(target, ast.Subscript):
        return False
    inner = target.value
    if isinstance(inner, ast.Subscript):
        return True
    if isinstance(inner, ast.Attribute):
        if inner.attr in INDEXERS and isinstance(inner.value, ast.Subscript):
            return True
        return inner.attr == "values"
    return isinstance(inner, ast.Call) and getattr(inner.func, "attr", None) == "to_numpy"


def scan_chained_assignment(path):
    """(line, root variable, kind) for every copy-on-write hazard shape in ``path``."""
    tree = ast.parse(path.read_text())
    hits = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign):
            targets = n.targets
        elif isinstance(n, ast.AugAssign):
            targets = [n.target]
        else:
            targets = []
        hits += [(n.lineno, _root(t), "chained assignment") for t in targets if _chained(t)]
        if isinstance(n, ast.Call) and any(
            k.arg == "inplace" and isinstance(k.value, ast.Constant) and k.value.value is True
            for k in n.keywords
        ):
            owner = getattr(n.func, "value", None)
            if isinstance(owner, (ast.Subscript, ast.Attribute)):
                hits.append((n.lineno, _root(owner), "inplace=True on a selection"))
    return hits


def scan_object_dtype_checks(path):
    """Lines that test for object dtype, which pandas 3 string columns no longer have."""
    tree = ast.parse(path.read_text())
    hits = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "attr", None) in {
            "select_dtypes",
            "is_object_dtype",
        }:
            hits.append(n.lineno)
        if isinstance(n, ast.Compare):
            text = ast.unparse(n)
            if ".dtype" in text and any(s in text for s in ("object", "'O'", '"O"')):
                hits.append(n.lineno)
    return sorted(set(hits))


def _pandas_files():
    for src in SOURCES:
        for path in sorted(src.rglob("*.py")):
            if "pandas" in path.read_text():
                yield path


# ---- the scanners themselves ----


HAZARDS = """
import pandas as pd
df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
df[df.a > 1]["b"] = 0
df["b"][0] = 9
df.loc[0]["b"] = 9
df["a"].fillna(0, inplace=True)
df["a"].values[0] = 5
df["b"][0] += 1
"""

CORRECT = """
import pandas as pd
df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
df.loc[df.a > 1, "b"] = 0
df.loc[0, "b"] = 9
df["a"] = df["a"].fillna(0)
df.fillna(0, inplace=True)
x = df["a"][0]
"""


def test_scanner_finds_every_hazard_shape(tmp_path):
    f = tmp_path / "hazards.py"
    f.write_text(HAZARDS)
    assert sorted(line for line, *_ in scan_chained_assignment(f)) == [4, 5, 6, 7, 8, 9]


def test_scanner_passes_correct_pandas(tmp_path):
    f = tmp_path / "correct.py"
    f.write_text(CORRECT)
    assert scan_chained_assignment(f) == []


def test_object_dtype_scanner(tmp_path):
    f = tmp_path / "dtypes.py"
    f.write_text(
        "df.select_dtypes(include='object')\n"
        "if s.dtype == object: pass\n"
        "if s.dtype.kind == 'O': pass\n"
        "if s.dtype == 'int64': pass\n"
    )
    assert scan_object_dtype_checks(f) == [1, 2, 3]


# ---- the codebase ----


def test_no_chained_assignment_on_pandas_objects():
    found = []
    for path in _pandas_files():
        rel = path.relative_to(ROOT).as_posix()
        found += [
            f"{rel}:{line} {kind} on {root!r}"
            for line, root, kind in scan_chained_assignment(path)
            if (rel, root) not in NOT_PANDAS
        ]
    assert not found, (
        "pandas 3 ignores these writes (copy-on-write); use df.loc[rows, col] = ... "
        "or reassign, or add a NOT_PANDAS entry if the object is not pandas:\n  "
        + "\n  ".join(found)
    )


def test_not_pandas_allowlist_is_current():
    """Every allowlist entry still matches something, so stale entries get removed."""
    used = {
        (path.relative_to(ROOT).as_posix(), root)
        for path in _pandas_files()
        for _, root, _ in scan_chained_assignment(path)
    }
    assert set(NOT_PANDAS) <= used, sorted(set(NOT_PANDAS) - used)


def test_no_object_dtype_checks():
    found = [
        f"{path.relative_to(ROOT).as_posix()}:{line}"
        for path in _pandas_files()
        for line in scan_object_dtype_checks(path)
    ]
    assert not found, (
        "pandas 3 string columns are not object dtype; use "
        "pd.api.types.is_string_dtype or compare values instead:\n  " + "\n  ".join(found)
    )


# ---- runtime guard ----


def test_guard_turns_chained_assignment_into_an_error():
    from u19_pipeline.utils.pandas_guard import raise_on_chained_assignment

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with warnings.catch_warnings():
        raise_on_chained_assignment()
        with pytest.raises(pd.errors.ChainedAssignmentError):
            df[df.a > 1]["b"] = 0
    assert df["b"].tolist() == [3, 4]


def test_guard_leaves_correct_writes_alone():
    from u19_pipeline.utils.pandas_guard import raise_on_chained_assignment

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with warnings.catch_warnings():
        raise_on_chained_assignment()
        df.loc[df.a > 1, "b"] = 0
    assert df["b"].tolist() == [3, 0]


def _func_name(call):
    f = call.func
    return f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)


@pytest.mark.parametrize("script", ENTRY_POINTS)
def test_entry_points_install_the_guard(script):
    """Each cron entry point calls raise_on_chained_assignment() at module level."""
    tree = ast.parse((ROOT / script).read_text())
    calls = [
        n
        for n in tree.body
        if isinstance(n, ast.Expr)
        and isinstance(n.value, ast.Call)
        and _func_name(n.value) == "raise_on_chained_assignment"
    ]
    assert calls, f"{script} does not call raise_on_chained_assignment()"


def test_pytest_promotes_chained_assignment_warning():
    """pyproject's pytest filterwarnings makes ChainedAssignmentError fail any test."""
    with pytest.raises(pd.errors.ChainedAssignmentError):
        warnings.warn("probe", pd.errors.ChainedAssignmentError, stacklevel=1)

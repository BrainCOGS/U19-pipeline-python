"""Tests for the ``--require-extras`` option in ``tests/conftest.py``.

Each case runs an inner pytest session (via ``pytester``) that uses a copy of
the real conftest. Plugin autoloading is off so third-party pytest plugins
pulled in by the extras (dash, dandi, faker) don't run inside it.
"""

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

CONFTEST = (Path(__file__).parent / "conftest.py").read_text()

MODULE_LEVEL_MISSING = """
import pytest
pytest.importorskip("u19_no_such_module")

def test_x():
    pass
"""

TEST_LEVEL_MISSING = """
import pytest

def test_x():
    pytest.importorskip("u19_no_such_module")
"""

PLAIN_SKIP = """
import pytest

def test_x():
    pytest.skip("not about extras")
"""

PRESENT = """
import pytest
json = pytest.importorskip("json")

def test_x():
    assert json.loads("0") == 0
"""


@pytest.fixture
def run(pytester, monkeypatch):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    pytester.makeconftest(CONFTEST)

    def _run(source, *args):
        pytester.makepyfile(test_inner=source)
        return pytester.runpytest_inprocess(*args)

    return _run


@pytest.mark.parametrize(
    "source", [MODULE_LEVEL_MISSING, TEST_LEVEL_MISSING], ids=["module", "test"]
)
def test_missing_import_skips_by_default(run, source):
    result = run(source)
    result.assert_outcomes(skipped=1)
    assert result.ret in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED)


def test_missing_module_level_import_fails_with_flag(run):
    result = run(MODULE_LEVEL_MISSING, "--require-extras")
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*--require-extras*u19_no_such_module*"])


def test_missing_test_level_import_fails_with_flag(run):
    result = run(TEST_LEVEL_MISSING, "--require-extras")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*--require-extras*u19_no_such_module*"])


def test_plain_skip_is_left_alone_with_flag(run):
    run(PLAIN_SKIP, "--require-extras").assert_outcomes(skipped=1)


@pytest.mark.parametrize("args", [(), ("--require-extras",)], ids=["default", "flag"])
def test_present_import_runs(run, args):
    run(PRESENT, *args).assert_outcomes(passed=1)

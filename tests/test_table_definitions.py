"""Check, without a database, that Python classes resolve to the existing table names.

A class that resolves to a name that isn't in the database makes DataJoint
declare a new table when the module is imported, so a wrong tier writes to the
database on import.  The expected names match U19-pipeline-matlab, where
``acquisition.SessionBlockTrial`` is a ``dj.Part`` of ``acquisition.SessionBlock``.
"""

import importlib
import inspect
import re
import sys
from types import ModuleType

import datajoint as dj
import pytest

TABLE_MODULES = ("lab", "task", "subject", "acquisition", "behavior")


@pytest.fixture
def modules(monkeypatch):
    """Import the table modules with unactivated schemas, which need no connection."""
    import u19_pipeline

    for name in TABLE_MODULES:
        monkeypatch.delitem(sys.modules, f"u19_pipeline.{name}", raising=False)
        monkeypatch.delattr(u19_pipeline, name, raising=False)
    monkeypatch.setitem(dj.config, "custom", {"database.prefix": "u19_"})
    monkeypatch.setattr(dj, "schema", lambda *args, **kwargs: dj.Schema())
    monkeypatch.setattr(dj, "VirtualModule", lambda name, *args: ModuleType(name))

    loaded = {
        name: importlib.import_module(f"u19_pipeline.{name}") for name in TABLE_MODULES
    }
    for module in loaded.values():
        for master in _tables(module):
            # DataJoint sets this when the schema is activated.
            for part in _parts(master):
                part._master = master
    yield loaded

    for name in TABLE_MODULES:
        sys.modules.pop(f"u19_pipeline.{name}", None)
        if hasattr(u19_pipeline, name):
            delattr(u19_pipeline, name)


def _tables(module):
    # vars(), not inspect.getmembers(): DataJoint's class properties raise
    # on a table whose schema isn't activated.
    return [
        cls
        for cls in vars(module).values()
        if inspect.isclass(cls)
        and cls.__module__ == module.__name__
        and issubclass(cls, dj.user_tables.UserTable)
    ]


def _parts(master):
    return [
        cls
        for cls in vars(master).values()
        if inspect.isclass(cls) and issubclass(cls, dj.Part)
    ]


def _table_names(module):
    return {
        table.table_name
        for master in _tables(module)
        for table in (master, *_parts(master))
    }


def test_session_block_tables_match_the_database(modules):
    names = _table_names(modules["acquisition"])

    assert {name for name in names if name.startswith("_session_block")} == {
        "_session_block",
        "_session_block__trial",
    }


def test_session_block_trial_key_comes_from_session_block(modules):
    trial = modules["acquisition"].SessionBlock.Trial

    primary_key = trial.definition.partition("---")[0]
    assert re.search(r"->\s*master\b", primary_key)
    assert re.search(r"^\s*trial_idx\s*:\s*int\b", primary_key, re.MULTILINE)


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("TowersBlock", "_session_block"),
        ("TowersBlock.Trial", "_session_block__trial"),
    ],
)
def test_towers_block_references_session_block_tables(modules, table, expected):
    behavior = modules["behavior"]
    cls = behavior
    for attribute in table.split("."):
        cls = getattr(cls, attribute)

    references = re.findall(r"->\s*acquisition\.([\w.]+)", cls.definition)
    assert len(references) == 1
    parent = behavior.acquisition
    for attribute in references[0].split("."):
        parent = getattr(parent, attribute)
    assert parent.table_name == expected

"""Post-processing step lists for ephys jobs (the counterpart of the PreCluster tables).

A job can select an ordered list of post-processing steps, built from paramsets the same
way its preprocessing is. BrainCogsEphysSorters runs them after the sort: ``spikeinterface``
(sorting analyzer extensions) and ``bombcell`` (unit labels good / mua / noise /
non_soma). The tables mirror element-array-ephys's ``ephys_precluster``:

  PostClusterMethod          the methods (lookup)
  PostClusterParamSet        method + params, one row per distinct params
  PostClusterParamSteps      a named, ordered list of paramsets
  PostClusterParamSteps.Step
  EphysPostParams            the list a job (recording_process.Processing) selected

The first four live in the ephys element schema (``<prefix>pipeline_ephys_element``),
next to the PreCluster tables, so the website and RecordingProcessJobGUI find them where
they find those. ``EphysPostParams`` lives in ``<prefix>recording_process``, next to
``Processing.EphysParams``. It is a table of its own, not a new part of ``Processing`` or
a new column of ``EphysParams``, so no existing table is altered. A job without a row
has no post-processing, which is every job until someone selects a list.

The schemas are not activated on import: importing this module never creates a table.
``scripts/create_ephys_postcluster_tables.py`` declares them (``activate(create=True)``)
once, by hand. Everything else calls ``tables()``, which returns None until they exist.
"""

import types

import datajoint as dj

ELEMENT_SCHEMA_SUFFIX = "pipeline_ephys_element"
RECORDING_PROCESS_SCHEMA_SUFFIX = "recording_process"

element_schema = dj.Schema()
job_schema = dj.Schema()


@element_schema
class PostClusterMethod(dj.Lookup):
    definition = """
    # Method for post-clustering (post-processing) of a sort
    postcluster_method: varchar(16)
    ---
    postcluster_method_desc: varchar(1000)
    """


@element_schema
class PostClusterParamSet(dj.Lookup):
    definition = """
    # Parameter set of one post-processing step
    paramset_idx: smallint
    ---
    -> PostClusterMethod
    paramset_desc: varchar(128)
    param_set_hash: uuid
    unique index (param_set_hash)
    params: longblob  # dictionary of all applicable parameters
    """


@element_schema
class PostClusterParamSteps(dj.Lookup):
    definition = """
    # Ordered list of post-processing paramsets
    postcluster_param_steps_id: smallint
    ---
    postcluster_param_steps_name: varchar(32)
    postcluster_param_steps_desc: varchar(128)
    """

    class Step(dj.Part):
        definition = """
        -> master
        step_number: smallint  # order of operations
        ---
        -> PostClusterParamSet
        """


@job_schema
class EphysPostParams(dj.Manual):
    definition = """
    # The post-processing step list an ephys job selected (no row: no post-processing)
    -> recording_process.Processing
    ---
    -> PostClusterParamSteps
    """


TABLES = (
    PostClusterMethod,
    PostClusterParamSet,
    PostClusterParamSteps,
    EphysPostParams,
)


def schema_names(prefix):
    return prefix + ELEMENT_SCHEMA_SUFFIX, prefix + RECORDING_PROCESS_SCHEMA_SUFFIX


def activate(prefix=None, create=False, recording_process=None):
    """Bind the tables to their schemas; with ``create=True`` declare missing ones.

    ``recording_process``: the module whose ``Processing`` EphysPostParams refers to.
    By default a virtual module of ``<prefix>recording_process``, so this works without
    importing the pipeline (and its element dependencies).
    """

    if prefix is None:
        prefix = dj.config["custom"]["database.prefix"]
    element_name, job_name = schema_names(prefix)
    if recording_process is None:
        recording_process = dj.VirtualModule("recording_process", job_name)
    element_schema.activate(element_name, create_schema=create, create_tables=create)
    job_schema.activate(
        job_name,
        create_schema=create,
        create_tables=create,
        add_objects={
            "recording_process": recording_process,
            "PostClusterParamSteps": PostClusterParamSteps,
        },
    )
    return types.SimpleNamespace(**{t.__name__: t for t in TABLES})


_tables = None


def tables():
    """The tables, or None while they have not been created in this database yet."""

    global _tables
    if _tables is None:
        try:
            activated = activate()
        except (dj.DataJointError, KeyError) as error:  # KeyError: no database.prefix
            print("post-processing tables unavailable:", error)
            return None
        if not all(t.is_declared for t in TABLES):
            return None
        _tables = activated
    return _tables

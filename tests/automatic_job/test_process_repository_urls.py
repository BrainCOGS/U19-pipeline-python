"""Every processing repository a cluster job runs from is pulled before the job is queued.

``params_config`` declares DataJoint schemas at import, so its two literals are read from the
source instead of importing it.
"""

import ast
import pathlib

import u19_pipeline.automatic_job as automatic_job

PARAMS_CONFIG = pathlib.Path(automatic_job.__file__).parent / "params_config.py"


def literal(name):
    tree = ast.parse(PARAMS_CONFIG.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {PARAMS_CONFIG}")


def test_every_cluster_repository_is_pulled_before_its_jobs():
    modalities = literal("recording_modality_dict")
    git_urls = literal("process_repository_git_urls")

    cluster_repositories = {
        m["process_repository"]
        for m in modalities
        if m["local_or_cluster"] == "cluster"
    }
    assert cluster_repositories <= set(git_urls)


def test_imaging_checkout_is_pulled_from_u19():
    git_urls = literal("process_repository_git_urls")

    # public https: the cluster account needs no GitHub key
    assert (
        git_urls["U19-pipeline-python"]
        == "https://github.com/BrainCOGS/U19-pipeline-python.git"
    )

"""Per-modality processing parameters (cluster, repo, script) for tasks.

Wraps the modality config in
``u19_pipeline.automatic_job.params_config.recording_modality_df`` (the same
``program_selection_params`` dict the legacy handler passes to slurm_creator /
transfers): ``local_or_cluster``, ``process_cluster``, ``process_repository``,
``process_script``.
"""

from __future__ import annotations


def program_selection_params_for(modality: str) -> dict:
    """Return the ``program_selection_params`` dict for a modality."""
    import u19_pipeline.automatic_job.params_config as config

    df = config.recording_modality_df
    row = df.loc[df["recording_modality"] == modality]
    if row.empty:
        raise ValueError(f"no modality config for {modality!r}")
    return row.to_dict("records")[0]

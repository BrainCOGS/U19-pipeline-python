"""Slurm resources an ephys job asks for, derived from its paramsets."""

import pytest

from u19_pipeline.automatic_job import ephys_slurm_resources as esr

LEGACY_KS4 = {"clustering_method": "kilosort4", "n_chan_bin": 385}
CATGT = [{"catgt": {"apfilter": ["biquad", 2, 300, 0], "extras": ["ap", "gblcar"]}}]
NOTHING = [{"Nothing": {"gfix": [0.4, 0.1, 0.02]}}]
SI_STEPS = {"steps": [{"name": "phase_shift", "kwargs": {}}]}
SI_KS4 = {
    "clustering_method": "kilosort4",
    "sorter_params": {},
    "si_params": {"save_preprocessed_copy": True},
    "preprocessing": SI_STEPS,
}
SPOCK_DEFAULT = {
    "nodes": 1,
    "ntasks": 1,
    "cpus-per-task": 4,
    "time": "30:00:00",
    "mem": "80G",
    "gres": "gpu:1",
}


# ------------------------------------------------------------- jobs that stay as they were


@pytest.mark.parametrize("preparams", [CATGT, NOTHING, [], None])
def test_direct_kilosort_job_changes_nothing(preparams):
    assert esr.ephys_slurm_overrides(LEGACY_KS4, preparams) == {}
    assert esr.apply_overrides(SPOCK_DEFAULT, {}) == SPOCK_DEFAULT


def test_legacy_paramset_resources_key_is_a_sorter_setting_not_a_request():
    params = {**LEGACY_KS4, "resources": {"cpus": 32}}
    assert esr.ephys_slurm_overrides(params, NOTHING) == {}


@pytest.mark.parametrize("params", [None, {}, "kilosort4", []])
def test_odd_params_do_not_break_slurm_generation(params):
    assert esr.ephys_slurm_overrides(params, None) == {}


# --------------------------------------------------------------------------------- cores


def test_spikeinterface_job_that_asks_for_nothing_keeps_the_cluster_default():
    assert esr.ephys_slurm_overrides(SI_KS4, NOTHING) == {}
    assert esr.apply_overrides(SPOCK_DEFAULT, {})["cpus-per-task"] == 4


@pytest.mark.parametrize("method", ["spikeinterface", "dredge"])
def test_spikeinterface_preprocessing_without_n_jobs_keeps_the_default(method):
    assert esr.ephys_slurm_overrides(LEGACY_KS4, [{method: {}}]) == {}


def test_n_jobs_in_the_paramset_is_the_core_request():
    params = {**SI_KS4, "si_params": {"n_jobs": 12}}
    assert esr.ephys_slurm_overrides(params, NOTHING)["cpus-per-task"] == 12


def test_largest_n_jobs_wins():
    params = {
        **SI_KS4,
        "si_params": {"n_jobs": 4},
        "preprocessing": {**SI_STEPS, "job_kwargs": {"n_jobs": 6}},
        "postprocessing": {"extensions": {}, "job_kwargs": {"n_jobs": 16}},
    }
    preparams = [{"spikeinterface": {**SI_STEPS, "job_kwargs": {"n_jobs": 10}}}]
    assert esr.ephys_slurm_overrides(params, preparams)["cpus-per-task"] == 16


def test_n_jobs_of_a_spikeinterface_step_counts_for_a_legacy_sorter():
    preparams = [{"dredge": {"job_kwargs": {"n_jobs": 24}}}]
    assert esr.ephys_slurm_overrides(LEGACY_KS4, preparams)["cpus-per-task"] == 24


@pytest.mark.parametrize("n_jobs", [-1, 0, 0.5, "8", None, True])
def test_n_jobs_that_is_not_a_core_count_is_not_a_request(n_jobs):
    params = {**SI_KS4, "si_params": {"n_jobs": n_jobs}}
    assert esr.ephys_slurm_overrides(params, NOTHING) == {}


def test_resources_cpus_wins_over_n_jobs():
    params = {**SI_KS4, "si_params": {"n_jobs": 4}, "resources": {"cpus": 20}}
    assert esr.ephys_slurm_overrides(params, NOTHING)["cpus-per-task"] == 20


# ----------------------------------------------------------------------- gpu, memory, time


def test_gpu_memory_and_time_come_from_resources():
    params = {**SI_KS4, "resources": {"gpus": 2, "mem": "120G", "time": "12:00:00"}}
    overrides = esr.ephys_slurm_overrides(params, NOTHING)
    assert overrides == {"gres": "gpu:2", "mem": "120G", "time": "12:00:00"}
    assert esr.apply_overrides(SPOCK_DEFAULT, overrides) == {
        "nodes": 1,
        "ntasks": 1,
        "cpus-per-task": 4,
        "time": "12:00:00",
        "mem": "120G",
        "gres": "gpu:2",
    }


def test_all_four_resources_together():
    resources = {"cpus": 6, "gpus": 1, "mem": "60G", "time": "02:00:00"}
    merged = esr.apply_overrides(
        SPOCK_DEFAULT,
        esr.ephys_slurm_overrides({**SI_KS4, "resources": resources}, NOTHING),
    )
    assert (merged["cpus-per-task"], merged["gres"], merged["mem"], merged["time"]) == (
        6,
        "gpu:1",
        "60G",
        "02:00:00",
    )


def test_zero_gpus_drops_the_gres_line():
    """A CPU-only sorter should not wait for, or hold, a GPU."""
    params = {**SI_KS4, "clustering_method": "spykingcircus2", "resources": {"gpus": 0}}
    overrides = esr.ephys_slurm_overrides(params, NOTHING)
    assert overrides["gres"] is None
    assert "gres" not in esr.apply_overrides(SPOCK_DEFAULT, overrides)


def test_without_resources_the_cluster_defaults_stay():
    merged = esr.apply_overrides(
        SPOCK_DEFAULT, esr.ephys_slurm_overrides(SI_KS4, NOTHING)
    )
    assert (merged["gres"], merged["mem"], merged["time"]) == (
        "gpu:1",
        "80G",
        "30:00:00",
    )


def test_apply_overrides_does_not_touch_the_shared_default():
    before = dict(SPOCK_DEFAULT)
    esr.apply_overrides(SPOCK_DEFAULT, {"gres": None, "cpus-per-task": 4})
    assert before == SPOCK_DEFAULT


@pytest.mark.parametrize("resources", [None, {}])
def test_empty_resources(resources):
    params = {**SI_KS4, "resources": resources}
    assert esr.ephys_slurm_overrides(params, NOTHING) == {}


@pytest.mark.parametrize(
    "resources",
    [
        [],
        "8",
        {"cores": 8},
        {"cpus": 0},
        {"cpus": -4},
        {"cpus": 2.5},
        {"cpus": "8"},
        {"cpus": True},
        {"gpus": -1},
        {"gpus": "1"},
        {"gpus": 1.0},
        {"mem": 80},
        {"mem": "80"},
        {"mem": "80 G"},
        {"mem": "80G; rm -rf /"},
        {"time": 30},
        {"time": "30h"},
        {"time": "30:00:00\n#SBATCH --account=x"},
    ],
)
def test_resources_slurm_would_refuse_are_rejected(resources):
    with pytest.raises(esr.EphysResourcesError):
        esr.ephys_slurm_overrides({**SI_KS4, "resources": resources}, NOTHING)


@pytest.mark.parametrize("time", ["30:00:00", "2-00:00:00", "0:30:00", "100:00:00"])
def test_time_formats(time):
    assert esr.validate_resources({"time": time}) == {"time": time}

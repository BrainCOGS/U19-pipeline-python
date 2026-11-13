import copy
import os
import subprocess
import pathlib
import json
import re
import shlex
import datajoint as dj
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
from u19_pipeline.utility import create_str_from_dict, is_this_spock
import u19_pipeline.automatic_job.params_config as config
from u19_pipeline.utils.file_utils import write_file
import u19_pipeline.utils.slurm_utils as slurm_utils

# Functions to create slurm jobs

slurms_filepath = "u19_pipeline/automatic_job/SlurmFiles"
default_slurm_filename = "slurm_real.slurm"

default_preprocessing_tool = "kilosort2"
default_matlab_ver = "R2020b"


def generate_slurm_file(job_id, program_selection_params):
    """
    Generate and send slurm file to be queued in processing cluster
    """

    # Get all associated directories given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(program_selection_params["process_cluster"])

    # Start with default values
    slurm_dict = copy.deepcopy(cluster_vars["slurm_default"])
    label_rec_process = "job_id_" + str(job_id)
    slurm_dict["job-name"] = label_rec_process

    slurm_dict["output"] = str(
        pathlib.Path(cluster_vars["log_files_dir"], label_rec_process + ".log")
    )
    slurm_dict["error"] = str(
        pathlib.Path(cluster_vars["error_files_dir"], label_rec_process + ".log")
    )

    print("slurm_dict", slurm_dict)

    if program_selection_params["process_cluster"] == "spock":
        if program_selection_params.get("recording_modality") == "imaging":
            suite2p_job_file = pathlib.Path(
                cluster_vars["params_files_dir"],
                config.default_suite2p_job_filename % job_id,
            ).as_posix()
            slurm_text = generate_slurm_spock_imaging(slurm_dict, suite2p_job_file)
        else:
            slurm_text = generate_slurm_spock(slurm_dict)
    else:
        slurm_text = generate_slurm_tiger(slurm_dict)

    slurm_file_name = default_slurm_filename
    slurm_file_local_path = str(pathlib.Path(slurms_filepath, slurm_file_name))

    print(slurm_file_local_path)
    print(cluster_vars["slurm_files_dir"])
    print(slurm_file_name)

    write_file(slurm_file_local_path, slurm_text)

    if program_selection_params["process_cluster"] == "spock" and is_this_spock():
        status = config.system_process["SUCCESS"]
        slurm_destination = slurm_file_local_path
    else:
        slurm_destination = pathlib.Path(
            cluster_vars["slurm_files_dir"], slurm_file_name
        ).as_posix()
        status = transfer_slurm_file(
            slurm_file_local_path, slurm_destination, cluster_vars
        )

    print(status)
    print(slurm_destination)
    print(cluster_vars)

    return status, slurm_destination


def queue_slurm_file(
    job_id,
    program_selection_params,
    raw_directory,
    proc_directory,
    modality,
    slurm_location,
):

    id_slurm_job = -1
    job_id = str(job_id)

    # Get all associated variables given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(program_selection_params["process_cluster"])

    print("queue_slurm_file **********************************")

    processing_repository = program_selection_params["process_repository"]
    repository_dir = pathlib.Path(
        cluster_vars[modality + "_process_dir"], processing_repository
    ).as_posix()

    command = [
        "ssh",
        cluster_vars["user"] + "@" + cluster_vars["hostname"],
        "sbatch",
        "--export=recording_process_id="
        + job_id
        + ",raw_data_directory='"
        + raw_directory
        + "',processed_data_directory='"
        + proc_directory
        + "',repository_dir='"
        + repository_dir
        + "',process_script_path='"
        + program_selection_params["process_script"]
        + "'",
        slurm_location,
    ]

    if program_selection_params["process_cluster"] == "spock" and is_this_spock():
        command = command[2:]

    # The job's uv environment has to be complete before sbatch: nodes have no internet
    if ephys_job_uses_uv(program_selection_params):
        status, error_message = sync_uv_env(cluster_vars, repository_dir)
        if status != config.system_process["SUCCESS"]:
            return status, id_slurm_job, error_message

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # p = os.popen(command_new).read()
    p.wait()
    stdout, stderr = p.communicate()

    print(stdout)
    print(stderr)

    if p.returncode == config.system_process["SUCCESS"]:
        error_message = ""
        batch_job_sentence = stdout.decode("UTF-8")
        print("batch_job_sentence", batch_job_sentence)
        id_slurm_job = batch_job_sentence.replace("Submitted batch job ", "")
        id_slurm_job = re.sub(r"[\n\t\s]*", "", id_slurm_job)
    else:
        error_message = stderr.decode("UTF-8")

    return p.returncode, id_slurm_job, error_message


def run_sacct(ssh_user, host, sacct_args, local_user=False, timeout=120):
    """
    Run sacct (through ssh unless local_user)
    Returns:
    returncode (int) = return code of the command (-1 if it could not be run or timed out)
    stdout     (str) = output of sacct
    stderr     (str) = error of sacct/ssh (or why it could not be run)
    """
    command = ["sacct"] + sacct_args
    if not local_user:
        command = ["ssh", ssh_user + "@" + host] + command

    try:
        p = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        return -1, "", "sacct did not answer in " + str(timeout) + " s"
    except OSError as e:
        return -1, "", str(e)

    return p.returncode, p.stdout, p.stderr


def check_slurm_job(ssh_user, host, jobid, local_user=False):
    """
    Check the state of a slurm job
    Returns:
    state_pipeline (int) = status_update_idx (NO_CHANGE if the state could not be retrieved, to retry later)
    error_message  (str) = slurm id & how the job ended (state, exit code, node, time & memory) if it errored
    """
    jobid = str(jobid)

    sacct_args = [
        "-j",
        jobid,
        "-P",
        "-n",
        "--format=" + ",".join(slurm_utils.sacct_format),
    ]
    returncode, stdout, stderr = run_sacct(
        ssh_user, host, sacct_args, local_user=local_user
    )
    job_info = None
    if returncode == config.system_process["SUCCESS"]:
        job_info = slurm_utils.parse_sacct_output(stdout, jobid)
    else:
        # Older sacct may not know some of the fields, retry asking only for the state
        returncode, stdout, stderr = run_sacct(
            ssh_user,
            host,
            ["-j", jobid, "-P", "-n", "--format=State"],
            local_user=local_user,
        )
        if returncode == config.system_process["SUCCESS"] and stdout.strip():
            job_info = {"State": stdout.strip().splitlines()[0].strip()}

    if returncode != config.system_process["SUCCESS"]:
        # A failed ssh/sacct call says nothing about the job, check again next time
        error_message = (
            "Failed to retrieve status of slurm job "
            + jobid
            + ": "
            + " ".join(stderr.split())
        )
        print(error_message)
        return config.status_update_idx["NO_CHANGE"], error_message[:255]

    if job_info is None:
        return config.status_update_idx[
            "ERROR_STATUS"
        ], "slurm " + jobid + " not found in sacct"

    # "CANCELLED by <uid>" -> "CANCELLED"
    state_slurm_job = job_info["State"].split(" ")[0].rstrip("+")
    if state_slurm_job not in config.slurm_states:
        return config.status_update_idx[
            "ERROR_STATUS"
        ], "slurm " + jobid + " unexpected state " + slurm_utils.describe_slurm_job(
            job_info
        )

    state_pipeline = config.slurm_states[state_slurm_job]["pipeline_status"]
    error_message = ""
    if state_pipeline == config.status_update_idx["ERROR_STATUS"]:
        # The slurm id is replaced when the job is resubmitted, keep it in the message
        error_message = (
            "slurm " + jobid + " " + slurm_utils.describe_slurm_job(job_info)
        )

    print("state_pipeline ....", state_pipeline)
    print("error_message", error_message)

    return state_pipeline, error_message


def transfer_slurm_file(slurm_file_local_path, slurm_destination, cluster_vars):
    """
    Create scp command from cluster directories and local slurm file
    """

    user_host = cluster_vars["user"] + "@" + cluster_vars["hostname"]
    slurm_destination = user_host + ":" + slurm_destination
    status = ft.scp_file_transfer(slurm_file_local_path, slurm_destination)

    return status


def create_slurm_params_file(slurm_dict):

    text_dict = ""
    for slurm_param in slurm_dict.keys():
        if isinstance(slurm_dict[slurm_param], list):
            for list_param in slurm_dict[slurm_param]:
                text_dict += (
                    "#SBATCH --" + str(slurm_param) + "=" + str(list_param) + "\n"
                )
        else:
            text_dict += (
                "#SBATCH --"
                + str(slurm_param)
                + "="
                + str(slurm_dict[slurm_param])
                + "\n"
            )

    return text_dict


# Which python environment ephys jobs on spock run in. Set in dj_local_conf.json:
#   "custom": {"spock_ephys_env": "uv"}
# "conda" (the default): the hand-maintained conda environment u19_pipeline_python_env3.
# "uv": the environment uv builds from the processing repository's uv.lock, in
#       <repository_dir>/.venv. sync_uv_env brings it up to date on the login node before
#       every sbatch, and the job stops before it starts if it does not match the lock.
# Imaging jobs (generate_slurm_spock_imaging) and tiger jobs are not affected.
spock_ephys_env_key = "spock_ephys_env"
spock_ephys_envs = ("conda", "uv")

# A first uv sync downloads torch and the CUDA libraries (several minutes); after that it
# takes about a second. A hung ssh must not block the handler forever.
uv_sync_timeout_s = 30 * 60


def spock_ephys_env():
    """
    Python environment for ephys jobs on spock: "conda" or "uv" (see spock_ephys_env_key)
    """

    custom = dj.config.get("custom") or {}
    env = str(custom.get(spock_ephys_env_key, "conda")).strip().lower()
    if env not in spock_ephys_envs:
        raise ValueError(
            "dj.config['custom']['"
            + spock_ephys_env_key
            + "'] must be one of "
            + str(spock_ephys_envs)
            + f", got {custom.get(spock_ephys_env_key)!r}"
        )

    return env


def ephys_job_uses_uv(program_selection_params):
    """
    True for the jobs generate_slurm_file writes with the uv version of generate_slurm_spock
    """

    return (
        program_selection_params["process_cluster"] == "spock"
        and program_selection_params.get("recording_modality") != "imaging"
        and spock_ephys_env() == "uv"
    )


def sync_uv_env(cluster_vars, repository_dir):
    """
    Bring the uv environment of repository_dir up to date on the cluster login node

    Compute nodes have no internet, so python, the packages and the environment itself
    (<repository_dir>/.venv) have to be in place before sbatch. "uv sync --locked" installs
    exactly what uv.lock lists, does nothing when the environment already matches, and
    fails when uv.lock no longer matches pyproject.toml.

    Returns:
    status        (int) = system_process SUCCESS, or what uv/ssh returned
    error_message (str) = why the sync failed ('' if it did not)
    """

    remote = "cd " + shlex.quote(repository_dir) + " && uv sync --locked"
    if is_this_spock():
        # A login shell, so uv is on PATH as it is for the user
        command = ["bash", "-lc", remote]
    else:
        # ssh joins its arguments with spaces for the remote shell: quote the script, or
        # only "cd" would run under bash -lc
        command = [
            "ssh",
            cluster_vars["user"] + "@" + cluster_vars["hostname"],
            "bash -lc " + shlex.quote(remote),
        ]

    print("sync_uv_env", command)
    failed = "uv sync --locked failed in " + repository_dir + ": "
    try:
        p = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=uv_sync_timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return config.system_process["ERROR"], (
            failed + "no answer in " + str(uv_sync_timeout_s) + " s"
        )
    except OSError as e:
        return config.system_process["ERROR"], failed + str(e)

    print(p.stdout)
    print(p.stderr)

    if p.returncode == config.system_process["SUCCESS"]:
        return p.returncode, ""

    reason = " ".join(p.stderr.split()) or "exit code " + str(p.returncode)
    return p.returncode, failed + reason


# How an ephys job on spock gets its python, for each value of spock_ephys_env().
# matlab is for the Kilosort 2 / 3 sorters, which run in MATLAB.
spock_ephys_env_setup = {
    "conda": """
    source ~/.bashrc
    module load anacondapy/2023.07-cuda -s
    module load matlab/R2024a -s

    conda activate u19_pipeline_python_env3

    cd ${repository_dir}
    python -u ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    """,
    "uv": """
    source ~/.bashrc
    module load matlab/R2024a -s

    # Without the checkout the script path would resolve against the submit directory
    cd ${repository_dir} || { echo "repository_dir not found: ${repository_dir}" >&2; exit 1; }

    # The node has no internet and other jobs share this environment, so nothing is
    # installed here: the job only runs if .venv already matches uv.lock.
    uv sync --frozen --offline --check || { echo "the uv environment ${repository_dir}/.venv is missing or does not match uv.lock (or uv is not on PATH): run 'uv sync --locked' in ${repository_dir} on the login node" >&2; exit 1; }

    source .venv/bin/activate
    python -c 'import sys; print("PYTHON:", sys.executable, sys.version.split()[0])'
    python -u ${process_script_path}
    """,
}


def generate_slurm_spock(slurm_dict, python_env=None):
    """
    Slurm script for an ephys job on spock

    python_env: "conda" or "uv"; by default what spock_ephys_env() reads from dj.config
    """

    if python_env is None:
        python_env = spock_ephys_env()

    slurm_text = "#!/bin/bash\n"
    # slurm_text += module_defininition_text()
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
"""
    slurm_text += spock_ephys_env_setup[python_env]

    return slurm_text


# Conda environment on spock that suite2p imaging jobs run in. It needs suite2p==1.1.0, a torch
# build for the node GPUs (A100 / L40S) and element-calcium-imaging with suite2p_settings.run_suite2p.
# Kept apart from u19_pipeline_python_env3 (ephys jobs), whose suite2p 0.x / cellpose 3 would have
# to be upgraded for suite2p 1.1.0.
spock_imaging_conda_env = "u19_suite2p_env"


def generate_slurm_spock_imaging(slurm_dict, suite2p_job_file):
    """
    Slurm script for a suite2p job on a spock GPU node (u19_pipeline/automatic_job/suite2p_slurm_job.py).

    The job reads everything it needs from suite2p_job_file (written and transferred by the
    handler before sbatch), so it never needs the database.
    """

    # Imaging resources (1 GPU, cpus, memory) live in ft.slurm_dict_spock_imaging; only the
    # per-job fields set by generate_slurm_file are carried over from the incoming dict.
    imaging_dict = copy.deepcopy(ft.slurm_dict_spock_imaging)
    for key in ("job-name", "output", "error"):
        imaging_dict[key] = slurm_dict[key]

    slurm_text = "#!/bin/bash\n"
    slurm_text += create_slurm_params_file(imaging_dict)
    slurm_text += (
        """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "CUDA_VISIBLE_DEVICES: ${CUDA_VISIBLE_DEVICES}"
    nvidia-smi --query-gpu=name,driver_version,memory.used,memory.total --format=csv

    source ~/.bashrc
    module load anacondapy/2023.07-cuda -s

    conda activate """
        + spock_imaging_conda_env
        + """

    export suite2p_job_file="""
        + "'"
        + suite2p_job_file
        + "'"
        + """
    # CPU-side work (tiff reading, parts of detection) uses the cores slurm gave the job
    export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}
    export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}

    # Without the checkout the script path would resolve against the submit directory
    cd ${repository_dir} || { echo "repository_dir not found: ${repository_dir}" >&2; exit 1; }
    python -u ${process_script_path}
    """
    )

    return slurm_text


def module_defininition_text():

    return """
    module ()
    {
        local _mlredir=1;
        if [ -n "${MODULES_REDIRECT_OUTPUT+x}" ]; then
            if [ "$MODULES_REDIRECT_OUTPUT" = '0' ]; then
                _mlredir=0;
            else
                if [ "$MODULES_REDIRECT_OUTPUT" = '1' ]; then
                    _mlredir=1;
                fi;
            fi;
        fi;
        case " $@ " in
            *' --no-redirect '*)
                _mlredir=0
            ;;
            *' --redirect '*)
                _mlredir=1
            ;;
        esac;
        if [ $_mlredir -eq 0 ]; then
            _module_raw "$@";
        else
            _module_raw "$@" 2>&1;
        fi
    }
    """


def generate_slurm_spockmk2_ephys(slurm_dict):

    slurm_text = "#!/bin/bash\n"
    slurm_text += "source ~/.bashrc"
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2023.07-cuda -s
    module load matlab/R2024a -s

    conda activate BraincogsEphysSorters_Env

    cd ${repository_dir}
    python -u ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    """

    return slurm_text


def generate_slurm_tiger(slurm_dict):

    slurm_text = "#!/bin/bash\n"
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anaconda3/2022.5 -s
    module load matlab/R2024a -s

    conda activate BrainCogsEphysSorters_env

    cd ${repository_dir}
    python -u ${process_script_path}
    """

    return slurm_text


def generate_slurm_dlc(slurm_dict):

    slurm_text = "#!/bin/bash\n"
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    module load anacondapy/2021.11 -s
    conda activate /usr/people/alvaros/.conda/envs/u19_datajoint_py39_env

    python -u ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    """

    return slurm_text


def generate_slurm_dlc2(slurm_dict):

    slurm_text = "#!/bin/bash\n"
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += """
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    module load anaconda3/5.3.1 -s
    conda activate /home/alvaros/.conda/envs/u19_datajoint_py39_env

    python -u ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    """

    return slurm_text

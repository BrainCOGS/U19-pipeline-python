"""Run suite2p for one imaging job inside a slurm job on a spock GPU node.

Submitted by slurm_creator.generate_slurm_spock_imaging. Reads the job file the pipeline wrote
and transferred before sbatch (imaging_element_populate.build_suite2p_job), so it needs no
database; the pipeline ingests the output afterwards with ProcessingTask task_mode='load'.
If the output folder already holds a complete run with the job's settings, suite2p is not run
again: a rerun first deletes every plane's results, and one that does not finish leaves
nothing to load (job 1405).

Imports nothing from u19_pipeline on purpose: that would load the DataJoint configuration.
Any failure exits non-zero, so slurm reports the job FAILED and the pipeline marks it as an error.
"""

import json
import logging
import os
import pathlib
import socket
import time

from element_calcium_imaging.suite2p_settings import (
    build_suite2p_inputs,
    run_suite2p,
    verify_outputs,
)


def suite2p_inputs(job):
    """
    The (db, settings) run_suite2p builds for this job (same arguments and torch_device override)
    """
    image_files = [pathlib.Path(f) for f in job["image_files"]]
    if not image_files:
        raise FileNotFoundError("No input image files for suite2p processing")
    db, settings, _ = build_suite2p_inputs(
        job["params"],
        data_path=image_files[0].parent.as_posix(),
        file_list=[f.as_posix() for f in image_files],
        save_path0=pathlib.Path(job["output_dir"]).as_posix(),
        input_format=image_files[0].suffix.lstrip(".").lower(),
        scan_info=job["scan_info"],
    )
    settings["torch_device"] = job["torch_device"]
    return db, settings


def output_problem(job):
    """
    None if the output folder holds a complete suite2p run with this job's settings,
    otherwise what is missing or different
    """
    try:
        verify_outputs(*suite2p_inputs(job))
    except RuntimeError as e:
        return str(e)
    return None


def require_complete_output(job):
    """
    Raise RuntimeError, with what to do about it, unless the output folder holds a complete
    suite2p run with this job's settings
    """
    problem = output_problem(job)
    if problem is not None:
        # The pipeline keeps the last 255 characters of the message, so the advice goes last
        raise RuntimeError(
            f"{problem}\nsuite2p output in {job['output_dir']} cannot be loaded. A suite2p run "
            "over existing output deletes each plane's results first; if it does not finish only "
            "the old 'combined' folder is left. Requeue the job to run suite2p again."
        )


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
    )

    job_file = pathlib.Path(os.environ["suite2p_job_file"])
    job = json.loads(job_file.read_text())
    print("suite2p job file:", job_file)

    recording_process_id = os.environ.get("recording_process_id")
    if recording_process_id is not None and str(job["job_id"]) != recording_process_id:
        raise RuntimeError(
            f"{job_file} is for job {job['job_id']}, not {recording_process_id}"
        )

    image_files = job["image_files"]
    missing = [f for f in image_files if not pathlib.Path(f).exists()]
    if missing:
        raise FileNotFoundError(
            f"{len(missing)} of {len(image_files)} input files not found on "
            f"{socket.gethostname()}, e.g. {missing[0]}"
        )

    output_dir = pathlib.Path(job["output_dir"])
    if output_problem(job) is None:
        print(
            f"Complete suite2p output already in {output_dir}, not running suite2p again"
        )
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"suite2p on {job['torch_device']}: {len(image_files)} files -> {output_dir}")

    start = time.time()
    run_suite2p(
        job["params"],
        image_files=image_files,
        output_dir=output_dir,
        scan_info=job["scan_info"],
        torch_device=job["torch_device"],
    )
    print(f"suite2p finished in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()

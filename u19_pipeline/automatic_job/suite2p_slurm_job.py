"""Run suite2p for one imaging job inside a slurm job on a spock GPU node.

Submitted by slurm_creator.generate_slurm_spock_imaging. Reads the job file the pipeline wrote
and transferred before sbatch (imaging_element_populate.build_suite2p_job), so it needs no
database; the pipeline ingests the output afterwards with ProcessingTask task_mode='load'.

Imports nothing from u19_pipeline on purpose: that would load the DataJoint configuration.
Any failure exits non-zero, so slurm reports the job FAILED and the pipeline marks it as an error.
"""

import json
import logging
import os
import pathlib
import socket
import time

from element_calcium_imaging.suite2p_settings import run_suite2p


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")

    job_file = pathlib.Path(os.environ["suite2p_job_file"])
    job = json.loads(job_file.read_text())
    print("suite2p job file:", job_file)

    recording_process_id = os.environ.get("recording_process_id")
    if recording_process_id is not None and str(job["job_id"]) != recording_process_id:
        raise RuntimeError(f"{job_file} is for job {job['job_id']}, not {recording_process_id}")

    image_files = job["image_files"]
    missing = [f for f in image_files if not pathlib.Path(f).exists()]
    if missing:
        raise FileNotFoundError(f"{len(missing)} of {len(image_files)} input files not found on "
                                f"{socket.gethostname()}, e.g. {missing[0]}")

    output_dir = pathlib.Path(job["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"suite2p on {job['torch_device']}: {len(image_files)} files -> {output_dir}")

    start = time.time()
    run_suite2p(job["params"],
                image_files=image_files,
                output_dir=output_dir,
                scan_info=job["scan_info"],
                torch_device=job["torch_device"])
    print(f"suite2p finished in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()

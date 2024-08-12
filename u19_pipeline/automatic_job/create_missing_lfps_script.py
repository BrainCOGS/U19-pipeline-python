
import pathlib
import sys

import datajoint as dj

import u19_pipeline.automatic_job.params_config as config
from u19_pipeline import ephys_pipeline, recording, recording_process


def main(recording_id):
    rec_query = dict()
    rec_query['recording_id'] = recording_id

    recording_processes = (recording_process.Processing() & rec_query).fetch('job_id', 'recording_id', 'fragment_number', 'recording_process_pre_path', as_dict=True)
    #Create lfp trace if needed (neuropixel 2.0 probes)
    recording_directory = (recording.Recording & rec_query).fetch1('recording_directory')
    recording_directory = pathlib.Path(dj.config['custom']['ephys_root_data_dir'][0], recording_directory).parent.as_posix()
    for i in recording_processes:
        probe_dir = pathlib.Path(dj.config['custom']['ephys_root_data_dir'][0], i['recording_process_pre_path']).as_posix()
        ephys_pipeline.create_lfp_trace(config.catgt_script, recording_directory, probe_dir)


if __name__ == "__main__":


    from scripts.conf_file_finding import try_find_conf_file
    try_find_conf_file()
    args = sys.argv[1:]
    print(args)
    main(args[0])
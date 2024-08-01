import pathlib
import warnings

import u19_pipeline.automatic_job.params_config as config
from u19_pipeline import recording, recording_process
from u19_pipeline.imaging_pipeline import imaging_element


def populate_element_data(job_id, display_progress=True, reserve_jobs=False, suppress_errors=False):

    populate_settings = {'display_progress': display_progress, 
                         'reserve_jobs': reserve_jobs, 
                         'suppress_errors': suppress_errors}

    process_key = (recording_process.Processing * recording.Recording & 
                   dict(job_id=job_id)).fetch1('KEY')

    if (recording.Recording & process_key).fetch1('recording_modality') != 'imaging':
        warnings.warn(f'Recording modality is not `imaging` for job_id: {job_id}')
        return

    fragment_number, recording_process_pre_path, recording_process_post_path = \
                            (recording_process.Processing & process_key).fetch1(
                                            'fragment_number',
                                            'recording_process_pre_path',
                                            'recording_process_post_path')

    preprocess_param_steps_id, paramset_idx = \
                        (recording_process.Processing.ImagingParams & process_key
                        ).fetch1('preprocess_param_steps_id', 
                                    'paramset_idx')

    preprocess_paramsets = (imaging_element.PreprocessParamSteps.Step() & 
                            dict(
                                preprocess_param_steps_id=preprocess_param_steps_id)
                            ).fetch('paramset_idx')

    processing_method = (imaging_element.ProcessingParamSet & 
                            dict(paramset_idx=paramset_idx)).fetch1(
                                                            'processing_method')

    preprocess_task_mode = 'none' if len(preprocess_paramsets) == 0 else 'load'

    preprocess_key = dict(recording_id=process_key['recording_id'],
                            tiff_split=fragment_number,
                            scan_id=0,
                            preprocess_param_steps_id=preprocess_param_steps_id)

    imaging_element.PreprocessTask.insert1(
                                dict(**preprocess_key,
                                    preprocess_output_dir=recording_process_pre_path,
                                    task_mode=preprocess_task_mode),
                                    skip_duplicates=True)

    if not imaging_element.Preprocess & preprocess_key:
        imaging_element.Preprocess.populate(preprocess_key, **populate_settings)

    process_key = dict(**preprocess_key,
                        paramset_idx=paramset_idx)

    pathlib.Path(f'/mnt/cup/braininit/Data/Processed/imaging/{recording_process_post_path}/{processing_method}_output').mkdir(parents=True, exist_ok=True)

    imaging_element.ProcessingTask.insert1(
        dict(**process_key,
                processing_output_dir=f'{recording_process_post_path}/{processing_method}_output',
                task_mode='trigger'), skip_duplicates=True)

    if not imaging_element.Processing & process_key:
        imaging_element.Processing.populate(process_key, **populate_settings)

    if (imaging_element.Processing - imaging_element.Curation) & process_key:
        imaging_element.Curation().create1_from_processing_task(process_key)

    if not imaging_element.MotionCorrection & process_key:
        imaging_element.MotionCorrection.populate(process_key, **populate_settings)

    if not imaging_element.Segmentation & process_key:
        imaging_element.Segmentation.populate(process_key, **populate_settings)

    if not imaging_element.Fluorescence & process_key:
        imaging_element.Fluorescence.populate(process_key, **populate_settings)

    if not imaging_element.Activity & process_key:
        imaging_element.Activity.populate(process_key, **populate_settings)

    return config.status_update_idx['NEXT_STATUS']


if __name__ == '__main__':
    populate_element_data()
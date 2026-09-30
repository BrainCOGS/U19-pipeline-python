from u19_pipeline import recording, recording_process
from u19_pipeline.imaging_pipeline import imaging_element, scan_element, get_imaging_root_data_dir
from element_interface.utils import find_full_path
import pathlib
import warnings

import u19_pipeline.automatic_job.params_config as config


def imaging_runs_on_cluster():
    '''
    True when suite2p for imaging jobs runs in a slurm job (params_config.recording_modality_dict),
    so Processing only loads its output.
    '''
    local_or_cluster = config.recording_modality_df.loc[
        config.recording_modality_df['recording_modality'] == 'imaging', 'local_or_cluster']
    return local_or_cluster.iloc[0] == 'cluster'


def get_job_keys(job_id):
    '''
    Imaging element keys and paths for a recording process job
    '''

    process_key = (recording_process.Processing * recording.Recording &
                   dict(job_id=job_id)).fetch1('KEY')

    fragment_number, recording_process_pre_path, recording_process_post_path = \
                            (recording_process.Processing & process_key).fetch1(
                                            'fragment_number',
                                            'recording_process_pre_path',
                                            'recording_process_post_path')

    preprocess_param_steps_id, paramset_idx = \
                        (recording_process.Processing.ImagingParams & process_key
                        ).fetch1('preprocess_param_steps_id',
                                    'paramset_idx')

    processing_method = (imaging_element.ProcessingParamSet &
                            dict(paramset_idx=paramset_idx)).fetch1(
                                                            'processing_method')

    preprocess_key = dict(recording_id=process_key['recording_id'],
                            tiff_split=fragment_number,
                            scan_id=0,
                            preprocess_param_steps_id=preprocess_param_steps_id)

    return dict(preprocess_key=preprocess_key,
                paramset_idx=paramset_idx,
                processing_method=processing_method,
                recording_process_pre_path=recording_process_pre_path,
                processing_output_dir=f'{recording_process_post_path}/{processing_method}_output')


def build_suite2p_job(job_id, torch_device='cuda'):
    '''
    Everything suite2p_slurm_job.py needs to run suite2p for this job without the database:
    stored settings, input files, ScanInfo values and output folder. Paths are absolute, as
    mounted on both the pipeline host and spock (/mnt/cup/...).
    '''

    keys = get_job_keys(job_id)

    if keys['processing_method'] != 'suite2p':
        raise NotImplementedError(f"Cluster imaging processing only runs suite2p, not {keys['processing_method']}")

    preprocess_paramsets = (imaging_element.PreprocessParamSteps.Step() &
                            dict(preprocess_param_steps_id=keys['preprocess_key']['preprocess_param_steps_id'])
                            ).fetch('paramset_idx')
    if len(preprocess_paramsets) > 0:
        # Preprocess.make implements neither load nor trigger, so there would be no inputs for suite2p
        raise NotImplementedError('Cluster imaging processing does not support preprocessing steps')

    scan_key = {k: keys['preprocess_key'][k] for k in ('recording_id', 'tiff_split', 'scan_id')}
    fps, ndepths, nchannels = (scan_element.ScanInfo & scan_key).fetch1('fps', 'ndepths', 'nchannels')
    image_files = [find_full_path(get_imaging_root_data_dir(), f).as_posix()
                   for f in (scan_element.ScanInfo.ScanFile & scan_key).fetch('file_path', order_by='file_path')]

    params = (imaging_element.ProcessingParamSet & dict(paramset_idx=keys['paramset_idx'])).fetch1('params')

    return dict(job_id=int(job_id),
                params=params,
                image_files=image_files,
                scan_info=dict(fs=float(fps), nplanes=int(ndepths), nchannels=int(nchannels)),
                output_dir=pathlib.Path(config.imaging_processed_root, keys['processing_output_dir']).as_posix(),
                torch_device=torch_device)


def populate_element_data(job_id, display_progress=True, reserve_jobs=False, suppress_errors=False):

    populate_settings = {'display_progress': display_progress,
                         'reserve_jobs': reserve_jobs,
                         'suppress_errors': suppress_errors}

    process_key = (recording_process.Processing * recording.Recording &
                   dict(job_id=job_id)).fetch1('KEY')

    if (recording.Recording & process_key).fetch1('recording_modality') != 'imaging':
        warnings.warn(f'Recording modality is not `imaging` for job_id: {job_id}')
        return

    keys = get_job_keys(job_id)
    preprocess_key = keys['preprocess_key']

    preprocess_paramsets = (imaging_element.PreprocessParamSteps.Step() &
                            dict(
                                preprocess_param_steps_id=preprocess_key['preprocess_param_steps_id'])
                            ).fetch('paramset_idx')

    if len(preprocess_paramsets)==0:
        preprocess_task_mode = 'none'
    else:
        preprocess_task_mode = 'load'

    imaging_element.PreprocessTask.insert1(
                                dict(**preprocess_key,
                                    preprocess_output_dir=keys['recording_process_pre_path'],
                                    task_mode=preprocess_task_mode),
                                    skip_duplicates=True)

    if not imaging_element.Preprocess & preprocess_key:
        imaging_element.Preprocess.populate(preprocess_key, **populate_settings)

    process_key = dict(**preprocess_key,
                        paramset_idx=keys['paramset_idx'])

    pathlib.Path(config.imaging_processed_root, keys['processing_output_dir']).mkdir(parents=True, exist_ok=True)

    # On the cluster suite2p already ran in the slurm job (suite2p_slurm_job.py): only load its output
    processing_task_mode = 'load' if imaging_runs_on_cluster() else 'trigger'

    imaging_element.ProcessingTask.insert1(
        dict(**process_key,
                processing_output_dir=keys['processing_output_dir'],
                task_mode=processing_task_mode), skip_duplicates=True)

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

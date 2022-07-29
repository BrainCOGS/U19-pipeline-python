from u19_pipeline.ephys_pipeline import probe_element, ephys_element
from u19_pipeline import recording, recording_process

def populate_element_data(job_id, display_progress=True, reserve_jobs=False, suppress_errors=False):

    populate_settings = {'display_progress': display_progress, 
                         'reserve_jobs': reserve_jobs, 
                         'suppress_errors': suppress_errors}

    process_key = (recording_process.Processing * recording.Recording & 
                            dict(recording_modality='electrophysiology',
                                 job_id=job_id)).fetch1('KEY')

    fragment_number, recording_process_pre_path, recording_process_post_path = \
                            (recording_process.Processing & process_key).fetch1(
                                            'fragment_number',
                                            'recording_process_pre_path',
                                            'recording_process_post_path')

    precluster_param_steps_id, paramset_idx = \
                        (recording_process.Processing.EphysParams & process_key
                        ).fetch1('precluster_param_steps_id', 
                                    'paramset_idx')

    precluster_paramsets = (ephys_element.PreClusterParamSteps.Step() & 
                            dict(
                                precluster_param_steps_id=precluster_param_steps_id)
                            ).fetch('paramset_idx')

    clustering_method = (ephys_element.ClusteringParamSet & 
                            dict(paramset_idx=paramset_idx)).fetch1(
                                                            'clustering_method')

    if len(precluster_paramsets)==0:
        task_mode = 'none'
    else:
        task_mode = 'load'

    precluster_key = dict(recording_id=process_key['recording_id'],
                            insertion_number=fragment_number,
                            precluster_param_steps_id=precluster_param_steps_id)

    ephys_element.PreClusterTask.insert1(
                                dict(**precluster_key,
                                    precluster_output_dir=recording_process_pre_path,
                                    task_mode=task_mode),
                                    skip_duplicates=True)

    ephys_element.PreCluster.populate(precluster_key, **populate_settings)

    if '1.0' in (ephys_element.ProbeInsertion * probe_element.Probe & 
                    precluster_key).fetch1('probe_type'):
        ephys_element.LFP.populate(precluster_key, **populate_settings)

    cluster_key = dict(**precluster_key,
                        paramset_idx=paramset_idx)

    ephys_element.ClusteringTask.insert1(
        dict(**cluster_key,
                clustering_output_dir=f'{recording_process_post_path}/{clustering_method}_output',
                task_mode='load'), skip_duplicates=True)

    ephys_element.Clustering.populate(cluster_key, **populate_settings)

    if (ephys_element.Clustering - ephys_element.Curation) & cluster_key:
        ephys_element.Curation().create1_from_clustering_task(cluster_key)

    ephys_element.CuratedClustering.populate(cluster_key, **populate_settings)

    #ephys_element.WaveformSet.populate(cluster_key, **populate_settings)


if __name__ == '__main__':
    populate_element_data()

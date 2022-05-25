from u19_pipeline.ephys_pipeline import ephys_element
from u19_pipeline import recording, recording_process

def run(display_progress=True, reserve_jobs=False, suppress_errors=False):

    populate_settings = {'display_progress': display_progress, 
                         'reserve_jobs': reserve_jobs, 
                         'suppress_errors': suppress_errors}

    ephys_element.EphysRecording.populate(**populate_settings)

    for key in (recording_process.Processing * recording.Recording & 
                            dict(recording_modality='electrophysiology', 
                                 status_processing_id=7)).fetch('KEY'):

        recording_id, fragment_number, recording_process_pre_path = \
                                (recording_process.Processing & key).fetch1(
                                                'recording_id', 
                                                'fragment_number',
                                                'recording_process_pre_path')
    
        precluster_param_list_id = (recording_process.Processing.EphysParams & 
                                                key).fetch1('precluster_param_list_id')
        
        ephys_element.PreClusterTask.insert1(
                                dict(recording_id=recording_id,
                                     insertion_number=fragment_number,
                                     precluster_param_list_id=precluster_param_list_id,
                                     precluster_output_dir=recording_process_pre_path,
                                     task_mode='load'))

    ephys_element.PreCluster.populate(**populate_settings)

    ephys_element.LFP.populate(**populate_settings)

    for key in (recording_process.Processing * recording.Recording & 
                            dict(recording_modality='electrophysiology', 
                                 status_processing_id=7)).fetch('KEY'):

        recording_id, fragment_number, recording_process_post_path = \
                                (recording_process.Processing & key).fetch1(
                                                'recording_id', 
                                                'fragment_number', 
                                                'recording_process_post_path')

        precluster_param_list_id, cluster_paramset_idx = \
                            (recording_process.Processing.EphysParams & key).fetch1(
                                                        'precluster_param_list_id', 
                                                        'cluster_paramset_idx')

        ephys_element.ClusteringTask.insert1(
            dict(recording_id=recording_id, 
                 insertion_number=fragment_number, 
                 precluster_param_list_id=precluster_param_list_id,
                 paramset_idx=cluster_paramset_idx,
                 clustering_output_dir=recording_process_post_path+'/kilosort_output',
                 task_mode='load'))

    ephys_element.Clustering.populate(**populate_settings)

    for key in (ephys_element.Clustering - ephys_element.Curation).fetch('KEY'):
        ephys_element.Curation().create1_from_clustering_task(key)

    ephys_element.CuratedClustering.populate(**populate_settings)

    ephys_element.WaveformSet.populate(**populate_settings)


if __name__ == '__main__':
    run()

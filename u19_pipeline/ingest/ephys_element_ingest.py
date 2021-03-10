import re
import pathlib

from u19_pipeline import ephys
from u19_pipeline.ephys_element import (probe_element, ephys_element,
                                        get_ephys_root_data_dir, get_session_directory)
from elements_ephys.readers import spikeglx

"""
The ingestion routine for imaging element includes:

Manual insertion:
1. ephys_element.ProbeInsertion
    + this can stem from Session, check ephys data and create probe insertion(s) accordingly

2. ephys_element.ClusteringTask
    + this requires users to add new ClusteringParamSet (use ClusteringParamSet.insert_new_params method)
    (for an example, see: https://github.com/ttngu207/workflow-ephys/blob/main/notebooks/run_workflow.ipynb)
    + manually insert new ClusteringTask for each ProbeInsertion
    (for an example, see: https://github.com/ttngu207/workflow-ephys/blob/main/notebooks/run_workflow.ipynb)
"""

acq_software = 'SpikeGLX'


def process_session(sess_key):
    """
    For each entry in `acquisition.Session` table, search for scan data and create corresponding ProbeInsertion entries
    Note: the following step should be to call populate on `ephys_element.EphysRecording`
        before the `ClusteringTask` can be created

    :param scan_key: a `KEY` of `acquisition.Session`
    """
    sess_dir = pathlib.Path(get_session_directory(sess_key))
    ephys_meta_filepaths = [fp for fp in sess_dir.rglob('*.ap.meta')]

    if not len(ephys_meta_filepaths):
        print(f'No SpikeGLX data found for session:{sess_key} - at {sess_dir}')
        return

    probe_list, probe_insertion_list = [], []
    for meta_filepath in ephys_meta_filepaths:
        spikeglx_meta = spikeglx.SpikeGLXMeta(meta_filepath)

        probe_key = {'probe_type': spikeglx_meta.probe_model, 'probe': spikeglx_meta.probe_SN}
        if probe_key['probe'] not in [p['probe'] for p in probe_list] and probe_key not in probe_element.Probe():
            probe_list.append(probe_key)

        probe_dir = meta_filepath.parent
        probe_number = re.search('(imec)?\d{1}$', probe_dir.name).group()
        probe_number = int(probe_number.replace('imec', ''))

        probe_insertion_list.append({**sess_key, 'probe': spikeglx_meta.probe_SN, 'insertion_number': int(probe_number)})

    probe_element.Probe.insert(probe_list, skip_duplicates=True)
    ephys_element.ProbeInsertion.insert(probe_insertion_list, skip_duplicates=True)


if __name__ == '__main__':
    for sess_key in ephys.EphysSession.fetch('KEY'):
        process_session(sess_key)

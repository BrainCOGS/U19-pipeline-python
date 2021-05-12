# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: 'Python 3.7.0 64-bit (''u19'': conda)'
#     language: python
#     name: python37064bitu19conda866a936538d140d38655f260f3a92360
# ---

# # Run ephys element workflow

# This notebook walks you through the steps to run the ephys workflow.  
# The workflow requires neuropixels meta file and kilosort output data.  
# A typical organization of the files is as follows:  
#
# ```
# root_data_dir/
# └───subject1/
# │   └───session0/
# │   │   └───imec0/
# │   │   │   │   *imec0.ap.meta
# │   │   │   └───ksdir/
# │   │   │       │   spike_times.npy
# │   │   │       │   templates.npy
# │   │   │       │   ...
# │   │   └───imec1/
# │   │       │   *imec1.ap.meta   
# │   │       └───ksdir/
# │   │           │   spike_times.npy
# │   │           │   templates.npy
# │   │           │   ...
# │   └───session1/
# │   │   │   ...
# └───subject2/
# │   │   ...
# ```
#
#
#
# Let's start by importing the relevant modules.

from u19_pipeline import acquisition, ephys
from u19_pipeline.ephys_element import ephys_element, probe_element
import datajoint as dj

# The module ephys_element contains all the tables designed in the DataJoint ephys element, we could draw the diagram to see the schema structures

dj.Diagram(acquisition.Session) + ephys.EphysSession + dj.Diagram(ephys_element)

# ## Ingest Probe and ProbeInsertion by ephys_element_ingest

# The original U19 pipeline contains a table `ephys.EphysSession` where datapath to the neuropixel meta file and the kilosort output folder were stored.

ephys.EphysSession()

# A module `ephys_element_ingest` was provided to process a ephys session based on the neuropixel meta file: ingest entries into tables `Probe` and `ProbeInsertion`

from u19_pipeline.ingest import ephys_element_ingest

for sess_key in ephys.EphysSession.fetch('KEY'):
    ephys_element_ingest.process_session(sess_key)

probe_element.Probe()

ephys_element.ProbeInsertion()

# ## Populate EphysRecording

# The `probe_element.EelectrodeConfig` table contains the configuration information of the electrodes used, i.e. which 384 electrodes out of the total 960 on the probe were used in this ephys session, while the table `ephys_element.EphysRecording` specify which ElectrodeConfig is used in a particular ephys session. 

ephys_element.EphysRecording()

ephys_element.EphysRecording.populate(display_progress=True)

# Here is an overview of the Electrode used in a EphysRecording for a particular probe insertion

probe_insertion_key = ephys_element.ProbeInsertion.fetch('KEY', limit=1)[0]
ephys_element.EphysRecording * probe_element.ElectrodeConfig.Electrode & probe_insertion_key

# ## Populate clustering results

# The next major table in the ephys pipeline is the `ClusteringTask`, which is a manual table that is inserted when a Kilosort2 clustering task is finished and the clustering results are ready for processing. The `ClusteringTask` table depends on the table `ClusteringParamSet`, which are the parameters of the clustering task and needed to be inserted first. A method of the class `ClusteringParamSet` called `insert_new_params` helps on the insertion of params_set

# insert clustering task manually
params_ks = {
    "fs": 30000,
    "fshigh": 150,
    "minfr_goodchannels": 0.1,
    "Th": [10, 4],
    "lam": 10,
    "AUCsplit": 0.9,
    "minFR": 0.02,
    "momentum": [20, 400],
    "sigmaMask": 30,
    "ThPr": 8,
    "spkTh": -6,
    "reorder": 1,
    "nskip": 25,
    "GPU": 1,
    "Nfilt": 1024,
    "nfilt_factor": 4,
    "ntbuff": 64,
    "whiteningRange": 32,
    "nSkipCov": 25,
    "scaleproc": 200,
    "nPCs": 3,
    "useRAM": 0
}
ephys_element.ClusteringParamSet.insert_new_params(
    'kilosort2', 0, 'Spike sorting using Kilosort2', params_ks)
ephys_element.ClusteringParamSet()

# We are then able to insert an entry into the `ClusteringTask` table. One important field of the table is `clustering_output_dir`, which specifies the Kilosort2 output directory for the later processing. For the current pipeline, the directory could be reconstructed from directories stored in existing tables

ephys_element.ClusteringTask()

ephys_key = ephys_element.EphysRecording.fetch1('KEY')
ephys_dir = ephys.EphysSession.fetch1('ephys_directory') + '/towersTask_g0_imec0'

ephys_element.ClusteringTask.insert1(
    dict(**ephys_key, paramset_idx=0, clustering_output_dir=ephys_dir), skip_duplicates=True)
ephys_element.ClusteringTask()

# We are then able to populate the clustering results. The `Clustering` table now validates the Kilosort2 outcomes. In the future release of elements-ephys, this table will be used to trigger Kilosort2.

ephys_element.Clustering.populate(display_progress=True)

# The next step in the pipeline is the curation of spike sorting results. If a manual curation was implemented, an entry needs to be manually inserted into the table `Curation`, which specifies the directory to the curated results in `curation_output_dir`. If we would like to process the Kilosort2 outcome directly, an entry is also needed in `Curation`. A method `create1_from_clustering_task` was provided to help this insertion. It copies the `clustering_output_dir` in `ClusteringTask` to the field `curation_output_dir` in the table `Curation` with a new `curation_id`.

key = ephys_element.ClusteringTask.fetch1('KEY')
ephys_element.Curation().create1_from_clustering_task(key)

ephys_element.Curation()

# Then we could populate table `CuratedClustering`, ingesting either the output of Kilosort2 or the curated results.

ephys_element.CuratedClustering.populate(display_progress=True)

# The part table `CuratedClustering.Unit` contains the spike sorted units

ephys_element.CuratedClustering.Unit()

# ## Populate LFP and waveform

ephys_element.LFP.populate(display_progress=True)

ephys_element.LFP()

# The current workflow also contain tables to save spike waveforms:
#
# `WaveformSet`: a table to drive the processing of all spikes waveforms resulting from a CuratedClustering.  
# `WaveformSet.Waveform`: mean waveform across spikes for a given unit and electrode.  
# `WaveformSet.PeakWaveform`: mean waveform across spikes for a given unit at the electrode with peak spike amplitude.

# May take a while to populate depending on data size.
ephys_element.WaveformSet.populate(display_progress=True)

# TODO: 
#     - Sync table under ephys module.
#     - Unit table could contain the clustering results for multiple sorters. U19 team needs to implemnt the workflow for other sorters by overwriting the make function of the table `ephys_element.Clustering`. e.g.
#     ```
#     def ironclust_make(key):
#     if (ephys_element.ClusteringMethod & key).fetch1('clustering_method') != 'ironclust':
#         return
#     pass
#     ephys_element.CuratedClustering.make = ironclust_make
#     ephys_element.CuratedClustering.populate()
#     ```

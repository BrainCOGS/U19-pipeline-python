# from scripts.conf_file_finding import try_find_conf_file
# try_find_conf_file()

import os 
import datajoint as dj
import numpy as np
from u19_pipeline import imaging, acquisition, subject
from u19_pipeline.imaging_element import imaging_element, scan_element, get_suite2p_dir
from u19_pipeline.ingest.imaging_element_ingest import process_scan
from u19_pipeline.imaging_element import (scan_element, imaging_element, Equipment,
                                          get_imaging_root_data_dir, get_scan_image_files)
from element_calcium_imaging.readers import get_scanimage_acq_time, parse_scanimage_header
import scanreader
import pathlib

subject = 'emdia_gps24'
date = '2022-01-11'
acq_software = 'mesoscope'
# subject = 'emdia_teto6s_12'
# date = '2021-11-14'
# acq_software = 'mesoscope'
# ingest parameters for Suite2p
pars = {
    'look_one_level_down': 0.0,
    'fast_disk': [],
    'delete_bin': False,
    'mesoscan': False,
    'h5py': [],
    'h5py_key': 'data',
    'save_path0': [],
    'subfolders': [],
    'nplanes': 1,
    'nchannels': 1,
    'functional_chan': 1,
    'tau': 1.0,
    'fs': 10.0,
    'force_sktiff': False,
    'preclassify': 0.0,
    'save_mat': False,
    'combined': True,
    'aspect': 1.0,
    'do_bidiphase': False,
    'bidiphase': 0.0,
    'do_registration': True,
    'keep_movie_raw': False,
    'nimg_init': 300,
    'batch_size': 500,
    'maxregshift': 0.1,
    'align_by_chan': 1,
    'reg_tif': False,
    'reg_tif_chan2': False,
    'subpixel': 10,
    'smooth_sigma': 1.15,
    'th_badframes': 1.0,
    'pad_fft': False,
    'nonrigid': True,
    'block_size': [128, 128],
    'snr_thresh': 1.2,
    'maxregshiftNR': 5.0,
    '1Preg': False,
    'spatial_hp': 50.0,
    'pre_smooth': 2.0,
    'spatial_taper': 50.0,
    'roidetect': True,
    'sparse_mode': False,
    'diameter': 12,
    'spatial_scale': 0,
    'connected': True,
    'nbinned': 5000,
    'max_iterations': 20,
    'threshold_scaling': 1.0,
    'max_overlap': 0.75,
    'high_pass': 100.0,
    'inner_neuropil_radius': 2,
    'min_neuropil_pixels': 350,
    'allow_overlap': False,
    'chan2_thres': 0.65,
    'baseline': 'maximin',
    'win_baseline': 60.0,
    'sig_baseline': 10.0,
    'prctile_baseline': 8.0,
    'neucoeff': 0.7,
    'xrange': np.array([0, 0]),
    'yrange': np.array([0, 0])}

key = (imaging.Scan & dict(session_date =date, subject_fullname=subject)).fetch1('KEY')

for scan_key in (imaging.Scan & key).fetch('KEY'):

    for fov_key in (imaging.FieldOfView & scan_key).fetch('KEY'):

        scan_filepaths = get_scan_image_files(fov_key)

        # for scan_folder in scan_filepaths:
        try:  # attempt to read .tif as a scanimage file
            #TODO: Can use tiffile function to load
            # scan_folder = scan_folder.rsplit('/', maxsplit=1)[0]
            # if os.path.exists(str(scan_folder) + '/' +'suite2p'):
            # else:
            loaded_scan = scanreader.read_scan(scan_filepaths) #TODO: Try except scanreader and tiffile
            header = parse_scanimage_header(loaded_scan)
            scanner = header['SI_imagingSystem'].strip('\'') #TODO: If using tiffile, hardcode it to `mesoscope`
        except Exception as e:
            print(f'ScanImage loading error: {scan_filepaths}\n{str(e)}')
        scan_key = {**scan_key, 'scan_id': fov_key['fov']}
        if scan_key not in scan_element.Scan():
            Equipment.insert1({'scanner': scanner}, skip_duplicates=True)
            scan_element.Scan.insert1(
                {**scan_key, 'scanner': scanner, 'acq_software': acq_software})
            scan_element.ScanInfo.populate(key, display_progress=True)

    imaging_element.ProcessingParamSet.insert_new_params(
        'suite2p', 0, 'Calcium imaging analysis with Suite2p using default Suite2p parameters', pars)

    #TODO: Test if another for loop is even required - logically shouldn't be needed
    scan_keys = (scan_element.Scan & key).fetch('KEY')
    scan_folder = scan_filepaths.rsplit('/', maxsplit=1)[0]
    if os.path.exists(str(scan_folder) + '/' +'suite2p'): #TODO: Check pathlib.Path to be more consistent 
        for scan_key in scan_keys:
            output_dir = get_suite2p_dir(scan_key)
        #TODO: Check if the `suite2p` folder already exists in the output_dir, if yes load, otherwise trigger
        #  https://github.com/datajoint/element-interface/blob/5860916aa560749c9899e8cef83addacf160f0e3/element_interface/suite2p_loader.py#L79
            ops_filepaths = list(output_dir.rglob('*ops.npy'))
            fpath = pathlib.Path(output_dir) #TODO: Check pathlib.Path to be more consistent 
            ops_fp = fpath / 'ops.npy'
            if not ops_fp.exists():
                raise FileNotFoundError(
                    'No "ops.npy" found. Invalid suite2p plane folder: {}'.format(self.fpath))
            self.creation_time = datetime.fromtimestamp(ops_fp.stat().st_ctime)

            iscell_fp = self.fpath / 'iscell.npy'
            if not iscell_fp.exists():
                raise FileNotFoundError(
                    'No "iscell.npy" found. Invalid suite2p plane folder: {}'.format(self.fpath))

        imaging_element.ProcessingTask.insert1(dict(**scan_key, paramset_idx=0, processing_output_dir=output_dir, task_mode='load'), skip_duplicates=True)

    imaging_element.Processing.populate(key, display_progress=True)

    processing_keys = imaging_element.Processing.fetch('KEY')
    for processing_key in processing_keys:
        imaging_element.Curation().create1_from_processing_task(processing_key)

    imaging_element.MotionCorrection.populate(key, display_progress=True)
    imaging_element.Segmentation.populate(key, display_progress=True)

    imaging_element.Fluorescence.populate(key, display_progress=True)
    # This table computes the activity such as df/f or deconvoluted inferred spikes
    imaging_element.Activity.populate(key, display_progress=True)

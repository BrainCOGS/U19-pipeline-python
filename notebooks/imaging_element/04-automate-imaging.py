import os 
import datajoint as dj
import numpy as np
from u19_pipeline import imaging, acquisition, subject
from u19_pipeline.imaging_element import imaging_element, scan_element, \
                                        get_suite2p_dir, Equipment,\
                                          get_imaging_root_data_dir, \
                                        get_scan_image_files
from u19_pipeline.ingest.imaging_element_ingest import process_scan
from element_interface.scanimage_utils import get_scanimage_acq_time, parse_scanimage_header
import scanreader
import pathlib
import tifffile
import datetime

subject = 'koay_K65'
date = '2018-02-02'
acq_software = 'ScanImage'

parameters = {
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

key = (imaging.Scan & dict(session_date=date, subject_fullname=subject)).fetch1('KEY')

for scan_key in (imaging.Scan & key).fetch('KEY'):
    for fov_key in (imaging.FieldOfView & scan_key).fetch('KEY'):

        scan_filepaths = get_scan_image_files(fov_key)
        try: 
            #TODO: Can use tiffile function to loads
            print('LOADED Scan using Scanreader')
            loaded_scan = scanreader.read_scan(scan_filepaths) #TODO: Try except scanreader and tiffile
            header = parse_scanimage_header(loaded_scan)
            scanner = header['SI_imagingSystem'].strip('\'') #TODO: If using tiffile, hardcode it to `mesoscope`
        except Exception as e:
            print('LOADED Scan using Tifffile')
            # scan_filepaths = scan_filepaths[:25] # TODO load all TIFF files from session possibly using TIFFSequence
            loaded_scan = tifffile.imread(scan_filepaths)
            scanner = 'mesoscope'
        else: #TODO: Use except instead of except)
            print(f'ScanImage loading error')  #TODO: Modify the error message

        scan_key = {**scan_key, 'scan_id': fov_key['fov']}
        if scan_key not in scan_element.Scan():
            Equipment.insert1({'scanner': scanner}, skip_duplicates=True)
            scan_element.Scan.insert1(
                {**scan_key, 'scanner': scanner, 'acq_software': acq_software})
            scan_element.ScanInfo.populate(scan_key, display_progress=True)

        imaging_element.ProcessingParamSet.insert_new_params(
        'suite2p', 0, 'Calcium imaging analysis with Suite2p using default Suite2p parameters', parameters) #TODO: element-calcium-imaging (scan and imaging module)

        #TODO: Test if another for loop is even required - logically shouldn't be needed
        output_dir = [x.rsplit('/', maxsplit=1)[0] for x in scan_filepaths]
        output_dir = [pathlib.Path(x) for x in output_dir]
        scan_folder = [x / 'suite2p' for x in output_dir]
        if (scan_folder[0]).exists(): 
            output_dir = get_suite2p_dir(scan_key)
            print(output_dir)
            p = pathlib.Path(output_dir).glob('**/*')
            plane_filepaths = [x for x in p if x.is_dir()]
            for plane_filepath in plane_filepaths:
                ops_fp = plane_filepath / 'ops.npy'
                if not ops_fp.exists():
                    raise FileNotFoundError(
                        'No "ops.npy" found. Invalid suite2p plane folder: {}'.format(ops_fp))
                iscell_fp = plane_filepath / 'iscell.npy'
                if not iscell_fp.exists():
                    raise FileNotFoundError(
                        'No "iscell.npy" found. Invalid suite2p plane folder: {}'.format(iscell_fp))
                imaging_element.ProcessingTask.insert1(dict(**scan_key, paramset_idx=0, processing_output_dir=plane_filepath, task_mode='load'), skip_duplicates=True)
        else:
            print(output_dir)
            imaging_element.ProcessingTask.insert1(dict(**scan_key, paramset_idx=0, processing_output_dir=output_dir[0], task_mode='trigger'), skip_duplicates=True)
            imaging_element.Processing.populate(scan_key, display_progress=True)

            processing_keys = imaging_element.Processing.fetch('KEY')
            for processing_key in processing_keys:
                imaging_element.Curation().create1_from_processing_task(processing_key)

            imaging_element.MotionCorrection.populate(scan_key, display_progress=True)
            imaging_element.Segmentation.populate(scan_key, display_progress=True)

            imaging_element.Fluorescence.populate(scan_key, display_progress=True)
            # This table computes the activity such as df/f or deconvoluted inferred spikes
            imaging_element.Activity.populate(scan_key, display_progress=True)

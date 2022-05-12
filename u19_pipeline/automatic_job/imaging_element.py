from concurrent.futures import process
import os 
import datajoint as dj
import numpy as np
from u19_pipeline import lab, imaging_rec, acquisition, subject, recording
from u19_pipeline.imaging_element import imaging_element, scan_element, \
                                        get_processed_dir, Equipment,\
                                          get_imaging_root_data_dir, \
                                        get_scan_image_files
from u19_pipeline.ingest.imaging_element_ingest import process_scan
from element_interface.scanimage_utils import get_scanimage_acq_time, parse_scanimage_header
from element_interface.utils import find_full_path
import scanreader
import pathlib
import tifffile
import datetime
import h5py

# subject_fullname = 'koay_K65'
# session_date = '2018-02-02'
# session_number = 0
# subject_fullname = 'emdiamanti_gps8'
# session_date = '2021-02-27'
# session_number = 0

acq_software = 'ScanImage'
#recording_id = os.environ['recording_id']
recording_process_id = os.environ['recording_process_id']
#process_method = os.environ['process_method']
#paramset_idx = os.environ['paramset_idx']

# parameters = {
#     'look_one_level_down': 0.0,
#     'fast_disk': [],
#     'delete_bin': False,
#     'mesoscan': False,
#     'h5py': [],
#     'h5py_key': 'data',
#     'save_path0': [],
#     'subfolders': [],
#     'nplanes': 1,
#     'nchannels': 1,
#     'functional_chan': 1,
#     'tau': 1.0,
#     'fs': 10.0,
#     'force_sktiff': False,
#     'preclassify': 0.0,
#     'save_mat': False,
#     'combined': True,
#     'aspect': 1.0,
#     'do_bidiphase': False,
#     'bidiphase': 0.0,
#     'do_registration': True,
#     'keep_movie_raw': False,
#     'nimg_init': 300,
#     'batch_size': 500,
#     'maxregshift': 0.1,
#     'align_by_chan': 1,
#     'reg_tif': False,
#     'reg_tif_chan2': False,
#     'subpixel': 10,
#     'smooth_sigma': 1.15,
#     'th_badframes': 1.0,
#     'pad_fft': False,
#     'nonrigid': True,
#     'block_size': [128, 128],
#     'snr_thresh': 1.2,
#     'maxregshiftNR': 5.0,
#     '1Preg': False,
#     'spatial_hp': 50.0,
#     'pre_smooth': 2.0,
#     'spatial_taper': 50.0,
#     'roidetect': True,
#     'sparse_mode': False,
#     'diameter': 12,
#     'spatial_scale': 0,
#     'connected': True,
#     'nbinned': 5000,
#     'max_iterations': 20,
#     'threshold_scaling': 1.0,
#     'max_overlap': 0.75,
#     'high_pass': 100.0,
#     'inner_neuropil_radius': 2,
#     'min_neuropil_pixels': 350,
#     'allow_overlap': False,
#     'chan2_thres': 0.65,
#     'baseline': 'maximin',
#     'win_baseline': 60.0,
#     'sig_baseline': 10.0,
#     'prctile_baseline': 8.0,
#     'neucoeff': 0.7,
#     'xrange': np.array([0, 0]),
#     'yrange': np.array([0, 0])}



# parameters_caiman = {'fr': 30,                             
#                     'decay_time': 0.4,
#                     'strides': (48, 48),          
#                     'overlaps': (24, 24),       
#                     'max_shifts': (6,6),         
#                     'max_deviation_rigid': 3,     
#                     'pw_rigid': True,             
#                     'p': 1,                       
#                     'gnb': 2,                    
#                     'merge_thr': 0.85,            
#                     'rf': 15,                     
#                     'stride_cnmf': 6,             
#                     'K': 4,                     
#                     'gSig': [4, 4],               
#                     'method_init': 'greedy_roi',  
#                     'ssub': 1,                    
#                     'tsub': 1,                    
#                     'min_SNR': 2.0,               
#                     'rval_thr': 0.85,              
#                     'cnn_thr': 0.99,             
#                     'cnn_lowest': 0.1}

# imaging_element.ProcessingParamSet.insert_new_params('suite2p', 0, 'Calcium imaging analysis with Suite2p using default Suite2p parameters', parameters) 
# imaging_element.ProcessingParamSet.insert_new_params('caiman', 1, 'Calcium imaging analysis with CaImAn using default CaImAn parameters', parameters_caiman) 

#scan_key = (imaging_rec.Scan & dict(recording_id=recording_id)).fetch1('KEY')

#Recording process key
rec_process_key = dict(recording_process_id=recording_process_id)
rec_process_str_key = 'recording_process_id_'+str(recording_process_id)

#Get fov key
rec_process = (imaging_rec.ImagingProcessing & rec_process_key).fetch1()
fov_key = rec_process.copy()
fov_key.pop('recording_process_id')

#Recording process extra info
recording_process_info = (lab.Location * recording.RecordingProcess * recording.Recording & rec_process_key).fetch(
    'acquisition_type', 'preprocess_paramset_idx', 'process_paramset_idx', as_dict=True)


#Paramset idx and key
paramset_idx = recording_process_info[0]['process_paramset_idx']
paramset_idx_key = dict()
paramset_idx_key['paramset_idx'] = paramset_idx
scanner = recording_process_info[0]['acquisition_type']


#Scan id always 0 because we will control that on recording_process 
scan_id = 0

#Get preprocess params
preprocess_params_key = dict()
preprocess_params_key['preprocess_paramset_idx'] = recording_process_info[0]['preprocess_paramset_idx']
preprocess_params = recording.PreprocessParamSet().get_preprocess_params(preprocess_params_key)
processing_method = preprocess_params['processing_method']
task_mode      = preprocess_params['task_mode']

print('got processing_method', processing_method)
print('got task_mode', task_mode)

print('got paramset_idx', paramset_idx)


#Get directories for kov
scan_filepaths = get_scan_image_files(fov_key)
scan_filepaths = scan_filepaths[:1]
print(scan_filepaths)

if rec_process_key not in scan_element.Scan():
    try: 
        #TODO: Can use tiffile function to loads
        print('LOADED Scan using Scanreader')
        loaded_scan = scanreader.read_scan(scan_filepaths)
        header = parse_scanimage_header(loaded_scan)
        #scanner = header['SI_imagingSystem'].strip('\'') #TODO: If using tiffile, hardcode it to `mesoscope`
    except Exception as e:
        print('LOADED Scan using Tifffile')
        scan_filepaths = scan_filepaths # TODO load all TIFF files from session possibly using TIFFSequence
        loaded_scan = tifffile.imread(scan_filepaths)
        #scanner = 'mesoscope'
    except: #TODO: Use except instead of else)
        print(f'ScanImage loading error')  #TODO: Modify the error message

    #Equipment.insert1({'scanner': scanner}, skip_duplicates=True)
    scan_element.Scan.insert1(
        {**rec_process_key, 'scan_id': scan_id, 'scanner': scanner, 'acq_software': acq_software})
    scan_element.ScanInfo.populate(rec_process_key, display_progress=True)

# output_dir = [x.rsplit('/', maxsplit=1)[0] for x in scan_filepaths]
# output_dir = [pathlib.Path(x) for x in output_dir]
# scan_folder = [x / process_method for x in output_dir]

fov_directory = (imaging_rec.FieldOfView & fov_key).fetch1('fov_directory')
#output_dir = pathlib.Path('/usr/people/gs6614/temp_output') / fov_directory / processing_method #TODO fix to possibly work with existing suite2p directories

recording_process_id_folder_found = True
generic_process_folder_found = True
# look firs for recording_process_id# directory
relative_output_dir = (pathlib.Path(fov_directory) / rec_process_str_key).as_posix()
output_dir = pathlib.Path(get_imaging_root_data_dir(), relative_output_dir)
if not output_dir.exists():
    recording_process_id_folder_found = False
    # look for suite2p directory
    relative_output_dir = (pathlib.Path(fov_directory) / processing_method).as_posix()
    output_dir = pathlib.Path(get_imaging_root_data_dir(), relative_output_dir)
if not output_dir.exists():
    generic_process_folder_found = False


# No results to load from
if task_mode == 'load' and not generic_process_folder_found:
    FileNotFoundError(processing_method + ' FOLDER NOT FOUND cannot load results!!!')

# Results from this specific recording_process_id already triggered
if task_mode == 'trigger' and recording_process_id_folder_found:
    print('Overwritting process output from original processing folder', output_dir)

#If trigger and not recording_process_id_folder_found make it
if task_mode == 'trigger':
    relative_output_dir = (pathlib.Path(fov_directory) / rec_process_str_key).as_posix()
    output_dir = pathlib.Path(get_imaging_root_data_dir(), relative_output_dir)
    output_dir.mkdir(parents=True,exist_ok=True)


print('RELATIVE OUTPUT DIR')
print(relative_output_dir)
print(output_dir)
#output_dir.mkdir(parents=True,exist_ok=True)

#Check if found output dir is correct
if task_mode == 'load':
    if processing_method == 'suite2p':
        print('SUITE2P METHOD SELECTED')
        # output_dir = get_suite2p_dir(scan_key)
        p = pathlib.Path(output_dir).glob('**/*')
        plane_filepaths = [x for x in p if x.is_dir()]
        for plane_filepath in plane_filepaths:
            ops_fp = plane_filepath / 'ops.npy'
            iscell_fp = plane_filepath / 'iscell.npy'
            if not ops_fp.exists() or not iscell_fp.exists():
                raise FileNotFoundError(
                    'No "ops.npy" or "iscell.npy" found. Invalid suite2p plane folder: {}'.format(plane_filepath))

    elif processing_method == 'caiman':
        raise ValueError("caiman not supported yet")
#     _required_hdf5_fields = ['/motion_correction/reference_image',
#                             '/motion_correction/correlation_image',
#                             '/motion_correction/average_image',
#                             '/motion_correction/max_image',
#                             '/estimates/A']
# #TODO: Load Caiman output files
#     # pass
#     if not output_dir.exists():
#     # if not caiman_dir.exists():
#         print('CaImAn directory not found: {}'.format(output_dir))

#     for fp in output_dir.glob('*.hdf5'):
#         task_mode='trigger'
#         with h5py.File(fp, 'r') as h5f:
#             if all(s in h5f for s in _required_hdf5_fields):
#                 caiman_fp = fp
#                 break
#     # else:
#     #     raise FileNotFoundError(
#     #         'No CaImAn analysis output file found at {}'
#     #         ' containg all required fields ({})'.format(output_dir[0], _required_hdf5_fields))


if paramset_idx_key not in imaging_element.ProcessingParamSet():

    #Get all information from process params from recording.ProcessParamSet schema
    process_params_key = dict()
    process_params_key['process_paramset_idx'] = paramset_idx
    process_params_info = (recording.ProcessParamSet() & process_params_key).fetch(as_dict=True)
    process_params = recording.ProcessParamSet().get_process_params(process_params_key)

    #print('process_params_info', process_params_info)
    print('process_params', process_params)
    print('description', process_params_info[0]['process_paramset_desc'])

    #Insert in imaging element equivalent ProcessParamSet
    imaging_element.ProcessingParamSet.insert_new_params(
    processing_method=processing_method, paramset_idx=paramset_idx+1, paramset_desc=process_params_info[0]['process_paramset_desc'], params=process_params)

imaging_element.ProcessingTask.insert1(dict(**rec_process_key,
                                            scan_id=scan_id,
                                            paramset_idx=paramset_idx+1, 
                                            processing_output_dir=relative_output_dir, 
                                            task_mode=task_mode), 
                                        skip_duplicates=True)

imaging_element.Processing.populate(rec_process_key, display_progress=True)

processing_keys = imaging_element.Processing.fetch('KEY')
for processing_key in processing_keys:
    imaging_element.Curation().create1_from_processing_task(processing_key)

imaging_element.MotionCorrection.populate(rec_process_key, display_progress=True)
imaging_element.Segmentation.populate(rec_process_key, display_progress=True)

imaging_element.Fluorescence.populate(rec_process_key, display_progress=True)
imaging_element.Activity.populate(rec_process_key, display_progress=True)

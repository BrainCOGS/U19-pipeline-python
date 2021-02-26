from u19_pipeline import acquisition, imaging
from u19_pipeline.imaging_element import (scan_element, imaging_element, Equipment,
                                          get_imaging_root_data_dir, get_scan_image_files)

import scanreader
from elements_imaging.readers import get_scanimage_acq_time, parse_scanimage_header

"""
The ingestion routine for imaging element includes:

Manual insertion:
1. scan_element.Scan 
    + this can mirror imaging.Scan
    
2. imaging_element.ProcessingTask 
    + this requires users to add new ProcessingParamSet (use ProcessingParamSet.insert_new_params method)
    + manually insert new ProcessingTask for each scan
"""

acq_software = 'ScanImage'

for scan_key in imaging.Scan.fetch('KEY'):
    scan_filepaths = get_scan_image_files(scan_key)
    try:  # attempt to read .tif as a scanimage file
        loaded_scan = scanreader.read_scan(scan_filepaths)
        recording_time = get_scanimage_acq_time(loaded_scan)
        header = parse_scanimage_header(loaded_scan)
        scanner = header['SI_imagingSystem'].strip('\'')
    except Exception as e:
        print(f'ScanImage loading error: {scan_filepaths}\n{str(e)}')
        continue

    scan_key = {**scan_key, 'scan_id': 0}
    if scan_key not in scan_element.Scan:
        Equipment.insert1({'scanner': scanner}, skip_duplicates=True)
        scan_element.Scan.insert({**scan_key, 'scanner': scanner, 'acq_software': acq_software})
        
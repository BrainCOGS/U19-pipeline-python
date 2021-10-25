
import pathlib
import os
import glob
import subprocess


file_patterns_acq = {
    "raw_imaging": ['/*.tiff', '/*.tif'],
    "commpressed_imaging": ['/*.tiff.gz', '/*.tif.gz'],
    "segmented_imaging_files": ['/*.modeling.mat', '/*suite2p/'],
    "raw_np_files": ['/*ap.bin', '/*ap.meta'],
    "sorted_np_files": ['/*.npy']
}

def check_file_pattern_dir(filepath, file_patterns):
    """
    Check if directory (or its childs) contains some files with specific pattern names
    """
    dirs_with_session_files = []
    child_dirs = [x[0] for x in os.walk(filepath)]
    patterns_found = 0
    for dir in child_dirs:
        for pat in file_patterns:
            found_file = glob.glob(dir+pat)
            if len(found_file) > 0:
                patterns_found = 1
                break

        if patterns_found:
            break

    if patterns_found:
        return 1
    else:
        return 0

def get_size_directory(path):
    command = ["du", path, '-s']
    s = subprocess.run(command, capture_output=True)
    output = s.stdout.decode('UTF-8')
    if len(output) != 0:
        kbytes = int(output.split('\t')[0])
    else:
        kbytes = -1
    return kbytes
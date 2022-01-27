
import pathlib
import os
import glob
import subprocess
import sys

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
    """
    get directory size of a folder for linux systems
    """
    command = ["du", path, '-s']
    s = subprocess.run(command, capture_output=True)
    output = s.stdout.decode('UTF-8')
    if len(output) != 0:
        kbytes = int(output.split('\t')[0])
    else:
        kbytes = -1
    return kbytes


def get_size_directory_time(path):
    """
    get directory size divided by date of a folder for linux systems
    """

    command = ["du", "--separate-dirs", "--time", path]
    output = subprocess.check_output(command)
    output = output.decode('UTF-8')
    list_values = output.split("\n")
    list_return = []
    for line in list_values:
        line_values = line.split("\t")
        dict_line = dict()
        if len(line_values) == 3:
            dict_line['size'] = line_values[0]
            dict_line['date'] = line_values[1]
            dict_line['directory'] = line_values[2]
            list_return.append(dict_line)
        
    return list_return



#!/bin/bash

# 1st Argument is directory where matlab script is located
cd $1

# 2nd Argument is string_key for given recording
key=$2
matlab_command="populate_Imaging_AcquiredTiff('"
matlab_command+=$2
matlab_command+="');exit;"

# Load module and execute string
module load matlab/R2020b
matlab -singleCompThread -nodisplay -nosplash -r $matlab_command
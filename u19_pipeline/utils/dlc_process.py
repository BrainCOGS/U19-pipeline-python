
import os
import pathlib
import pickle
import sys

import deeplabcut
import numpy as np
import pandas as pd
from scipy import stats
from skimage.measure import EllipseModel

import u19_pipeline.utils.path_utils as pu

# Pupil diameter pipeline functions

def analyzeVideo(videoPath=None, modelPath=None, destinationFolder=None):
    """
    Stores the analized video data from videoPath as h5 file in the destination folder using the DLC model in modelPath
    Arguments:
        videoPath: path of the video to analyze
        modelPath: path of the DLC model that will be used to analyze the video
        destinationFolder: path of the folder that will store the analyzed data results
    """
    # Analyze the video using the selected modelPath and videoPath

    configPath = os.path.join(modelPath,'config.yaml')
    deeplabcut.analyze_videos(configPath, videoPath, destfolder=destinationFolder)

def getPupilDiameter(destinationFolder=None):
    """
    Returns a pupil diameter numpy array from an analized video data stored in analyzedVideoDataPath
    Arguments:
        analyzedVideoDataPath: path of the video to analyze
    Returns:
        An array that contains the pupil diameter (index is the video frame) [numpy Array]
    """
    # TODO make the function
    
    # Read the analyzed video data h5 file
    h5_file = pu.get_filepattern_paths(destinationFolder, "/*.h5")

    print(h5_file)

    if len(h5_file) == 0:
        raise Exception('No h5 file in directory: '+ destinationFolder)
    if len(h5_file) > 1:
        raise Exception('To many h5 files in directory: '+ destinationFolder)
    
    h5_file = h5_file[0]
    labels = pd.read_hdf(h5_file)

    # Create a data frame of the same size ad the analyzed video data filled with zeros
    df = pd.DataFrame(np.zeros(1), columns=['PupilDiameter'])
    # For each frame, get the x and y coordinates of the points around the pupil, fit an ellipse and calculate the diameter of a circle with the same area as the ellipse
    for i in range(labels.index.size):
        subset = labels.loc[i]
        x = subset.xs('x', level='coords').to_numpy()[0:8]
        y = subset.xs('y', level='coords').to_numpy()[0:8]
        xy = np.column_stack((x,y))
        # Fit the points to an ellipse and get the parameters (estimate X center coordinate, estimate Y center coordinate, a, b, theta)
        ellipse = EllipseModel()
        ellipse.estimate(xy)
        # Calculate the area of the ellipse from the parameters a and b
        ellipseArea = np.pi * ellipse.params[2] * ellipse.params[3]
        # Get the diameter of a circle from the area of the ellipse
        pupilDiameter = 2 * np.sqrt(ellipseArea/np.pi)
        df.loc[i] = pupilDiameter

    # Get outliers (frames where either the mice have the eyes closed (blink or groom) or deeplabcut fails to track the pupil correctly)

    # Calculate the zscore of the data frame
    zscore = np.abs(stats.zscore(df))
    # Set a treshold for a valid zscore value (determined empirically)
    outlierFlags = np.abs(zscore) > 2
    # Get a boolean array where true correspond to the frame with an outlier diameter
    outlierFlags = outlierFlags.rename(columns={outlierFlags.columns[0]: "OutlierFlag"})
    # Concatenate outlier flags array to remove outliers from pupil diameter array
    temp = pd.concat([df, outlierFlags], axis=1)
    temp.loc[temp['OutlierFlag'] is True, 'PupilDiameter'] = None
    pupilDiameter = temp['PupilDiameter'].to_numpy()

    filename = pathlib.Path(destinationFolder, "pupil_diameter.pickle").as_posix()

    file_to_store = open(filename, "wb")
    pickle.dump(pupilDiameter, file_to_store)
    file_to_store.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    print(args)
    analyzeVideo(videoPath=args[0], modelPath=args[1], destinationFolder=args[2])
    getPupilDiameter(destinationFolder=args[2])
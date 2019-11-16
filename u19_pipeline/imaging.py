"""This module defines tables in the schema U19_imaging"""

import datajoint as dj
try:
    from ScanImageTiffReader import ScanImageTiffReader
    import scanreader
except:
    pass

import numpy as np
import datetime
import platform

import re
from . import lab, reference, acquisition
import os
import glob
from os import path
import scipy.io as sio


schema = dj.schema(dj.config['database.prefix'] + 'imaging')


@schema
class Scan(dj.Imported):
    definition = """
    -> acquisition.Session
    ---
    scan_directory       : varchar(255)
    gdd=null             : float
    wavelength=920       : float                        # in nm
    pmt_gain=null        : float
    -> [nullable] reference.BrainArea.proj(imaging_area="brain_area")
    frame_time           : longblob
    """

    class File(dj.Part):
        definition = """
        -> Scan
        file_number          : int                          # file number of a given scan
        ---
        scan_filename        : varchar(255)
        """


@schema
class ScanInfo(dj.Imported):
    definition = """
    # scan meta information from the tiff file
    -> Scan
    ---
    nfields=1            : tinyint                      # number of fields
    nchannels            : tinyint                      # number of channels
    nframes              : int                          # number of recorded frames
    nframes_requested    : int                          # number of requested frames (from header)
    px_height            : smallint                     # lines per frame
    px_width             : smallint                     # pixels per line
    um_height=null       : float                        # height in microns
    um_width=null        : float                        # width in microns
    x=null               : float                        # (um) center of scan in the motor coordinate system
    y=null               : float                        # (um) center of scan in the motor coordinate system
    fps                  : float                        # (Hz) frames per second
    zoom                 : decimal(5,2)                 # zoom factor
    bidirectional        : tinyint                      # true = bidirectional scanning
    usecs_per_line       : float                        # microseconds per scan line
    fill_fraction_temp   : float                        # raster scan temporal fill fraction (see scanimage)
    fill_fraction_space  : float                        # raster scan spatial fill fraction (see scanimage)
    """



@schema
class MotionCorrectionMethod(dj.Lookup):
    definition = """
    mcorr_method:           varchar(128)
    """

    contents = zip(['cv.motionCorrect'])


@schema
class MotionCorrection(dj.Imported):
    definition = """
    -> Scan.File
    -> MotionCorrectionMethod       # meta file, frameMCorr-method
    ---
    x_shifts                        : longblob      # nFrames x 2, meta file, frameMCorr-xShifts
    y_shifts                        : longblob      # nFrames x 2, meta file, frameMCorr-yShifts
    reference_image                 : longblob      # 512 x 512, meta file, frameMCorr-reference
    motion_corrected_average_image  : longblob      # 512 x 512, meta file, activity
    mcorr_metric                    : varchar(64)   # frameMCorr-metric-name
    #motion_corrected_movie          : longblob      # in summary file, 1/10 down sampled, need to be externalized
    """
    key_source = Scan()

    def make(self, key):
        scan_dir = (Scan & key).fetch1('scan_directory')
        files = Scan.File & key

        meta_pattern = key['subject_id'] + '_' + str(key['session_date']).replace('-', '') + '*meta.mat'
        file_name_pattern = path.join(scan_dir, meta_pattern)
        f = glob.glob(file_name_pattern)
        if not len(f):
            return
        meta_data = sio.loadmat(f[0], struct_as_record=False, squeeze_me=True)

        for ikey, file_key in enumerate(files.fetch('KEY')):
            if ikey > 16:
                return
            file_number = (files & file_key).fetch1('file_number')

            mcorr = file_key.copy()
            sync = file_key.copy()
            frame_mcorr = meta_data['frameMCorr'][ikey]
            mcorr.update(
                mcorr_method=frame_mcorr.method,
                x_shifts=frame_mcorr.xShifts,
                y_shifts=frame_mcorr.yShifts,
                reference_image=frame_mcorr.reference,
                motion_corrected_average_image=meta_data['activity'],
                mcorr_metric=frame_mcorr.metric.name
            )
            self.insert1(mcorr)
            sync.update(
                mcorr_method=frame_mcorr.method,
                frame_behavior_idx=meta_data['imaging'][ikey].iteration,
                frame_block_idx=meta_data['imaging'][ikey].block,
                frame_trial_idx=meta_data['imaging'][ikey].trial
            )

            SyncImagingBehavior.insert1(sync)


@schema
class SyncImagingBehavior(dj.Manual): # info in meta imaging
    definition = """
    -> MotionCorrection
    ---
    frame_behavior_idx:    longblob   # register the sample number of behavior recording to each frame, some extra zeros in file 1, marking that the behavior recording hasn't started yet.
                                      #1 x nFrames, metadata-imaging-iteration
    frame_block_idx:       longblob   # register block number for each frame, metadata-imaging-block
    frame_trial_idx:       longblob   # register trial number for each frame, metadata-imaging-trial
    """


@schema
class SegmentationMethod(dj.Lookup):
    definition = """
    method:    varchar(16)
    """
    contents = zip(['cnmf', 'manual'])


@schema
class Segmentation(dj.Imported):
    definition = """
    -> Scan
    -> SegmentationMethod
    """
    key_source = (Scan & MotionCorrection) * \
        (SegmentationMethod & 'method="cnmf"')

    def make(self, key):
        self.insert1(key)
        scan_dir = (Scan & key).fetch1('scan_directory')
        files = Scan.File & key
        file_numbers = files.fetch('file_number')

        file_name_pattern = key['subject_id'] + '_' + str(key['session_date']).replace('-', '') \
            + str(min(file_numbers)) + '-' + str(max(file_numbers)) + '*.cnmf-proto-roi-posthoc.mat'

        file_pattern = path.join(scan_dir, file_name_pattern)
        f = glob.glob(file_pattern)
        cnmf = sio.loadmat(f[0], struct_as_record=False, squeeze_me=True)
        image_size = cnmf['cnmf'].region.ImageSize
        nrois = len(cnmf['roi'])

        for i_roi in range(0, nrois):
            roi = key.copy()
            roi['roi_spatial'] = np.reshape(
                cnmf['cnmf'].spatial.todense()[:, i_roi], image_size)
            self.Roi.insert1('roi')

    class Background(dj.Part):
        definition = """
        -> master
        ---
        background_spatial:   longblob   # 505 x 504, last column of cnmf spatial
        """

    class Roi(dj.Part):
        definition = """
        -> master
        roi_idx:       int
        ---
        roi_spatial:      longblob     # 505 x 504, from cnmf-
        """

class Morphology(dj.Manual):
    definition = """
    -> Segmentation.Roi
    ---
    morphology:  enum('Doughnut', 'Blob', 'Puncta', 'Filament', 'Other', 'Noise')
    """


@schema
class Trace(dj.Computed):
    definition = """
    -> Segmentation.Roi
    ---
    dff:   longblob     # delta f/f for each cell, 1 x nFrames  # cnmf-spiking?
    spiking
    
    """


# @schema
# class TrialTrace(dj.Computed):
#     definition = """
#     -> Segmentation.Roi
#     -> behavior.TowersBlock.Trial
#     ---
#     trial_diff:     longblob       # cut dff for each trial
#     cue_range:      blob           # [start_idx, stop_idx]
#     delay_range:    blob           # [start_idx, stop_idx]
#     arm_range:      blob           # [start_idx, stop_idx]
#     iti_range:      blob           # [start_idx, stop_idx]
#     """
#     key_source = MotionCorrection

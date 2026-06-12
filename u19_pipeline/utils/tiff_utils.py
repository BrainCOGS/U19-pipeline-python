import gzip
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import tifffile

import u19_pipeline.utils.tiff_matlab_imaging_utils as tmiu

tif_number_fmt = r'_[0-9]{5}\.tif'
tif_gz_number_fmt = r'_[0-9]{5}\.tif\.gz'

patt_acq_number = r'_[0-9]{5}_'
patt_file_number = r'_[0-9]{5}\.'


def check_tif_files(tif_dir):

    tif_dir = Path(tif_dir)

    is_compressed = False

    fl = sorted(tif_dir.glob('*.tif'))

    if not fl:

        gz_files = sorted(tif_dir.glob('*.tif.gz'))

        if gz_files:

            is_compressed = True

            for gz_file in gz_files:
                with gzip.open(gz_file, 'rb') as f_in:
                    with open(gz_file.with_suffix(''), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

            fl = sorted(tif_dir.glob('*.tif'))

        else:
            raise FileNotFoundError(
                "No tif or tif.gz files found"
            )

    fl = [f.name for f in fl]

    match = re.search(tif_number_fmt, fl[0])

    if match is None:
        raise ValueError(
            f'Invalid tif naming format: {fl[0]}'
        )

    basename = fl[0][:match.start()]

    return fl, basename, is_compressed


def get_parsed_info_2photon(fl):

    with ProcessPoolExecutor(max_workers=16) as exe:

        results = list(exe.map(tmiu.parse_tif_header_2photon,fl))

    imheader = [r[0] for r in results]
    parsed_info = [r[1] for r in results]

    return imheader, parsed_info


def get_parsed_info_mesoscope(fl):

    with ProcessPoolExecutor(max_workers=16) as exe:

        results = list(exe.map(tmiu.parse_tif_header_mesoscope,fl))

    imheader = [r[0] for r in results]
    parsed_info = [r[1] for r in results]

    return imheader, parsed_info


def get_recording_info(fl, imheader, parsed_info):

    frames_per_file = np.zeros(len(fl), dtype=int)

    for i, file in enumerate(fl):

        if i == 0:
            rec_info = parsed_info[i]
            rec_info['Timing']['BehavFrames'] = np.array(rec_info['Timing']['BehavFrames'], dtype=object)

        else:

            if parsed_info[i]['Timing']['Frame_ts_sec'][0] == 0:

                parsed_info[i]['Timing']['Frame_ts_sec'] += (
                    rec_info['Timing']['Frame_ts_sec'][-1]
                    + 1 / rec_info['frameRate']
                )

            rec_info['Timing']['Frame_ts_sec'] = np.concatenate([
                rec_info['Timing']['Frame_ts_sec'],
                parsed_info[i]['Timing']['Frame_ts_sec']
            ])

            aux = np.array(parsed_info[i]['Timing']['BehavFrames'], dtype=object)
            aux = np.squeeze(aux)

            if aux.size > 0:
                rec_info['Timing']['BehavFrames'] = np.concatenate([
                    rec_info['Timing']['BehavFrames'],
                    aux
                ])

        frames_per_file[i] = len(imheader[i])

    rec_info['nFrames'] = len(
        rec_info['Timing']['Frame_ts_sec']
    )

    return rec_info, frames_per_file


def get_nfovs(rec_info, is_mesoscope):

    if is_mesoscope:
        return len(rec_info['ROI'])

    return 1

def check_acqtime(acq_time, scan_directory):

    try:
        dt = datetime.strptime(acq_time,'%Y %m %d %H %M %S.%f'
        )

        return dt.strftime('%Y-%m-%d %H:%M:%S')

    except Exception:

        dirname = Path(scan_directory).name
        date_match = re.search(r'(\d{8})', dirname)

        if date_match:

            d = date_match.group(1)

            return (
                f'{d[:4]}-{d[4:6]}-{d[6:8]} 00:00:00'
            )

        return '1000-01-01 00:00:00'
    

def get_last_good_frame(frames_per_file, scan_directory):

    last_good_file = tmiu.select_files_from_mean_f(scan_directory)
    cumulative_frames = np.cumsum(frames_per_file)


    return last_good_file, cumulative_frames


def create_scan_info_key(key, rec_info, bucket_dir):

    filename = Path(rec_info['Filename']).name

    scan_info_key = key.copy()

    scan_info_key['file_name_base'] = str(
        Path('/') / bucket_dir / filename
    )

    scan_info_key['scan_width'] = rec_info['Width']
    scan_info_key['scan_height'] = rec_info['Height']

    try:
        datetime.strptime(rec_info['AcqTime'],'%Y-%m-%d %H:%M:%S')
        scan_info_key['acq_time'] = rec_info['AcqTime']

    except Exception:
        scan_info_key['acq_time'] = '1000-01-01 00:00:00'

    scan_info_key['n_depths'] = rec_info['nDepths']
    scan_info_key['scan_depths'] = rec_info['Zs']
    scan_info_key['frame_rate'] = rec_info['frameRate']
    scan_info_key['inter_fov_lag_sec'] = rec_info['interROIlag_sec']
    scan_info_key['frame_ts_sec'] = rec_info['Timing']['Frame_ts_sec']

    scope = rec_info['Scope']
    scan_info_key['power_percent'] = scope.get('Power_percent', 0)
    scan_info_key['channels'] = scope['Channels']
    scan_info_key['cfg_filename'] = scope['cfgFilename']
    scan_info_key['usr_filename'] = scope['usrFilename']
    scan_info_key['fast_z_lag'] = scope['fastZ_lag']
    scan_info_key['fast_z_flyback_time'] = scope['fastZ_flybackTime']
    scan_info_key['line_period'] = scope['linePeriod']
    scan_info_key['scan_frame_period'] = scope['scanFramePeriod']
    scan_info_key['scan_volume_rate'] = scope['scanVolumeRate']
    scan_info_key['flyback_time_per_frame'] = scope['flybackTimePerFrame']
    scan_info_key['flyto_time_per_scan_field'] = scope['flytoTimePerScanfield']
    scan_info_key['fov_corner_points'] = scope['fovCornerPoints']

    scan_info_key['nfovs'] = rec_info['nfovs']
    scan_info_key['nframes'] = rec_info['nFrames']
    scan_info_key['nframes_good'] = rec_info['nframes_good']
    scan_info_key['last_good_file'] = rec_info['last_good_file']

    if 'stacks_enabled' in scope:
        scan_info_key['stacks_enabled'] = scope['stacks_enabled']
    
    if 'stackActuator' in scope:
        scan_info_key['stack_actuator'] = scope['stackActuator']
    
    if 'stackDefinition' in scope:
        scan_info_key['stack_definition'] = scope['stackDefinition']
    
    if 'motionCorrection_enabled' in scope:
        scan_info_key['motion_correction_enabled'] = scope['motionCorrection_enabled']
    
    if 'motionCorMode' in scope:
        scan_info_key['motion_correction_mode'] = scope['motionCorMode']
    
    return scan_info_key


def remove_compressed_videos(fl, directory):
    """
    Remove .gz files if the corresponding extracted .tif
    files already exist.

    Parameters
    ----------
    fl : list
        List of TIFF filenames

    directory : str or Path
        Directory containing TIFF files
    """

    directory = Path(directory)

    for tif_file in fl:

        file_base = directory / tif_file

        gz_file = Path(str(file_base) + '.gz')

        if gz_file.exists() and file_base.exists():

            print(f"Removing {gz_file}")

            gz_file.unlink()

        else:

            print(
                f"Could not find compressed pair {file_base}"
            )



# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def replace_frame_timestamp(imdescription, lag):
    """
    Replace frameTimestamps_sec value with lag-adjusted value.
    """
    pattern = r'(?<=frameTimestamps_sec = )([0-9]+\.[0-9]+)'
    match = re.search(pattern, imdescription)

    if match:
        old = float(match.group(1))
        new = old + lag
        imdescription = re.sub(pattern, f"{new}", imdescription, count=1)

    return imdescription


def clean_software_string(software):
    """
    Apply ScanImage header corrections.
    """
    if software is None:
        return ""

    software = software.replace(
        "hRoiManager.mroiEnable = 1",
        "hRoiManager.mroiEnable = 0"
    )

    idx = software.find("SI.hRoiManager.imagingFovUm")
    if idx != -1:
        newline_idx = software.find("\n", idx)
        if newline_idx != -1:
            software = software[:idx] + software[newline_idx + 1:]

    return software


# -----------------------------------------------------------------------------
# Mesoscope ROI splitting
# -----------------------------------------------------------------------------

def get_fov_mesoscope(
    fl,
    key_data,
    skip_parsing,
    imheader,
    rec_info,
    basename,
    cumulative_frames,
    scan_dirs_db,
    imaging_root
):
    """
    Python translation of MATLAB insert_fov_mesoscope.

    Splits mesoscope TIFF stacks into separate ROI/depth TIFFs.
    """

    n_roi = rec_info["nROIs"]

    if not skip_parsing:

        print("\tparsing ROIs...")

        roi_nr = [roi["pixelResolutionXY"][1] for roi in rec_info["ROI"]]
        roi_nc = [roi["pixelResolutionXY"][0] for roi in rec_info["ROI"]]

        inter_roi_lag = rec_info["interROIlag_sec"]
        depths = rec_info["nDepths"]

        which_depths = sorted(
            list(set([roi["Zs"] for roi in rec_info["ROI"]]))
        )

        # ---------------------------------------------------------------------
        # Create directories
        # ---------------------------------------------------------------------

        for idepth in range(depths):

            which_roi = [
                i for i, roi in enumerate(rec_info["ROI"])
                if roi["Zs"] == which_depths[idepth]
            ]

            for iroi in which_roi:
                dirname = f"ROI{iroi+1:02d}_z{idepth+1}"
                os.makedirs(dirname, exist_ok=True)

        # ---------------------------------------------------------------------
        # Process TIFFs
        # ---------------------------------------------------------------------

        for iF, tif_file in enumerate(fl):

            with tifffile.TiffFile(tif_file) as tif:

                pages = tif.pages

                current_header = pages[0].tags

                # -------------------------------------------------------------
                # Read stack
                # -------------------------------------------------------------

                first_page = pages[0].asarray()

                thisstack = np.zeros(
                    (
                        imheader[iF][0]["Height"],
                        512,
                        len(imheader[iF])
                    ),
                    dtype=np.uint16
                )

                pixel2sum = 1

                for iframe, page in enumerate(pages):

                    temp_stack = page.asarray()

                    if temp_stack.shape[1] != thisstack.shape[1]:

                        pixel2sum = imheader[iF][0]["Width"] // 512

                        reshaped = temp_stack.reshape(
                            temp_stack.shape[0],
                            pixel2sum,
                            512
                        )

                        temp_stack = reshaped.sum(axis=1)

                    else:
                        pixel2sum = 1

                    thisstack[:, :, iframe] = temp_stack

                nr, nc, _ = thisstack.shape

                # -------------------------------------------------------------
                # Split ROIs
                # -------------------------------------------------------------

                for idepth in range(depths):

                    ilag = 0
                    rowct = 0

                    which_roi = [
                        i for i, roi in enumerate(rec_info["ROI"])
                        if roi["Zs"] == which_depths[idepth]
                    ]

                    for iroi in which_roi:

                        zidx = list(range(idepth, thisstack.shape[2], depths))

                        substack = thisstack[
                            rowct:rowct + roi_nr[iroi],
                            :nc,
                            :
                        ][:, :, zidx]

                        # -----------------------------------------------------
                        # Output filename
                        # -----------------------------------------------------

                        match = re.search(r'_[0-9]{5}\.tif', tif_file)

                        thisfn = (
                            f"./ROI{iroi+1:02d}_z{idepth+1}/"
                            f"{tif_file[:match.start()]}"
                            f"ROI{iroi+1:02d}_z{idepth+1}_"
                            f"{tif_file[match.start()+1:]}"
                        )

                        # -----------------------------------------------------
                        # Metadata
                        # -----------------------------------------------------

                        first_desc = imheader[iF][zidx[0]]["ImageDescription"]

                        first_desc = replace_frame_timestamp(
                            first_desc,
                            inter_roi_lag * ilag
                        )

                        software = clean_software_string(
                            current_header.get("Software", None).value
                            if "Software" in current_header else ""
                        )

                        artist = (
                            current_header.get("Artist", None).value
                            if "Artist" in current_header else ""
                        )

                        metadata = {
                            "ImageDescription": first_desc,
                            "Software": software,
                            "Artist": artist
                        }

                        # -----------------------------------------------------
                        # Write TIFF
                        # -----------------------------------------------------

                        with tifffile.TiffWriter(thisfn, bigtiff=True) as writer:

                            for iz in range(substack.shape[2]):

                                desc = imheader[iF][zidx[iz]][
                                    "ImageDescription"
                                ]

                                desc = replace_frame_timestamp(
                                    desc,
                                    inter_roi_lag * ilag
                                )

                                writer.write(
                                    substack[:, :, iz],
                                    description=desc,
                                    metadata=None,
                                    extratags=[]
                                )

                        ilag += 1

                        # -----------------------------------------------------
                        # Update row counter
                        # -----------------------------------------------------

                        if len(which_roi) > 1:

                            padsize = (
                                nr - sum([roi_nr[r] for r in which_roi])
                            ) / (len(which_roi) - 1)

                            rowct += int(padsize + roi_nr[iroi])

            # -----------------------------------------------------------------
            # Move original TIFF
            # -----------------------------------------------------------------

            os.makedirs("originalStacks", exist_ok=True)

            shutil.move(
                tif_file,
                os.path.join("originalStacks", tif_file)
            )


    ct = 1
    cumulative_frames = np.concatenate(
        ([0], cumulative_frames)
    )

    fov_keys = []
    tiff_files_entries = []
    for iroi in range(n_roi):

        ndepths = len(np.atleast_1d(rec_info["ROI"][iroi]["Zs"]))

        for iz in range(ndepths):

            fov_key = dict(key_data)

            fov_key["tiff_split"] = ct

            fov_key["tiff_split_directory"] = (
                f"{scan_dirs_db['recording_directory']}/"
                f"ROI{iroi+1:02d}_z{iz+1}/"
            )

            roi_name = rec_info["ROI"][iroi]["name"]

            if roi_name:
                thisname = f"{roi_name}_z{iz+1}"
            else:
                thisname = f"ROI{iroi+1:02d}_z{iz+1}"

            fov_key["tiff_split_name"] = thisname

            # Safe assignments
            fov_key["fov_depth"] = (
                rec_info["ROI"][iroi]["Zs"]
                if rec_info["ROI"][iroi]["Zs"] is not None
                else 0
            )

            fov_key["fov_center_xy"] = (
                rec_info["ROI"][iroi].get("centerXY", -1)
            )

            fov_key["fov_size_xy"] = (
                rec_info["ROI"][iroi].get("sizeXY", -1)
            )

            fov_key["fov_rotation_degrees"] = (
                rec_info["ROI"][iroi].get("rotationDegrees", -1)
            )

            fov_key["fov_pixel_resolution_xy"] = (
                rec_info["ROI"][iroi].get("pixelResolutionXY", -1)
            )

            fov_key["fov_discrete_plane_mode"] = (
                rec_info["ROI"][iroi].get("discretePlaneMode", -1)
            )
            if not fov_key["fov_discrete_plane_mode"]:
                fov_key["fov_discrete_plane_mode"] = 0

            fov_key["power_percent"] = (
                rec_info["ROI"][iroi].get(
                    "Power_percent",
                    rec_info["Scope"]["Power_percent"]
                )
            )

            fov_keys.append(fov_key)

            ct += 1

            tiff_split_directory = (
            Path(imaging_root)
            / fov_key["tiff_split_directory"]
            )

            tif_files = sorted(
            tiff_split_directory.glob("*.tif")
            )

            file_entries = []

            for iF, tif_file in enumerate(tif_files):

                entry = dict(key_data)

                entry["tiff_split"] = fov_key["tiff_split"]
                entry["file_number"] = iF + 1
                entry["tiff_split_filename"] = tif_file.name

                entry["file_frame_range"] = [
                    cumulative_frames[iF] + 1,
                    cumulative_frames[iF + 1]
                ]

                file_entries.append(entry)
            
            tiff_files_entries += file_entries


    return fov_keys, tiff_files_entries


# -----------------------------------------------------------------------------
# 2-photon FOV insert
# -----------------------------------------------------------------------------

def get_fov_photonmicro(key, rec_info, scan_dirs_db):
    """
    Insert FOV metadata for 2-photon imaging.
    """

    fovkey = dict(key)

    fovkey["tiff_split"] = 1
    fovkey["tiff_split_directory"] = (
        scan_dirs_db["recording_directory"]
    )

    fovkey["fov_depth"] = 0
    fovkey["fov_center_xy"] = 0
    fovkey["fov_size_xy"] = 0
    fovkey["fov_rotation_degrees"] = 0
    fovkey["fov_pixel_resolution_xy"] = 0
    fovkey["fov_discrete_plane_mode"] = 0

    fovkey["power_percent"] = (
        rec_info["Scope"]["Power_percent"]
    )

    return fovkey


# -----------------------------------------------------------------------------
# 2-photon file insertion
# -----------------------------------------------------------------------------

def get_fovfile_photonmicro(
    key,
    fl,
    imheader,
    patt_acq_number=r'_[0-9]{5}_',
    patt_file_number=r'_[0-9]{5}\.'
):
    """
    Insert TIFF split file entries for 2-photon imaging.
    """

    filekeys = []

    if len(fl) > 0:

        prefile_frame_range = 0

        for iF, filename in enumerate(fl):

            acq_string = re.findall(
                patt_acq_number,
                filename
            )

            number_string = re.findall(
                patt_file_number,
                filename
            )

            if len(acq_string) == 1 and len(number_string) == 1:

                file_number = int(
                    number_string[0][1:-1]
                )
                acq_number = int(
                    acq_string[0][1:-1]
                )
                if acq_number != 1:
                    file_number = file_number-1+(acq_number-1)*100

                nframes = len(imheader[iF])

                frame_range = [
                    prefile_frame_range + 1,
                    prefile_frame_range + nframes
                ]

                prefile_frame_range = frame_range[1]

                entry = dict(key)

                entry["tiff_split"] = 1
                entry["file_number"] = file_number
                entry["tiff_split_filename"] = Path(filename).name
                entry["file_frame_range"] = frame_range

                filekeys.append(entry)

    return filekeys
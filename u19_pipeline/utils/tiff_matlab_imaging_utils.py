import ast
import re
import time
from pathlib import Path

import numpy as np
import tifffile as tiff
from sklearn.linear_model import HuberRegressor

xySizeFactor = 1.05  # images are this much larger than nominal size
zFactor = 1.45  # actual displacement in z vs command


def select_files_from_mean_f(scan_directory, f_decrease_threshold=15):
    """
    Estimate bleaching by loading only the first frame
    from each TIFF file.

    Parameters
    ----------
    scan_directory : str or Path
        Directory containing TIFF files

    f_decrease_threshold : float
        Allowed fluorescence decrease percentage

    Returns
    -------
    last_good_file : int
        Index of last acceptable TIFF file

    last_good_frame : int
        Approximate cumulative frame count
    """

    start_time = time.time()

    print("Estimating bleaching...", end="", flush=True)

    scan_directory = Path(scan_directory)

    tif_files = sorted(scan_directory.glob("*.tif"))

    n_files = len(tif_files)

    if n_files == 0:
        raise FileNotFoundError(f"No TIFF files found in {scan_directory}")

    mean_f = np.zeros(n_files)

    frame_id = np.zeros(n_files, dtype=int)

    ct = 1

    # ---------------------------------------------------------
    # Read first frame from each TIFF
    # ---------------------------------------------------------

    for i, tif_path in enumerate(tif_files):
        print(".", end="", flush=True)

        with tiff.TiffFile(tif_path) as tif_obj:
            n_frames = len(tif_obj.pages)

            first_frame = tif_obj.pages[0].asarray()

        # MATLAB logic:
        # if image height < 512 use whole image
        # otherwise only use top 512 rows

        if first_frame.shape[0] < 512:
            mean_f[i] = np.mean(first_frame)
        else:
            mean_f[i] = np.mean(first_frame[:512, :])

        frame_id[i] = ct

        ct += n_frames

    # ---------------------------------------------------------
    # Fit fluorescence decay trend
    # ---------------------------------------------------------

    x = frame_id.reshape(-1, 1)

    if len(mean_f) > 1:
        try:
            # Robust regression equivalent to MATLAB robustfit
            model = HuberRegressor()

            model.fit(x, mean_f)

            yhat = model.predict(x)

        except Exception:
            # fallback to standard linear fit
            coeffs = np.polyfit(frame_id, mean_f, 1)

            yhat = np.polyval(coeffs, frame_id)

        threshold_value = yhat[0] - (f_decrease_threshold / 100.0) * yhat[0]

        valid_idx = np.where(yhat > threshold_value)[0]

        if len(valid_idx) > 0:
            last_good_file = valid_idx[-1]
        else:
            last_good_file = n_files - 1

    else:
        last_good_file = 0

    # ---------------------------------------------------------
    # Compute cumulative frame count
    # ---------------------------------------------------------

    cumulative_frames = np.cumsum(frame_id)

    last_good_frame = cumulative_frames[last_good_file]

    elapsed_minutes = (time.time() - start_time) / 60

    print(f" done after {elapsed_minutes:.1f} min")

    return last_good_file


def parse_tif_header_2photon(tif_fn, skip_behav_sync=False):
    """
    Parse ScanImage TIFF metadata for 2-photon imaging.

    Parameters
    ----------
    tif_fn : str or Path
        TIFF filename

    skip_behav_sync : bool
        Skip behavioral sync parsing

    Returns
    -------
    header : list
        Raw tifffile page metadata

    parsed_info : dict
        Parsed metadata dictionary
    """

    tif_fn = Path(tif_fn)

    # ---------------------------------------------------------
    # Load TIFF metadata
    # ---------------------------------------------------------

    with tiff.TiffFile(tif_fn) as tif:
        pages = tif.pages

        header = pages

        first_page = pages[0]

        # ScanImage stores metadata in ImageDescription
        image_description = first_page.description

        # Some ScanImage versions also use Software tag
        software = first_page.tags.get("Software")

        if software is not None:
            scope_str = str(software.value)
        else:
            scope_str = image_description

        parsed_info = {}

        # -----------------------------------------------------
        # General image info
        # -----------------------------------------------------

        parsed_info["Filename"] = str(tif_fn)

        parsed_info["Width"] = first_page.imagewidth

        parsed_info["Height"] = first_page.imagelength

        # Acquisition time
        acq_match = re.search(r"epoch = \[([0-9].+?)\]", image_description)

        parsed_info["AcqTime"] = acq_match.group(1) if acq_match else ""

        # Number of depths
        depth_match = re.search(r"SI\.hFastZ\.numFramesPerVolume = (\d+)", scope_str)

        if depth_match:
            parsed_info["nDepths"] = int(depth_match.group(1))
        else:
            parsed_info["nDepths"] = 0

        parsed_info["Zs"] = -1

        # Frame rate
        frame_rate_match = re.search(
            r"SI\.hRoiManager\.scanVolumeRate = ([0-9]+\.[0-9]+)", scope_str
        )

        parsed_info["frameRate"] = (
            float(frame_rate_match.group(1)) if frame_rate_match else 0
        )

        # ROI lag
        lag_match = re.search(
            r"SI\.hScan2D\.flytoTimePerScanfield = ([0-9]+\.[0-9]+)", scope_str
        )

        parsed_info["interROIlag_sec"] = float(lag_match.group(1)) if lag_match else 0

        # -----------------------------------------------------
        # Timing / behavioral sync
        # -----------------------------------------------------

        if not skip_behav_sync:
            parsed_info["Timing"] = {
                "Frame_ts_sec": np.zeros(len(pages)),
                "BehavFrames": [],
            }

            for i, page in enumerate(pages):
                desc = page.description

                # Frame timestamps
                ts_match = re.search(r"frameTimestamps_sec = ([0-9]+\.[0-9]+)", desc)

                if ts_match:
                    parsed_info["Timing"]["Frame_ts_sec"][i] = float(ts_match.group(1))

                # I2C behavioral sync
                i2c_match = re.search(r"I2CData = ({.+})", desc)

                if i2c_match:
                    raw_data = i2c_match.group(1)

                    try:
                        # MATLAB eval equivalent
                        behav_data = ast.literal_eval(raw_data)

                    except Exception:
                        behav_data = np.nan

                else:
                    behav_data = []

                parsed_info["Timing"]["BehavFrames"].append(behav_data)

        # -----------------------------------------------------
        # Microscope / ScanImage metadata
        # -----------------------------------------------------

        parsed_info["Scope"] = {}

        # Resolution scaling
        if "objectiveResolution" in scope_str:
            try:
                res_match = re.search(
                    r"SI\.objectiveResolution = ([0-9]+\.[0-9]+)", scope_str
                )

                resolution_factor = xySizeFactor * float(res_match.group(1))

            except Exception:
                res_match = re.search(r"SI\.objectiveResolution = (\d+)", scope_str)

                resolution_factor = xySizeFactor * float(res_match.group(1))

        else:
            resolution_factor = 1

        # -----------------------------------------------------
        # Power
        # -----------------------------------------------------

        power_match = re.search(r"SI\.hBeams\.powers = (\d+)", scope_str)

        parsed_info["Scope"]["Power_percent"] = (
            float(power_match.group(1)) if power_match else 0
        )

        # -----------------------------------------------------
        # Channels
        # -----------------------------------------------------

        channel_match = re.search(r"SI\.hChannels\.channelSave = (\d+)", scope_str)

        parsed_info["Scope"]["Channels"] = (
            int(channel_match.group(1)) if channel_match else 0
        )

        # -----------------------------------------------------
        # Config / user filenames
        # -----------------------------------------------------

        cfg_match = re.search(
            r"SI\.hConfigurationSaver\.cfgFilename = (.+cfg)", scope_str
        )

        parsed_info["Scope"]["cfgFilename"] = (
            cfg_match.group(1).strip() if cfg_match else ""
        )

        usr_match = re.search(
            r"SI\.hConfigurationSaver\.usrFilename = (.+usr)", scope_str
        )

        parsed_info["Scope"]["usrFilename"] = (
            usr_match.group(1).strip() if usr_match else ""
        )

        # -----------------------------------------------------
        # Fast-Z lag
        # -----------------------------------------------------

        lag_match = re.search(r"SI\.hFastZ\.actuatorLag = ([0-9eE\.\-]+)", scope_str)

        parsed_info["Scope"]["fastZ_lag"] = (
            float(lag_match.group(1)) if lag_match else 0
        )

        # -----------------------------------------------------
        # Fast-Z flyback
        # -----------------------------------------------------

        flyback_match = re.search(
            r"SI\.hFastZ\.flybackTime = ([0-9]+\.[0-9]+)", scope_str
        )

        parsed_info["Scope"]["fastZ_flybackTime"] = (
            float(flyback_match.group(1)) if flyback_match else 0
        )

        # -----------------------------------------------------
        # Timing-related ScanImage parameters
        # -----------------------------------------------------

        def extract_float(pattern, default=0):

            match = re.search(pattern, scope_str)

            return float(match.group(1)) if match else default

        parsed_info["Scope"]["linePeriod"] = extract_float(
            r"SI\.hRoiManager\.linePeriod = ([0-9.eE\-]+)"
        )

        parsed_info["Scope"]["scanFramePeriod"] = extract_float(
            r"SI\.hRoiManager\.scanFramePeriod = ([0-9.eE\-]+)"
        )

        parsed_info["Scope"]["scanFrameRate"] = extract_float(
            r"SI\.hRoiManager\.scanFrameRate = ([0-9.eE\-]+)"
        )

        parsed_info["Scope"]["scanVolumeRate"] = extract_float(
            r"SI\.hRoiManager\.scanVolumeRate = ([0-9.eE\-]+)"
        )

        parsed_info["Scope"]["flybackTimePerFrame"] = extract_float(
            r"SI\.hScan2D\.flybackTimePerFrame = ([0-9.eE\-]+)"
        )

        parsed_info["Scope"]["flytoTimePerScanfield"] = extract_float(
            r"SI\.hScan2D\.flytoTimePerScanfield = ([0-9.eE\-]+)"
        )

        # -----------------------------------------------------
        # FOV corner points
        # -----------------------------------------------------

        if "fovCornerPoints" in scope_str:
            fov_match = re.search(
                r"SI\.hScan2D\.fovCornerPoints = (\[.+?\])", scope_str, re.DOTALL
            )

            if fov_match:
                try:
                    new_fov_match = fov_match.group(1).replace(" ", ",")
                    new_fov_match = new_fov_match.replace(";", ",")

                    fov_points = np.array(ast.literal_eval(new_fov_match))
                    fov_points = fov_points.reshape(4, 2)

                    parsed_info["Scope"]["fovCornerPoints"] = (
                        resolution_factor * fov_points
                    )

                except Exception:
                    parsed_info["Scope"]["fovCornerPoints"] = 0

            else:
                parsed_info["Scope"]["fovCornerPoints"] = 0

        else:
            parsed_info["Scope"]["fovCornerPoints"] = 0

    return header, parsed_info


def parse_tif_header_mesoscope(tif_fn, skip_behav_sync=False):
    """
    Parse ScanImage mesoscope TIFF metadata.

    Parameters
    ----------
    tif_fn : str or Path
        TIFF filename

    skip_behav_sync : bool
        Skip I2C behavioral synchronization parsing

    Returns
    -------
    header : tifffile pages
        Raw TIFF page metadata

    parsed_info : dict
        Parsed ScanImage metadata
    """

    tif_fn = Path(tif_fn)

    # ---------------------------------------------------------
    # Load TIFF metadata
    # ---------------------------------------------------------

    with tiff.TiffFile(tif_fn) as tif:
        pages = tif.pages

        header = pages

        first_page = pages[0]

        image_description = first_page.description

        software_tag = first_page.tags.get("Software")

        artist_tag = first_page.tags.get("Artist")

        scope_str = (
            str(software_tag.value) if software_tag is not None else image_description
        )

        roi_info = str(artist_tag.value) if artist_tag is not None else ""

        parsed_info = {}

        # -----------------------------------------------------
        # General image info
        # -----------------------------------------------------

        parsed_info["Filename"] = str(tif_fn)

        parsed_info["Width"] = first_page.imagewidth

        parsed_info["Height"] = first_page.imagelength

        acq_match = re.search(r"(?<=epoch = )\[.+?]", image_description)

        parsed_info["AcqTime"] = acq_match.group(0) if acq_match else ""

        def extract_float(pattern, text, default=0):

            match = re.search(pattern, text)

            return float(match.group(0)) if match else default

        parsed_info["frameRate"] = extract_float(
            r"(?<=SI\.hRoiManager\.scanVolumeRate = )\d+\.\d+", scope_str
        )

        parsed_info["interROIlag_sec"] = extract_float(
            r"(?<=SI\.hScan2D\.flytoTimePerScanfield = )\d+\.\d+", scope_str
        )

        # -----------------------------------------------------
        # Timing / behavior sync
        # -----------------------------------------------------

        if not skip_behav_sync:
            parsed_info["Timing"] = {
                "Frame_ts_sec": np.zeros(len(pages)),
                "BehavFrames": [],
            }

            for i, page in enumerate(pages):
                desc = page.description

                ts_match = re.search(r"(?<=frameTimestamps_sec = )\d+\.\d+", desc)

                if ts_match:
                    parsed_info["Timing"]["Frame_ts_sec"][i] = float(ts_match.group(0))

                i2c_match = re.search(r"(?<=I2CData = ){.+}", desc)

                if i2c_match:
                    raw_data = i2c_match.group(0)

                    try:
                        behav_data = ast.literal_eval(raw_data)

                    except Exception:
                        behav_data = np.nan

                else:
                    behav_data = []

                parsed_info["Timing"]["BehavFrames"].append(behav_data)

        # -----------------------------------------------------
        # ROI parsing
        # -----------------------------------------------------

        roi_marks = [
            m.start() for m in re.finditer(r'"scanimage\.mroi\.Roi"', roi_info)
        ]

        parsed_info["nROIs"] = len(roi_marks)

        parsed_info["ROI"] = []

        # Resolution scaling
        resolution_match = re.search(
            r"(?<=SI\.objectiveResolution = )\d+\.\d+", scope_str
        )

        if resolution_match:
            resolution_factor = xySizeFactor * float(resolution_match.group(0))

        else:
            resolution_factor = 1

        for i_roi in range(len(roi_marks)):
            if i_roi != len(roi_marks) - 1:
                this_roi = roi_info[roi_marks[i_roi] : roi_marks[i_roi + 1]]

            else:
                this_roi = roi_info[roi_marks[i_roi] :]

            roi_dict = {}

            # -------------------------------------------------
            # ROI name
            # -------------------------------------------------

            name_match = re.search(r'(?<="name": ")\w+\d*', this_roi)

            roi_dict["name"] = name_match.group(0) if name_match else ""

            # -------------------------------------------------
            # Z position
            # -------------------------------------------------

            z_match = re.search(r'(?<="zs": )(|-)\d+', this_roi)

            roi_dict["Zs"] = float(z_match.group(0)) if z_match else 0

            # -------------------------------------------------
            # Helper for vector parsing
            # -------------------------------------------------

            def parse_array(pattern, text):

                match = re.search(pattern, text)

                if not match:
                    return np.array([])

                try:
                    return np.array(ast.literal_eval(match.group(0)))

                except Exception:
                    return np.array([])

            # -------------------------------------------------
            # ROI geometry
            # -------------------------------------------------

            roi_dict["centerXY"] = resolution_factor * parse_array(
                r'(?<="centerXY": )\[.+?]', this_roi
            )

            roi_dict["sizeXY"] = resolution_factor * parse_array(
                r'(?<="sizeXY": )\[.+?]', this_roi
            )

            rotation_match = re.search(
                r'(?<="rotationDegrees":) (\d+|\d+\.\d+)', this_roi
            )

            roi_dict["rotationDegrees"] = (
                float(rotation_match.group(0)) if rotation_match else 0
            )

            roi_dict["pixelResolutionXY"] = parse_array(
                r'(?<="pixelResolutionXY": )\[.+?]', this_roi
            )

            discrete_match = re.search(r'(?<="discretePlaneMode":) \d', this_roi)

            roi_dict["discretePlaneMode"] = (
                bool(int(discrete_match.group(0))) if discrete_match else False
            )

            power_match = re.search(r'(?<="powers":) \d+', this_roi)

            if power_match:
                roi_dict["Power_percent"] = float(power_match.group(0))

            parsed_info["ROI"].append(roi_dict)

        # -----------------------------------------------------
        # Scope metadata
        # -----------------------------------------------------

        parsed_info["Scope"] = {}

        if not re.search(r'(?<="powers":) \d+', roi_info):
            power_match = re.search(r"(?<=SI\.hBeams\.powers = )\d+", scope_str)

            parsed_info["Scope"]["Power_percent"] = (
                float(power_match.group(0)) if power_match else 0
            )

        else:
            parsed_info["Scope"]["Power_percent"] = "discrete powers per ROI"

        # Channels
        channel_match = re.search(r"(?<=SI\.hChannels\.channelSave = )\d+", scope_str)

        parsed_info["Scope"]["Channels"] = (
            int(channel_match.group(0)) if channel_match else 0
        )

        # Config filenames
        cfg_match = re.search(
            r"(?<=SI\.hConfigurationSaver\.cfgFilename = ').+cfg", scope_str
        )

        parsed_info["Scope"]["cfgFilename"] = cfg_match.group(0) if cfg_match else ""

        usr_match = re.search(
            r"(?<=SI\.hConfigurationSaver\.usrFilename = ').+usr", scope_str
        )

        parsed_info["Scope"]["usrFilename"] = usr_match.group(0) if usr_match else ""

        # -----------------------------------------------------
        # Timing metadata
        # -----------------------------------------------------

        timing_fields = {
            "fastZ_lag": r"(?<=SI\.hFastZ\.actuatorLag = )\d+\.\d+",
            "fastZ_flybackTime": r"(?<=SI\.hFastZ\.flybackTime = )\d+\.\d+",
            "linePeriod": r"(?<=SI\.hRoiManager\.linePeriod = )\d+\.\d+e-[0-9]+",
            "scanFramePeriod": r"(?<=SI\.hRoiManager\.scanFramePeriod = )\d+\.\d+",
            "scanFrameRate": r"(?<=SI\.hRoiManager\.scanFrameRate = )\d+\.\d+",
            "scanVolumeRate": r"(?<=SI\.hRoiManager\.scanVolumeRate = )\d+\.\d+",
            "flybackTimePerFrame": r"(?<=SI\.hScan2D\.flybackTimePerFrame = )\d+\.\d+",
            "flytoTimePerScanfield": r"(?<=SI\.hScan2D\.flytoTimePerScanfield = )\d+\.\d+",
        }

        for key, pattern in timing_fields.items():
            parsed_info["Scope"][key] = extract_float(pattern, scope_str)

        # -----------------------------------------------------
        # FOV corner points
        # -----------------------------------------------------

        fov_match = re.search(r"(?<=SI\.hScan2D\.fovCornerPoints = )\[.+?]", scope_str)

        if fov_match:
            try:
                new_fov_match = fov_match.group(0).replace(" ", ",")
                new_fov_match = new_fov_match.replace(";", ",")

                fov_points = np.array(ast.literal_eval(new_fov_match))
                fov_points = fov_points.reshape(4, 2)

                parsed_info["Scope"]["fovCornerPoints"] = resolution_factor * fov_points

            except Exception:
                parsed_info["Scope"]["fovCornerPoints"] = 0

        else:
            parsed_info["Scope"]["fovCornerPoints"] = 0

        # -----------------------------------------------------
        # Stack metadata
        # -----------------------------------------------------

        stack_match = re.search(r"(?<=SI\.hStackManager\.enable = )\w+", scope_str)

        stacks_enabled = stack_match.group(0) if stack_match else "false"

        parsed_info["Scope"]["stacks_enabled"] = 1 if stacks_enabled == "true" else 0

        if stacks_enabled == "true":
            actuator_match = re.search(
                r"(?<=SI\.hStackManager\.stackActuator = ')\w+", scope_str
            )

            definition_match = re.search(
                r"(?<=SI\.hStackManager\.stackDefinition = ')\w+", scope_str
            )

            parsed_info["Scope"]["stackActuator"] = (
                actuator_match.group(0) if actuator_match else ""
            )

            parsed_info["Scope"]["stackDefinition"] = (
                definition_match.group(0) if definition_match else ""
            )

        # -----------------------------------------------------
        # Motion correction
        # -----------------------------------------------------

        motion_match = re.search(r"(?<=SI\.hMotionManager\.enable = )\w+", scope_str)

        motion_enabled = motion_match.group(0) if motion_match else "false"

        parsed_info["Scope"]["motionCorrection_enabled"] = (
            1 if motion_enabled == "true" else 0
        )

        if motion_enabled == "true":
            correction_z_match = re.search(
                r"(?<=SI\.hMotionManager\.correctionEnableZ = )\w+", scope_str
            )

            if correction_z_match and correction_z_match.group(0) == "true":
                parsed_info["Scope"]["motionCorMode"] = "automated"

            else:
                parsed_info["Scope"]["motionCorMode"] = "manual"

        # -----------------------------------------------------
        # Depths / Z planes
        # -----------------------------------------------------

        depth_match = re.search(r"SI\.hFastZ\.numFramesPerVolume = \d+", scope_str)

        if depth_match:
            parsed_info["nDepths"] = int(
                re.search(r"\d+", depth_match.group(0)).group(0)
            )

        else:
            if stacks_enabled == "true":
                slices_match = re.search(
                    r"(?<=SI\.hStackManager\.actualNumSlices = )\d+", scope_str
                )

                parsed_info["nDepths"] = (
                    int(slices_match.group(0)) if slices_match else 1
                )

            else:
                parsed_info["nDepths"] = 1

        # -----------------------------------------------------
        # Z positions
        # -----------------------------------------------------

        parsed_info["Zs"] = None

        z_patterns = [
            r"(?<=SI\.hFastZ\.userZs = )\[.+?]",
            r"(?<=SI\.hStackManager\.zs = )\[.+?]",
            r"(?<=SI\.hFastZ\.position = )(|-)\d+",
        ]

        for pattern in z_patterns:
            z_match = re.search(pattern, scope_str)

            if z_match:
                try:
                    z_val = ast.literal_eval(z_match.group(0))

                    parsed_info["Zs"] = zFactor * np.array(z_val)

                except Exception:
                    try:
                        parsed_info["Zs"] = float(z_match.group(0))

                    except Exception:
                        pass

                break

    return header, parsed_info

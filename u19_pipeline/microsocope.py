import datajoint as dj

schema = dj.schema(dj.config['database.prefix'] + 'microscope')


@schema
class Microscope(dj.Lookup):
    definition = """
    microscope_name             :   varchar(32)
    ---
    room_number                 :   varchar(16)
    optical_bay                 :   varchar(8)
    loc_on_table                :   varchar(16)
    microscope_description=''   :   varchar(2047)
    """


@schema
class Laser(dj.Manual):
    definition = """
    laser_name:     varchar(32)
    ---
    laser_model:    varchar(64)
    laser_serial:   varchar(64)
    """


@schema
class LaserMaintenance(dj.Manual):
    definition = """
    -> Laser
    laser_maintenance_time:    datetime
    ---
    type_of_maintenance:       enum("Clean filter of power", "Clean filter of laser", "Change coolant", "Check PZT", "Check Spectrum", "Wavelength sweep")
    maintenance_notes='':      varchar(255)
    """


@schema
class LaserStatus(dj.Manual):
    definition = """
    # the status of a laser and microscope coupling from the laser_change_date
    -> Microscope
    -> Laser
    laser_change_date:    date
    """


@schema
class Channel(dj.Manual):
    definition = """
    -> Microscope
    channel_name:  varchar(16)
    """


@schema
class DichroicMirrorType(dj.Manual):
    definition = """
    mirror_type:             varchar(16)
    ---
    mirror_brand='':         varchar(64)
    mirror_model='':         varchar(64)
    mirror_spectrum=null:    varchar(255) # link to google drive picture
    """


@schema
class FilterType(dj.Manual):
    definition = """
    filter_type:             varchar(32)
    ---
    filter_brand='':         varchar(64)
    filter_model='':         varchar(64)
    filter_spectrum=null:    varchar(255) # link to google drive picture
    """


@schema
class ObjectiveLensType(dj.Manual):
    definition = """
    lens_type:             varchar(32)
    ---
    lens_brand='':         varchar(64)
    lens_model='':         varchar(64)
    """


@schema
class ScannerType(dj.Manual):
    definition = """
    scanner_type:              enum("galvo", "resonance")
    ---
    resonance_freq=null:       int
    mirror_size:               int    # in mm
    scanner_config:            enum("xy", "xyy", "conjugated x and y", "other")
    scanner_info='':           varchar(512)
    """


@schema
class Pmt(dj.Manual):
    definition = """
    pmt_name:               varchar(32)
    ---
    pmt_brand:              varchar(64)
    pmt_model:              varchar(64)
    pmt_serial:             varchar(64)
    pmt_date_of_first_use:  date
    """


@schema
class PreAmplifierType(dj.Manual):
    definition = """
    amp_type:      varchar(32)
    ---
    amp_brand='':  varchar(64)
    amp_model='':  varchar(64)
    """


@schema
class DaqSystemType(dj.Lookup):
    definition = """
    daq_name:       varchar(64)
    ---
    daq_notes='':   varchar(255)
    """
    contents = [
        ['National Instrument PXI', ''],
        ['National Instrument PCI', ''],
        ['Vidrio hardware', '']
    ]


@schema
class AcquisitionSoftware(dj.Lookup):
    definition = """
    acq_software:  varchar(64)
    acq_version:   varchar(16)
    """


@schema
class PmtInstallation(dj.Manual):
    definition = """
    -> Pmt
    pmt_install_date:      date
    ---
    -> Channel
    installation_notes='':   varchar(512)
    """


@schema
class DichroicMirrorStatus(dj.Manual):
    definition = """
    -> Microscope
    mirror_config_date:   date
    """

    class Mirror(dj.Part):
        definition = """
        -> master
        mirror_location:  enum('excitation path', 'detection box')
        ---
        -> DichroicMirrorType
        """


@schema
class FilterStatus(dj.Manual):
    definition = """
    -> Microscope
    filter_config_date:   date
    """

    class MultiPhotonFilter(dj.Part):
        definition = """
        # filter removing the near infrared lights and passing the whole visible spectrum
        -> master
        ---
        -> FilterType
        """

    class PmtFilter(dj.Part):
        definition = """
        # filter removing the near infrared lights and passing the whole visible spectrum
        -> master
        -> Channel
        ---
        -> FilterType
        """


@schema
class ObjectiveStatus(dj.Manual):
    definition = """
    -> Microscope
    objective_config_date:  date
    """

    class Objective(dj.Part):
        definition = """
        -> master
        objective_location:  varchar(32)
        ---
        -> ObjectiveLensType
        """


@schema
class PreAmplifierStatus(dj.Manual):
    definition = """
    -> Microscope
    amp_config_date:     date
    """

    class PreAmplifier(dj.Part):
        definition = """
        -> master
        -> Channel
        ---
        -> PreAmplifierType
        amp_gain:           float
        """


@schema
class DaqStatus(dj.Manual):
    definition = """
    -> Microscope
    daq_config_date:   date
    ---
    -> DaqSystemType
    """


@schema
class AcquisitionSoftwareStatus(dj.Manual):
    definition = """
    -> Microscope
    acq_config_date:   date
    ---
    -> AcquisitionSoftware
    """

@schema
class PsfStackData(dj.Manual):
    definition = """
    -> Microscope
    psf_date                    : date
    stack_num                   : smallint                      # stack number for that day
    ---
    stack_data                  : longblob
    """


@schema
class PsfStackStats(dj.Manual):
    definition = """
    -> PsfStackData
    ---
    who                         : varchar(63)                   # who acquired the data
    lens_mag                    : decimal(5,2)                  # lens magnification
    na                          : decimal(3,2)                  # numerical aperture of the objective lens
    fov_x                       : float                         # (um) field of view at selected magnification
    fov_y                       : float                         # (um) field of view at selected magnification
    mean_sigma_x                : float                         # full width at half magnitude, average over beads
    mean_sigma_y                : float                         # full width at half magnitude, average over beads
    mean_sigma_z                : float                         # full width at half magnitude, average over beads
    wavelength                  : smallint                      # (nm) laser wavelength
    mwatts                      : decimal(3,1)                  # mwatts out of objective
    path                        : varchar(1023)                 # file path
    full_filename               : varchar(255)                  # file name
    note                        : varchar(1023)                 # any other information
    beadstack_ts=CURRENT_TIMESTAMP: timestamp                   # automatic
    """

    class BeadPsf(dj.Part):
        definition = """
        -> master
        bead_idx                    : smallint                      # bead number in the stack
        ---
        dx                          : float                         # (um) pixel pitch
        dy                          : float                         # (um) pixel pitch
        dz                          : float                         # (um) slice pitch
        psf_stack                   : longblob                      # stack embedding the bead
        center_x                    : float                         # pixel coordinate
        center_y                    : float                         # pixel coordinate
        center_z                    : float                         # pixel coordinate
        base_x                      : float                         # base intensity in x projection
        base_y                      : float                         # base intensity in y projection
        base_z                      : float                         # base intensity in z projection
        amplitude_x                 : float                         # bead intensity in x projection
        amplitude_y                 : float                         # bead intensity in y projection
        amplitude_z                 : float                         # bead intensity in z projection
        sigma_x                     : float                         # full width at half magnitude
        sigma_y                     : float                         # full width at half magnitude
        sigma_z                     : float                         # full width at half magnitude
        xi                          : longblob                      # (um) x-projection coordinates
        yi                          : longblob                      # (um) y-projection coordinates
        zi                          : longblob                      # (um) z-projection coordinates
        proj_x                      : longblob                      # x projection
        proj_y                      : longblob                      # x projection
        proj_z                      : longblob                      # x projection
        """


@schema
class FlatFieldMeasurement(dj.Imported):
    definition = """
    -> Channel
    flat_field_measure_time:   datetime
    ---
    average_heatmap:     blob
    """


@schema
class PercentagePower(dj.Imported):
    definition = """
    -> Microscope
    power_measure_time:   datetime
    ---
    laser_wavelength:     float
    percentage: blob
    power:      blob
    """


@schema
class UnitsPerPhoton(dj.Imported):
    definition = """
    -> Channel
    unit_photon_measure_time:       datetime
    ---
    unit_photon_pmt_gain:           float
    units_per_photon:               float
    """


@schema
class NoiseFrequencySpectrum(dj.Imported):
    definition = """
    -> Channel
    noise_freq_measure_time:    datetime
    ---
    laser_wavelength:   float
    freq:               blob
    intensity:          blob
    """


@schema
class PowerLawFluoresLaser(dj.Imported):
    definition = """
    -> Channel
    power_law_time:  datetime
    ---
    fluores_value:      blob
    laser_power:        blob
    power_coefficient:  float
    """

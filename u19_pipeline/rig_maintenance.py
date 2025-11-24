#!/bin/env python

import datajoint as dj

prefix = dj.config["custom"]["database.prefix"]

schema = dj.schema(prefix + "rig_maintenance")


def connect_mod(x):
    return dj.VirtualModule(x, prefix + x)


lab = connect_mod("lab")


@schema
class MaintenanceType(dj.Lookup):
    definition = """
    # Defines the different types of maintenance and their required intervals
    maintenance_type         : varchar(100)                 # Name of maintenance type
    ---
    description              : varchar(255)                 # Description of maintenance
    interval_days            : int                           # Required interval in days
    notification_window       : int                           # Number of days to notify before maintenance due date
    number_of_lines          : int                           #  Show this dependent on how many lines the rig has 0. Applies to all rigs 1. Applies to rigs with one line 2. Applies to rigs with two lines
    """
    contents = [
        ["Replacing lines", "Replacing water/reward lines", 30],
        ["Replacing spheres", "Replacing spheres in the rig", 60],
        ["General Calibration", "General calibration (always after changing lines or solenoids)", 30],
        ["Replacing solenoid valve", "Replacing solenoid valve", 365],
        ["Deep Clean solenoid valve", "Deep cleaning of solenoid valve", 30],
        ["Replacing reward lines connectors", "Replacing reward lines connectors", 180],
    ]


@schema
class RigMaintenance(dj.Manual):
    definition = """
    # Records of maintenance performed on rigs
    -> lab.Location                          # Rig location
    -> MaintenanceType                       # Type of maintenance performed
    maintenance_date         : date          # Date when maintenance was performed
    ---
    -> lab.User                              # User who performed the maintenance
    maintenance_notes=''     : varchar(1024) # Additional notes about the maintenance
    maintenance_time=CURRENT_TIMESTAMP : timestamp  # Timestamp when record was created
    """
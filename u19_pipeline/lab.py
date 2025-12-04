"""This module defines tables in the schema U19_lab"""

import os
import pathlib
import sys

import datajoint as dj
import numpy as np
import pandas as pd

from u19_pipeline.utility import is_this_spock

schema = dj.schema(dj.config["custom"]["database.prefix"] + "lab")


@schema
class Lab(dj.Lookup):
    definition = """
    lab                  : varchar(16)                  # name of lab
    ---
    institution          : varchar(64)
    address              : varchar(128)
    time_zone            : varchar(32)
    pi_name              : varchar(64)
    """
    contents = [
        [
            "tanklab",
            "Princeton",
            "Princeton Neuroscience Institute, Princeton University Princeton, NJ 08544",
            "America/New_York",
            "D. W. Tank",
        ],
        [
            "wittenlab",
            "Princeton",
            "Princeton Neuroscience Institute, Princeton University Princeton, NJ 08544",
            "America/New_York",
            "I. Witten",
        ],
        [
            "wanglab",
            "Princeton",
            "Neuroscience Institute, Princeton University Princeton, NJ 08544",
            "America/New_York",
            "S. Wang",
        ],
    ]


@schema
class Location(dj.Lookup):
    definition = """
    # The physical location at which an session is performed or appliances
    # are located. This could be a room, a rig, or a bench
    location             : varchar(32)
    ---
    location_description="" : varchar(255)
    """
    contents = [
        ["Bezos2", ""],
        ["Bezos3", ""],
        ["BezosMeso", ""],
        ["TrainVR1", ""],
        ["floater", ""],
        ["vivarium", ""],
        ["pni-171jppw32", ""],
        ["pni-174cr4jk2", ""],
        ["valhalla", ""],
    ]


@schema
class Project(dj.Lookup):
    definition = """
    project              : varchar(64)
    ---
    project_description="" : varchar(255)
    """
    contents = [["behavioral task", ""], ["accumulation of evidence", ""]]


@schema
class MobileCarrier(dj.Lookup):
    definition = """
    mobile_carrier       : varchar(16)                  # allowed mobile carries
    """
    contents = zip(
        [
            "alltel",
            "att",
            "boost",
            "cingular",
            "cingular2",
            "cricket",
            "metropcs",
            "nextel",
            "sprint",
            "tmobile",
            "tracfone",
            "uscellular",
            "verizon",
            "virgin",
        ]
    )


@schema
class User(dj.Manual):
    definition = """
    user_id              : varchar(32)                  # username
    ---
    user_nickname        : varchar(32)                  # same as netID for new users, for old users, this is used in the folder name etc.
    full_name=null       : varchar(32)                  # first name
    email=null           : varchar(64)                  # email address
    phone=null           : varchar(12)                  # phone number
    -> [nullable] MobileCarrier
    slack=null           : varchar(32)                  # slack username
    contact_via          : enum('Slack','text','Email')
    presence             : enum('Available','Away')
    primary_tech="N/A"   : enum('yes','no','N/A')
    tech_responsibility="N/A" : enum('yes','no','N/A')
    day_cutoff_time      : blob
    slack_webhook=null   : varchar(255)
    watering_logs=null   : varchar(255)
    """


@schema
class UserSecondaryContact(dj.Manual):
    definition = """
    -> User
    ---
    -> User.proj(secondary_contact="user_id")
    """


@schema
class DutyRoaster(dj.Manual):
    definition = """
    duty_roaster_date    : date                         # date from which this assignment is valid.
    ---
    -> User.proj(monday_duty="user_id")
    -> User.proj(tuesday_duty="user_id")
    -> User.proj(wednesday_duty="user_id")
    -> User.proj(thursday_duty="user_id")
    -> User.proj(friday_duty="user_id")
    -> User.proj(saturday_duty="user_id")
    -> User.proj(sunday_duty="user_id")
    """


@schema
class UserLab(dj.Manual):
    definition = """
    -> User
    ---
    -> Lab
    """


@schema
class ProjectUser(dj.Manual):
    definition = """
    -> Project
    -> User
    """


@schema
class Protocol(dj.Lookup):
    definition = """
    protocol             : varchar(16)                  # protocol number
    ---
    reference_weight_pct=null : float                        # percentage of initial allowed
    protocol_description="" : varchar(255)                 # description
    active : int # whether the protocol is active (active = 1, inactive = 0 )
    """
    contents = [["1910", 0.8, "Tank Lab protocol"]]


@schema
class UserProtocol(dj.Lookup):
    definition = """
    -> User
    -> Protocol
    """


@schema
class LabManager(dj.Lookup):
    definition = """
    -> lab_man_index
    ---
    -> User.proj(lab_manager="user_id")
    -> Lab
    """


@schema
class NotificationSettings(dj.Manual):
    definition = """
    notification_settings_date : date                         # date from which this is valid.
    ---
    max_response_time    : float                        # in minutes, e.g. 30
    change_cutoff_time   : blob                         # time of day, e.g. [5,0] (=5pm)
    weekly_digest_day    : varchar(5)                   # weekday, e.g. Mon
    weekly_digest_time   : blob                         # time of day, e.g. [5,0] (=5pm)
    """


@schema
class Path(dj.Lookup):
    definition = """
    global_path          : varchar(255)                 # global path name
    system               : enum('windows','mac','linux')
    ---
    local_path           : varchar(255)                 # local computer path
    net_location         : varchar(255)                 # location on the network
    description=null     : varchar(255)
    """

    contents = [
        ["/Bezos-center", "windows", "Y:", r"\\cup.pni.princeton.edu\Bezos-center", ""],
        ["/Bezos-center", "mac", "/Volumes/Bezos-center", "//cup.pni.princeton.edu/Bezos-center", ""],
        ["/Bezos-center", "linux", "/mnt/Bezos-center", "//cup.pni.princeton.edu/Bezos-center", ""],
        ["/braininit", "windows", "Z:", r"\\cup.pni.princeton.edu\braininit", ""],
        ["/braininit", "mac", "/Volumes/braininit", "//cup.pni.princeton.edu/Bezos-center", ""],
        ["/braininit", "linux", "/mnt/braininit", "//cup.pni.princeton.edu/Bezos-center", ""],
    ]

    def get_local_path(self, path, local_os=None):
        # determine local os
        if local_os is None:
            local_os = sys.platform
            local_os = local_os[: (min(3, len(local_os)))]
        if local_os.lower() == "glo":
            local = 0
            home = "~"

        elif local_os.lower() == "lin":
            local = 1
            home = os.environ["HOME"]

        elif local_os.lower() == "win":
            local = 2
            home = os.environ["HOME"]

        elif local_os.lower() == "dar":
            local = 3
            home = "~"

        else:
            raise NameError("unknown OS")

        path = path.replace(os.path.sep, "/")
        path = path.replace("~", home)

        globs = dj.U("global_path") & self
        systems = ["linux", "windows", "mac"]

        mapping = [[], []]

        for iglob, glob in enumerate(globs.fetch("KEY")):
            mapping[iglob].append(glob["global_path"])
            for system in systems:
                mapping[iglob].append((self & glob & {"system": system}).fetch1("local_path"))

        mapping = np.asarray(mapping)

        for i in range(len(globs)):
            for j in range(len(systems)):
                n = len(mapping[i, j])
                if j != local and path[:n] == mapping[i, j][:n]:
                    path = os.path.join(mapping[i, local], path[n + 1 :])
                    break

        if os.path.sep == "\\" and local_os.lower() != "glo":
            path = path.replace("/", "\\")

        else:
            path = path.replace("\\", "/")

        return path

    def get_local_path2(self, bucket_path):
        local_os = sys.platform
        local_os = local_os[: (min(3, len(local_os)))]

        if local_os.lower() == "lin":
            system = "linux"
        elif local_os.lower() == "win":
            system = "windows"
        elif local_os.lower() == "dar":
            system = "mac"

        # Get path table from db and filter by OS
        path_df = self.get_path_table()
        path_df = path_df[path_df["system"] == system]

        # Search in path which of the main buckets we are referring from
        path_df["idx_global_path"] = path_df["global_path"].apply(lambda x: bucket_path.find(x))
        path_df = path_df[path_df["idx_global_path"] != -1]
        path_df = path_df[path_df["idx_global_path"] == path_df["idx_global_path"].min()].squeeze()
        path_df = path_df.to_dict()

        # Remove bucket "base" dir from path
        bucket_base_dir = path_df["bucket_path"]

        if bucket_path.find("/mnt/cup/") != -1:
            extra_bucket_dir = bucket_path.replace(bucket_base_dir + "/", "")
        else:
            extra_bucket_dir = bucket_path.replace(path_df["global_path"] + "/", "")

        if extra_bucket_dir[0] == "/":
            extra_bucket_dir = extra_bucket_dir[1:]

        # If we are in spock already directory is the bucket_path column
        if is_this_spock():
            baseDir = path_df["bucket_path"]
        elif system == "windows":
            # For pc the accesible path is the net_location field
            baseDir = path_df["net_location"]

            # Correct extra bucket dir to adjust windows filesep
            extra_bucket_dir = extra_bucket_dir.replace("/", "\\")

        else:
            # For mac and linux the accesible path is the local_path field
            baseDir = path_df["local_path"]

        format_dir = pathlib.PurePath(baseDir, extra_bucket_dir)
        format_dir = pathlib.Path(format_dir)

        return format_dir

    def get_path_table(self):
        """
        get path table as a dataframe
        path table serves to corresponding paths between local system and bucket cloud.
        """
        path_df = pd.DataFrame(self.fetch())
        path_df["global_path"] = path_df["global_path"].str.replace("/", "")
        return path_df


@schema
class EndpointNotification(dj.Lookup):
    definition = """
    email                 : varchar(64)                  # email address
    ---
    """


@schema
class VideoType(dj.Lookup):
    definition = """
    # Types of video that can be acquired in a behavior session
    video_type              : varchar(64)
    ---
    video_type_description  : varchar(2555)
    """
    contents = [["pupillometry", ""], ["behavior", ""]]


@schema
class SlackGroups(dj.Lookup):
    definition = """
    group_name: varchar(64)
    ---
    group_id: varchar(255)
    """
    contents = []


@schema
class SlackWebhooks(dj.Lookup):
    definition = """
    webhook_name            : varchar(64)
    ---
    webhook_url             : varchar(255)
    """


@schema
class DjCustomVariables(dj.Lookup):
    definition = """
    custom_variable         : varchar(64)
    index                   : int
    ---
    value                   : varchar(255)
    """


@schema
class DjStores(dj.Lookup):
    definition = """
    store_name              : varchar(64)
    ---
    protocol                : varchar(32)
    location                : varchar(255)
    """

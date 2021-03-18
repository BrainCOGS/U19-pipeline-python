"""This module defines tables in the schema U19_reference."""


import datajoint as dj

schema = dj.schema(dj.config['database.prefix'] + 'reference')


@schema
class BrainArea(dj.Lookup):
    definition = """
    brain_area           : varchar(16)
    ---
    area_full_name=""    : varchar(128)
    """
    contents = [
        ['Hippocampus', ''],
        ['Striatum', ''],
        ['PPC', 'Posterior Parietal Cortex'],
        ['EC', 'Entorhinal Cortex']
    ]


@schema
class VirusSource(dj.Lookup):
    definition = """
    virus_source         : varchar(32)
    """
    contents = zip([
        'UNC',
        'UPenn',
        'Addgene',
        'MIT',
        'Stanford',
        'Princeton',
        'custom'
    ])


@schema
class VirusType(dj.Lookup):
    definition = """
    virus_type           : varchar(64)
    """
    contents = zip([
        'AAV',
        'Rabies',
        'Psedotyped rabies',
        'Lenti'
    ])


@schema
class Virus(dj.Lookup):
    definition = """
    virus_nickname       : varchar(16)
    virus_id             : int                          # virus count of the same type
    ---
    virus_fullname       : varchar(64)
    -> VirusType
    -> VirusSource
    catlog_number=""     : varchar(64)
    titer                : float                        # 10^12 geno copies per mL
    date_came_in=null    : date
    virus_description="" : varchar(512)
    """
    contents = [
        ['jRCaMP1a', 1, 'AAV1.Syn.NES.jRCaMP1a.WPRE.SV40', 'AAV', 'Upenn', '', 33.6, None, ''],
        ['GCaMP6f', 1, 'AAV1.Syn.GCaMP6f.WPRE.SV40', 'AAV', 'UPenn', '', 26.5, None, ''],
        ['Syn.RFP', 1, 'AAV5.hSyn.TurboRFP.WPRE.rBG', 'AAV', 'UPenn', '', 44, None, ''],
        ['GFAP.GFP', 1, 'AAV5.GFAP.eGFP.WPRE.hGH', 'AAV', 'UPenn', '', 10.6, None, ''],
        ['CamKII.GFP', 1, 'AAV9.CamKII0.4.eGFP.WPRE.rBG', 'AAV', 'UPenn', '', 34.9, None, ''],
        ['Syn.RFP', 1, 'AAV9.hSyn.TurboRFP.WPRE.rBG', 'AAV', 'UPenn', '', 66.4, None, ''],
        ['ChR2(H134R)-YFP', 1, 'AAV9.hSyn.hChR2(H134R)-eYFP.WPRE.hGH', 'AAV', 'UPenn', '', 33.9, None, ''],
        ['Chronos-GFP', 1, 'AAV9.Syn.Chronos-GFP.WPRE.bGH', 'AAV', 'UPenn', '', 35.1, None, '']
    ]


@schema
class Template(dj.Lookup):
    definition = """
    # Template for the information of each column
    template_name        : varchar(64)                  # field name, such as 'Cage ID'
    ---
    original_field_names : longblob                     # cell array for original field names
    database_field_names : longblob                     # cell array for database field names
    """


    class RightNow(dj.Part):
        definition = """
        # ActionItems template for the information of each column
        -> Template
        data                 : varchar(64)
        ---
        plot_index           : int                          # index of where to plot in GUI
        """


    class Genotype(dj.Part):
        definition = """
        # ActionItems template for the information of each column
        -> Template
        value                : varchar(64)
        ---
        plot_index           : int                          # index of where to plot in GUI
        """


    class DailyInfo(dj.Part):
        definition = """
        # Daily Info template for the information of each column
        -> Template
        field                : varchar(64)                  # field name
        ---
        description=""       : varchar(255)                 # description of this field
        grouping=""          : varchar(16)
        identifier           : varchar(64)
        data                 : blob                         # cell array of the data types
        mandatory            : enum('yes','no')
        is_dynamic           : enum('yes','no')             # isDynamic for the original template
        is_filter            : enum('yes','no')             # isFilter for the original template
        is_trials            : enum('yes','no')             # isTrials for the original template
        plot_index           : int                          # index of where to plot in GUI
        """


    class Animal(dj.Part):
        definition = """
        # Animal template for the information of each column
        -> Template
        field                : varchar(64)                  # field name, such as 'Cage ID'
        ---
        description          : varchar(255)                 # description of this field
        grouping=""          : varchar(16)
        identifier           : varchar(64)                  # 'cage'
        data                 : blob                         # cell array of the data types
        future_plans         : enum('yes','no')             # futurePlans for the original template
        mandatory            : enum('yes','no')
        is_filter            : enum('yes','no')             # isFilter for the original template
        plot_index           : int                          # index of where to plot in GUI
        """


    class ActionItems(dj.Part):
        definition = """
        # ActionItems template for the information of each column
        -> Template
        value                : varchar(64)
        ---
        plot_index           : int                          # index of where to plot in GUI
        """

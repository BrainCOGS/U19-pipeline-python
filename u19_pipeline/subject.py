"""This module defines tables in the schema U19_subject"""


import datajoint as dj
from . import lab

schema = dj.schema(dj.config['database.prefix'] + 'subject')


@schema
class Species(dj.Lookup):
    definition = """
    binomial             : varchar(32)                  # binomial
    ---
    species_nickname     : varchar(32)                  # nickname
    """
    contents = [
        ['Mus musculus', 'Laboratory mouse']
    ]


@schema
class Source(dj.Lookup):
    definition = """
    source               : varchar(32)                  # name of source
    ---
    source_description="" : varchar(255)
    """
    contents = [
        ['Jax Lab', ''],
        ['Princeton', ''],
        ['Allen Institute', '']
    ]


@schema
class Strain(dj.Lookup):
    definition = """
    strain_name          : varchar(32)                  # strain name
    ---
    strain_description="" : varchar(255)                 # description
    """
    contents = [
        ['C57BL6/J', '']
    ]


@schema
class Allele(dj.Lookup):
    definition = """
    allele               : varchar(63)                  # informal name
    ---
    standard_name=""     : varchar(255)                 # standard name
    -> Source
    original_allele_source : varchar(255)                 # original source of the allele
    allele_description="" : varchar(1023)
    """
    contents = [
        ['Thy1-GP5.3', 'Thy1-GCaMP6f', 'Jax Lab', 'David W. Tank, Princeton University', ''],
        ['Ai93', '(TITL-GCaMP6f)-D', 'Jax Lab', 'Allen Institute', ''],
        ['Ai148', '(TIT2L-GC6f-ICL-tTA2)-D', 'Jax Lab', 'Allen Institute', ''],
        ['Thy1-YFP', '', 'Jax Lab', 'Joshua R Sanes, Harvard University', ''],
        ['VGAT-ChR2-EYFP', 'Slc32a1-COP4*H134R/EYFP', 'Jax Lab', 'Guoping Feng, Massachusetts Institute of Technology', ''],
        ['Emx1-Cre', 'Emx-IRES-Cre', 'Jax Lab', 'Kevin R. Jones, University of Colorado- Boulder', ''],
        ['D1-Cre', 'Drd1-cre', 'Jax Lab', 'Ming Xu, University of Chicago', ''],
        ['D2-Cre', 'Drd2-cre', 'Jax Lab', 'Unknown', ''],
        ['DAT-IRES-Cre', 'Slc6a3tm1.1(cre)Bkmn', 'Jax Lab', 'Cristina M Backman, National Institute on Drug Abuse (NIH)', ''],
        ['Slc17a7-IRES2-Cre', '', 'Jax Lab', 'Allen Institute', '']
    ]


@schema
class SequenceType(dj.Lookup):
    definition = """
    sequence_type        : varchar(63)
    ---
    seq_type_description="" : varchar(255)
    """
    contents = [
        ['calcium sensor', ''],
        ['optogenetics', ''],
        ['promoter', ''],
        ['recombinase', ''],
        ['fluorescent protein', '']
    ]


@schema
class Sequence(dj.Lookup):
    definition = """
    sequence             : varchar(63)                  # informal name
    ---
    -> SequenceType
    base_pairs=""        : varchar(1023)                # base pairs
    sequence_description="" : varchar(255)
    """
    contents = [
        ['GCaMP6f', 'calcium sensor', '', ''],
        ['GCaMP6s', 'calcium sensor', '', ''],
        ['ChR2', 'optogenetics', '', ''],
        ['EYFP', 'fluorescent protein', '', ''],
        ['Thy1', 'promoter', '', ''],
        ['Emx1', 'promoter', '', ''],
        ['Cre', 'recombinase', '', ''],
        ['D1', 'promoter', '', 'dopamine receptor type 1'],
        ['D2', 'promoter', '', 'dopamine receptor type 2']
    ]


@schema
class AlleleSequence(dj.Lookup):
    definition = """
    -> Allele
    -> Sequence
    """
    contents = [
        ['Thy1-YFP', 'Thy1'],
        ['Thy1-YFP', 'EYFP'],
        ['Thy1-GP5.3', 'GCaMP6f'],
        ['VGAT-ChR2-EYFP', 'EYFP'],
        ['VGAT-ChR2-EYFP', 'ChR2'],
        ['Emx1-Cre', 'Emx1'],
        ['Emx1-Cre', 'Cre'],
        ['D1-Cre', 'D1'],
        ['D1-Cre', 'Cre'],
        ['D2-Cre', 'D2'],
        ['D2-Cre', 'Cre'],
        ['DAT-IRES-Cre', 'Cre'],
        ['Slc17a7-IRES2-Cre', 'Cre']
    ]


@schema
class Line(dj.Lookup):
    definition = """
    line                 : varchar(128)                 # name
    ---
    -> Species
    -> Strain
    line_description=""  : varchar(2048)                # description
    target_phenotype=""  : varchar(255)                 # target phenotype
    is_active=1          : tinyint                      # is active
    """
    contents = [
        ['Unknown', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['C57BL6/J', 'Mus musculus', 'C57BL6/J', 'wild-type mice', '', 1],
        ['VGAT-ChR2-EYFP', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['Ai93-Emx1', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['Thy1-GP5.3', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['Thy1-YFP', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['DAT-IRES-CRE', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['DAT-Ai148', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['Slc17a7-Ai148', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['D1-CRE', 'Mus musculus', 'C57BL6/J', '', '', 1],
        ['D2-CRE', 'Mus musculus', 'C57BL6/J', '', '', 1]
    ]


@schema
class LineAllele(dj.Lookup):
    definition = """
    -> Line
    -> Allele
    """
    contents = [
        ['VGAT-ChR2-EYFP', 'VGAT-ChR2-EYFP'],
        ['Ai93-Emx1', 'Ai93'],
        ['Ai93-Emx1', 'Emx1-Cre'],
        ['Thy1-GP5.3', 'Thy1-GP5.3'],
        ['Thy1-YFP', 'Thy1-YFP'],
        ['DAT-IRES-CRE', 'DAT-IRES-Cre'],
        ['DAT-Ai148', 'DAT-IRES-Cre'],
        ['DAT-Ai148', 'Ai148'],
        ['Slc17a7-Ai148', 'Ai148'],
        ['Slc17a7-Ai148', 'Slc17a7-IRES2-Cre'],
        ['D1-CRE', 'D1-Cre'],
        ['D2-CRE', 'D2-Cre']
    ]


@schema
class Subject(dj.Manual):
    definition = """
    subject_fullname     : varchar(64)                  # username_mouse_nickname
    ---
    subject_nickname     : varchar(16)
    -> lab.User
    genomics_id=null     : int                          # number from the facility
    sex="Unknown"        : enum('Male','Female','Unknown') # sex
    dob=null             : date                         # birth date
    head_plate_mark=null : blob                         # little drawing on the head plate for mouse identification
    -> lab.Location
    -> [nullable] lab.Protocol
    -> [nullable] Line
    subject_description="" : varchar(255)               # description
    initial_weight=null  : float
    """


@schema
class ActItem(dj.Lookup):
    definition = """
    act_item             : varchar(64)                  # possible act item
    """
    contents = zip(['Post-op painkillers',
                    'Post-op monitoring',
                    'Acclimatize to humans',
                    'Topical antibiotics for headplate scab',
                    'Needs fattening'])


@schema
class SubjectActionManual(dj.Manual):
    definition = """
    -> Subject                                                  # For manual actions; links subject to an actionItem from the list
    -> ActItem
    """

@schema
class SubjectActionAutomatic(dj.Manual):
    definition = """
    -> Subject
    notification_date        : datetime                         # datetime when notification was automatically generated
    ---
    notification_message     : varchar(255)                     # Notification message e.g. low bodyweight warning
    """

@schema
class SubjectProject(dj.Manual):
    definition = """
    -> Subject
    -> lab.Project
    """


@schema
class Weaning(dj.Manual):
    definition = """
    -> Subject
    ---
    wean_date            : date
    """


@schema
class Death(dj.Manual):
    definition = """
    -> Subject
    ---
    death_date           : date
    """


@schema
class Cage(dj.Lookup):
    definition = """
    cage                 : char(8)                      # name of a cage
    ---
    -> lab.User.proj(cage_owner="user_id")
    """


@schema
class CagingStatus(dj.Manual):
    definition = """
    -> Subject
    ---
    -> Cage
    """

@schema
class HealthStatus(dj.Manual):
    definition = """
    -> Subject
    status_date          : date
    ---
    normal_behavior=1    : tinyint
    bcs=-1               : tinyint                      # Body Condition Score, from 1 (emaciated i.e. very malnourished) to 5 (obese), 3 being normal
    activity=-1          : tinyint                      # score from 0 (moves normally) to 3 (does not move) -1 unknown
    posture_grooming=-1  : tinyint                      # score from 0 (normal posture + smooth fur) to 3 (hunched + scruffy) -1 unknown
    eat_drink=-1         : tinyint                      # score from 0 (normal amounts of feces and urine) to 3 (no evidence of feces or urine) -1 unknown
    turgor=-1            : tinyint                      # score from 0 (skin retracts within 0.5s) to 3 (skin retracts in more than 2 s) -1 unknown
    comments=null        : varchar(255)
    """


    class Action(dj.Part):
        definition = """
        -> HealthStatus
        action_id            : tinyint                      # id of the action
        ---
        action               : varchar(255)
        """


@schema
class BreedingPair(dj.Manual):
    definition = """
    breeding_pair        : varchar(63)                  # name
    ---
    -> Line
    -> Subject
    bp_description=""    : varchar(2047)                # description
    bp_start_date=null   : date                         # start date
    bp_end_date=null     : date
    """


@schema
class Litter(dj.Manual):
    definition = """
    litter               : varchar(63)
    ---
    -> BreedingPair
    -> Line
    litter_descriptive_name="" : varchar(255)                 # descriptive name
    litter_description="" : varchar(255)                 # description
    litter_birth_date=null : date
    """


@schema
class LitterSubject(dj.Manual):
    definition = """
    -> Subject
    ---
    -> Litter
    """


@schema
class GenotypeTest(dj.Manual):
    definition = """
    -> Subject
    -> Sequence
    genotype_test_id     : varchar(63)
    ---
    test_result          : enum('Present','Absent')
    """


@schema
class Zygosity(dj.Manual):
    definition = """
    -> Subject
    -> Allele
    ---
    zygosity             : enum('Present','Absent','Homozygous','Heterozygous')
    """

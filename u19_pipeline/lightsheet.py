import datajoint as dj

schema = dj.schema('U19_lightsheet') 


@schema
class User(dj.Lookup):
    definition = """
    # Users of the light sheet microscope
    username : varchar(20)      # netid of user in the lab
    ---
    princeton_email       : varchar(50)
    """
    
@schema
class Experiment(dj.Manual):
    definition = """ # Experiments performed using the light sheet microscope
    experiment_id                :   smallint auto_increment    # allowed here are sql datatypes.
    ----
    -> User 
    labname                      :   varchar(50)
    correspondence_email = ''    :   varchar(100)
    title                        :   varchar(100)
    description                  :   varchar(250)
    notes = ""                   :   varchar(1000)
    species                      :   varchar(50)
    clearing_protocol            :   enum("iDISCO+_immuno","iDISCO abbreviated clearing","iDISCO abbreviated clearing (rat)","uDISCO","iDISCO_EdU")
    clearing_progress            :   enum("incomplete","in progress","complete")
    antibody1                    :   varchar(100)
    antibody2                    :   varchar(100)
    image_resolution             :   enum("1.3x","4x")
    channel488                   :   enum("","registration","cell_detection","probe_detection","injection_detection")                    
    channel555                   :   enum("","registration","cell_detection","probe_detection","injection_detection")
    channel647                   :   enum("","registration","cell_detection","probe_detection","injection_detection")
    channel790                   :   enum("","registration","cell_detection","probe_detection","injection_detection")
    """  

@schema
class IdiscoPlusClearing(dj.Manual): # dj.Manual is one of the 4 datajoint table types - Manual corresponds to externally inputted data
    definition = """ # Periodic calibration data of the light sheet microscope
    -> Experiment              # experiment_id, the primary key from the Experiment() table
    ----
    -> User                    # username, the researcher's netid from the User() table
    clearer                                                  :   varchar(20)   # the netid of the person who did the clearing
    exp_notes = ""                                           :   varchar(500)  # Note anything unusual that happened during experiment that could affect clearing
    perfusion_date = NULL                                    :   date 
    time_dehydr_pbs_wash1 = NULL                             :   datetime
    dehydr_pbs_wash1_notes = ""                              :   varchar(250)
    time_dehydr_pbs_wash2 = NULL                             :   datetime
    dehydr_pbs_wash2_notes = ""                              :   varchar(250)
    time_dehydr_pbs_wash3 = NULL                             :   datetime
    dehydr_pbs_wash3_notes = ""                              :   varchar(250)
    time_dehydr_methanol_20percent_wash1 = NULL                 :   datetime
    dehydr_methanol_20percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_40percent_wash1 = NULL                 :   datetime
    dehydr_methanol_40percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_60percent_wash1 = NULL                 :   datetime
    dehydr_methanol_60percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_80percent_wash1 = NULL                 :   datetime
    dehydr_methanol_80percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_100percent_wash1 = NULL                :   datetime
    dehydr_methanol_100percent_wash1_notes = ""                 :   varchar(250)
    time_dehydr_methanol_100percent_wash2 = NULL                :   datetime
    dehydr_methanol_100percent_wash2_notes = ""                 :   varchar(250)
    time_dehydr_h202_wash1 = NULL                            :   datetime
    dehydr_h202_wash1_notes = ""                             :   varchar(250)
    time_rehydr_methanol_100percent_wash1 = NULL                :   datetime
    rehydr_methanol_100percent_wash1_notes = ""                 :   varchar(250)
    time_rehydr_methanol_80percent_wash1 = NULL                 :   datetime
    rehydr_methanol_80percent_wash1_notes = ""                  :   varchar(250)
    time_rehydr_methanol_60percent_wash1 = NULL                 :   datetime
    rehydr_methanol_60percent_wash1_notes = ""                  :   varchar(250)
    time_rehydr_methanol_40percent_wash1 = NULL                 :   datetime
    rehydr_methanol_40percent_wash1_notes = ""                  :   varchar(250)
    time_rehydr_methanol_20percent_wash1 = NULL                 :   datetime
    rehydr_methanol_20percent_wash1_notes = ""                  :   varchar(250)
    time_rehydr_pbs_wash1 = NULL                             :   datetime
    rehydr_pbs_wash1_notes = ""                              :   varchar(250)
    time_rehydr_sodium_azide_wash1 = NULL                    :   datetime
    rehydr_sodium_azide_wash1_notes = ""                     :   varchar(250)
    time_rehydr_sodium_azide_wash2 = NULL                    :   datetime
    rehydr_sodium_azide_wash2_notes = ""                     :   varchar(250)
    time_rehydr_glycine_wash1 = NULL                         :   datetime
    rehydr_glycine_wash1_notes = ""                          :   varchar(250)
    time_blocking_start_roomtemp = NULL                      :   datetime
    blocking_start_roomtemp_notes = ""                       :   varchar(250)
    time_blocking_donkey_serum = NULL                        :   datetime
    blocking_donkey_serum_notes = ""                         :   varchar(250)
    time_antibody1_start_roomtemp = NULL                     :   datetime
    antibody1_start_roomtemp_notes = ""                      :   varchar(250)
    time_antibody1_ptwh_wash1 = NULL                         :   datetime
    antibody1_ptwh_wash1_notes = ""                          :   varchar(250)
    time_antibody1_ptwh_wash2 = NULL                         :   datetime
    antibody1_ptwh_wash2_notes = ""                          :   varchar(250)
    time_antibody1_added = NULL                              :   datetime
    antibody1_added_notes = ""                               :   varchar(250)
    time_wash1_start_roomtemp = NULL                         :   datetime
    wash1_start_roomtemp_notes = ""                          :   varchar(250)
    time_wash1_ptwh_wash1 = NULL                             :   datetime
    wash1_ptwh_wash1_notes = ""                              :   varchar(250)
    time_wash1_ptwh_wash2 = NULL                             :   datetime
    wash1_ptwh_wash2_notes = ""                              :   varchar(250)
    time_wash1_ptwh_wash3 = NULL                             :   datetime
    wash1_ptwh_wash3_notes = ""                              :   varchar(250)
    time_wash1_ptwh_wash4 = NULL                             :   datetime
    wash1_ptwh_wash4_notes = ""                              :   varchar(250)
    time_wash1_ptwh_wash5 = NULL                             :   datetime
    wash1_ptwh_wash5_notes = ""                              :   varchar(250)
    time_antibody2_added = NULL                              :   datetime
    antibody2_added_notes = ""                               :   varchar(250)
    time_wash2_start_roomtemp = NULL                         :   datetime
    wash2_start_roomtemp_notes = ""                          :   varchar(250)
    time_wash2_ptwh_wash1 = NULL                             :   datetime
    wash2_ptwh_wash1_notes = ""                              :   varchar(250)
    time_wash2_ptwh_wash2 = NULL                             :   datetime
    wash2_ptwh_wash2_notes = ""                              :   varchar(250)
    time_wash2_ptwh_wash3 = NULL                             :   datetime
    wash2_ptwh_wash3_notes = ""                              :   varchar(250)
    time_wash2_ptwh_wash4 = NULL                             :   datetime
    wash2_ptwh_wash3_notes = ""                              :   varchar(250)
    time_wash2_ptwh_wash5 = NULL                             :   datetime
    wash2_ptwh_wash5_notes = ""                              :   varchar(250)
    time_clearing_methanol_20percent_wash1 = NULL               :   datetime
    clearing_methanol_20percent_wash1_notes = ""                :   varchar(250)
    time_clearing_methanol_40percent_wash1 = NULL               :   datetime
    clearing_methanol_40percent_wash1_notes = ""                :   varchar(250)
    time_clearing_methanol_60percent_wash1 = NULL               :   datetime
    clearing_methanol_60percent_wash1_notes = ""                :   varchar(250)
    time_clearing_methanol_80percent_wash1 = NULL               :   datetime
    clearing_methanol_80percent_wash1_notes = ""                :   varchar(250)
    time_clearing_methanol_100percent_wash1 = NULL              :   datetime
    clearing_methanol_100percent_wash1_notes = ""               :   varchar(250)
    time_clearing_methanol_100percent_wash2 = NULL              :   datetime
    clearing_methanol_100percent_wash2_notes = ""               :   varchar(250)
    time_clearing_dcm_66percent_methanol_33percent = NULL       :   datetime
    clearing_dcm_66percent_methanol_33percent_notes = ""        :   varchar(250)
    time_clearing_dcm_wash1 = NULL                           :   datetime
    clearing_dcm_wash1_notes = ""                            :   varchar(250)
    time_clearing_dcm_wash2 = NULL                           :   datetime
    clearing_dcm_wash2_notes = ""                            :   varchar(250)
    time_clearing_dbe = NULL                                 :   datetime
    clearing_dbe_notes = ""                                  :   varchar(250)
    time_clearing_new_tubes = NULL                           :   datetime
    clearing_new_tubes_notes = ""                            :   varchar(250)
    clearing_notes = ""                                      :   varchar(500)
    """


@schema
class IdiscoAbbreviatedClearing(dj.Manual): # dj.Manual is one of the 4 datajoint table types - Manual corresponds to externally inputted data
    definition = """ # Periodic calibration data of the light sheet microscope
    -> Experiment              # experiment_id, the primary key from the Experiment() table
    ----
    -> User                    # username, the researcher's netid from the User() table
    clearer                                                  :   varchar(20)   # the netid of the person who did the clearing
    exp_notes = ""                                           :   varchar(500)  # Note anything unusual that happened during experiment that could affect clearing
    perfusion_date = NULL                                    :   date 
    time_pbs_wash1 = NULL                                    :   datetime
    pbs_wash1_notes = ""                                     :   varchar(250)
    time_pbs_wash2 = NULL                                    :   datetime
    pbs_wash2_notes = ""                                     :   varchar(250)
    time_pbs_wash3 = NULL                                    :   datetime
    pbs_wash3_notes = ""                                     :   varchar(250)
    time_dehydr_methanol_20percent_wash1 = NULL                 :   datetime
    dehydr_methanol_20percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_40percent_wash1 = NULL                 :   datetime
    dehydr_methanol_40percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_60percent_wash1 = NULL                 :   datetime
    dehydr_methanol_60percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_80percent_wash1 = NULL                 :   datetime
    dehydr_methanol_80percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_100percent_wash1 = NULL                :   datetime
    dehydr_methanol_100percent_wash1_notes = ""                 :   varchar(250)
    time_dehydr_methanol_100percent_wash2 = NULL                :   datetime
    dehydr_methanol_100percent_wash2_notes = ""                 :   varchar(250)
    time_dehydr_dcm_66percent_methanol_33percent = NULL         :   datetime
    dehydr_dcm_66percent_methanol_33percent_notes = ""          :   varchar(250)
    time_dehydr_dcm_wash1 = NULL                             :   datetime
    dehydr_dcm_wash1_notes = ""                              :   varchar(250)
    time_dehydr_dcm_wash2 = NULL                             :   datetime
    dehydr_dcm_wash2_notes = ""                              :   varchar(250)
    time_dehydr_dbe_wash1 = NULL                             :   datetime
    dehydr_dbe_wash1_notes = ""                              :   varchar(250)
    time_dehydr_dbe_wash2 = NULL                             :   datetime
    dehydr_dbe_wash2_notes = ""                              :   varchar(250)
    clearing_notes = ""                                      :   varchar(500)
    """

@schema
class IdiscoAbbreviatedRatClearing(dj.Manual): # dj.Manual is one of the 4 datajoint table types - Manual corresponds to externally inputted data
    definition = """ # Abbreviated Rat clearing table
    -> Experiment              # experiment_id, the primary key from the Experiment() table
    ----
    -> User                    # username, the researcher's netid from the User() table
    clearer                                                  :   varchar(20)   # the netid of the person who did the clearing
    exp_notes = ""                                           :   varchar(500)  # Note anything unusual that happened during experiment that could affect clearing
    perfusion_date = NULL                                    :   date 
    time_pbs_wash1 = NULL                                    :   datetime
    pbs_wash1_notes = ""                                     :   varchar(250)
    time_pbs_wash2 = NULL                                    :   datetime
    pbs_wash2_notes = ""                                     :   varchar(250)
    time_pbs_wash3 = NULL                                    :   datetime
    pbs_wash3_notes = ""                                     :   varchar(250)
    time_dehydr_methanol_20percent_wash1 = NULL                 :   datetime
    dehydr_methanol_20percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_20percent_wash2 = NULL                 :   datetime
    dehydr_methanol_20percent_wash2_notes = ""                  :   varchar(250)
    time_dehydr_methanol_40percent_wash1 = NULL                 :   datetime
    dehydr_methanol_40percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_40percent_wash2 = NULL                 :   datetime
    dehydr_methanol_40percent_wash2_notes = ""                  :   varchar(250)
    time_dehydr_methanol_60percent_wash1 = NULL                 :   datetime
    dehydr_methanol_60percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_80percent_wash1 = NULL                 :   datetime
    dehydr_methanol_80percent_wash1_notes = ""                  :   varchar(250)
    time_dehydr_methanol_80percent_wash2 = NULL                 :   datetime
    dehydr_methanol_80percent_wash2_notes = ""                  :   varchar(250)
    time_dehydr_methanol_100percent_wash1 = NULL                :   datetime
    dehydr_methanol_100percent_wash1_notes = ""                 :   varchar(250)
    time_dehydr_methanol_100percent_wash2 = NULL                :   datetime
    dehydr_methanol_100percent_wash2_notes = ""                 :   varchar(250)
    time_dehydr_h202_wash1 = NULL                            :   datetime
    dehydr_h202_wash1_notes = ""                             :   varchar(250)
    time_dehydr_methanol_100percent_wash3 = NULL                :   datetime
    dehydr_methanol_100percent_wash3_notes = ""                 :   varchar(250)
    time_dehydr_methanol_100percent_wash4 = NULL                :   datetime
    dehydr_methanol_100percent_wash4_notes = ""                 :   varchar(250)
    time_dehydr_methanol_100percent_wash5 = NULL                :   datetime
    dehydr_methanol_100percent_wash5_notes = ""                 :   varchar(250)
    time_dehydr_dcm_66percent_methanol_33percent = NULL         :   datetime
    dehydr_dcm_66percent_methanol_33percent_notes = ""          :   varchar(250)
    time_dehydr_dcm_wash1 = NULL                             :   datetime
    dehydr_dcm_wash1_notes = ""                              :   varchar(250)
    time_dehydr_dcm_wash2 = NULL                             :   datetime
    dehydr_dcm_wash2_notes = ""                              :   varchar(250)
    time_dehydr_dbe_wash1 = NULL                             :   datetime
    dehydr_dbe_wash1_notes = ""                              :   varchar(250)
    time_dehydr_dbe_wash2 = NULL                             :   datetime
    dehydr_dbe_wash2_notes = ""                              :   varchar(250)
    clearing_notes = ""                                      : varchar(500)
    """

@schema
class UdiscoClearing(dj.Manual): # dj.Manual is one of the 4 datajoint table types - Manual corresponds to externally inputted data
    definition = """ # uDISCO clearing table
    -> Experiment              # experiment_id, the primary key from the Experiment() table
    ----
    -> User                    # username, the researcher's netid from the User() table
    clearer                                                  :   varchar(20)   # the netid of the person who did the clearing
    exp_notes = ""                                           :   varchar(500)  # Note anything unusual that happened during experiment that could affect clearing
    perfusion_date = NULL                                    :   date 
    time_dehydr_pbs_wash1 = NULL                             :   datetime
    dehydr_pbs_wash1_notes = ""                              :   varchar(250)
    time_dehydr_butanol_30percent = NULL                     :   datetime
    dehydr_butanol_30percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_50percent = NULL                     :   datetime
    dehydr_butanol_50percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_70percent = NULL                     :   datetime
    dehydr_butanol_70percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_80percent = NULL                     :   datetime
    dehydr_butanol_80percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_90percent = NULL                     :   datetime
    dehydr_butanol_90percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_96percent = NULL                     :   datetime
    dehydr_butanol_96percent_notes = ""                      :   varchar(250)
    time_dehydr_butanol_100percent = NULL                    :   datetime
    dehydr_butanol_100percent_notes = ""                     :   varchar(250)
    time_clearing_dcm_wash1 = NULL                           :   datetime
    clearing_dcm_wash1_notes = ""                            :   varchar(250)
    time_clearing_babb_wash1 = NULL                          :   datetime
    clearing_babb_wash1_notes = ""                           :   varchar(250)
    clearing_notes = ""                                      : varchar(500)
    """
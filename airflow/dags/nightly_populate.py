# NOTE: scaffold — bodies are stubs
"""Nightly populate DAG for U19 behavior, optogenetics, pupillometry, and alerts.

Runs @daily.  All task bodies are TODO stubs that reference (but do not
re-implement) existing u19_pipeline code.  Dependency edges encode both the
DataJoint FK order and implicit edges discovered by auditing MATLAB make() bodies.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

from u19.callbacks import slack_failure_callback

log = logging.getLogger(__name__)

default_args = {
    "owner": "u19",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="u19_nightly_populate",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["u19", "nightly", "behavior"],
)
def u19_nightly_populate() -> None:
    """Nightly populate DAG.

    Wires the full behavior ingest + alert graph.  Edge comments explain the
    reasoning; see MATLAB make() bodies for the implicit edges.
    """

    # -------------------------------------------------------------------------
    # Reweight reset — independent, runs first each night
    # -------------------------------------------------------------------------

    @task(task_id="reset_reweight")
    def reset_reweight() -> None:
        """Reset subject reweight flags via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_ResetReweight()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_ResetReweight()")
        pass

    # -------------------------------------------------------------------------
    # Scheduling group — independent of behavior ingest
    # -------------------------------------------------------------------------

    @task(task_id="populate_schedule_for_tomorrow")
    def populate_schedule_for_tomorrow() -> None:
        """Populate schedule for tomorrow via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_ScheduleForTomorrow()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_ScheduleForTomorrow()")
        pass

    @task(task_id="populate_technician_schedule")
    def populate_technician_schedule() -> None:
        """Populate technician schedule via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_TechnicianSchedule()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TechnicianSchedule()")
        pass

    @task(task_id="update_protocol_level")
    def update_protocol_level() -> None:
        """Update protocol level via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_ProtocolLevel()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_ProtocolLevel()")
        pass

    @task(task_id="update_training_profile")
    def update_training_profile() -> None:
        """Update training profile via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_TrainingProfile()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TrainingProfile()")
        pass

    # -------------------------------------------------------------------------
    # Behavior chain — FK order: Session → SessionBlock / TowersSession
    # -------------------------------------------------------------------------

    @task(task_id="session")
    def session() -> None:
        """Ingest behavior sessions via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_BehaviorSession()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_BehaviorSession()")
        pass

    @task(task_id="session_block")
    def session_block() -> None:
        """Populate SessionBlock via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_SessionBlock()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_SessionBlock()")
        pass

    @task(task_id="towers_session")
    def towers_session() -> None:
        """Populate TowersSession via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_TowersSession()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersSession()")
        pass

    @task(task_id="towers_block")
    def towers_block() -> None:
        """Populate TowersBlock (and TowersBlockTrial) via MATLAB.

        NOTE: TowersBlock.make() ALSO writes TowersBlockTrial — treat them as
        a single atomic unit; do not add a separate towers_block_trial task.

        TODO: call run_matlab_batch(..., step="populate_TowersBlock()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersBlock()")
        pass

    @task(task_id="spatial_time_blobs")
    def spatial_time_blobs() -> None:
        """Populate SpatialTimeBlobs via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_SpatialTimeBlobs()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_SpatialTimeBlobs()")
        pass

    @task(task_id="towers_session_psych")
    def towers_session_psych() -> None:
        """Populate TowersSessionPsych via MATLAB (reads TowersSession only).

        TODO: call run_matlab_batch(..., step="populate_TowersSessionPsych()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersSessionPsych()")
        pass

    @task(task_id="towers_session_psych_task")
    def towers_session_psych_task() -> None:
        """Populate TowersSessionPsychTask via MATLAB.

        IMPLICIT EDGE: although the DataJoint FK declaration only references
        TowersSession, TowersSessionPsychTask.make() also reads TowersBlock and
        TowersBlockTrial at runtime.  Therefore this task must run AFTER
        towers_block, not just after towers_session.

        TODO: call run_matlab_batch(..., step="populate_TowersSessionPsychTask()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersSessionPsychTask()")
        pass

    # Barrier: cumulative tables read ALL prior sessions, so they must wait for
    # the entire nightly TowersSession batch to finish before running — not per-session.
    towers_session_batch_complete = EmptyOperator(task_id="towers_session_batch_complete")

    @task(task_id="towers_subject_cumulative_psych")
    def towers_subject_cumulative_psych() -> None:
        """Populate TowersSubjectCumulativePsych via MATLAB.

        Runs after the batch barrier — reads ALL prior sessions, not just today's.

        TODO: call run_matlab_batch(..., step="populate_TowersSubjectCumulativePsych()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersSubjectCumulativePsych()")
        pass

    @task(task_id="towers_subject_cumulative_psych_level")
    def towers_subject_cumulative_psych_level() -> None:
        """Populate TowersSubjectCumulativePsychLevel via MATLAB.

        Runs after the batch barrier — reads ALL prior sessions, not just today's.

        TODO: call run_matlab_batch(..., step="populate_TowersSubjectCumulativePsychLevel()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_TowersSubjectCumulativePsychLevel()")
        pass

    # -------------------------------------------------------------------------
    # Optogenetics chain
    # -------------------------------------------------------------------------

    @task(task_id="ingest_optogenetic_sessions")
    def ingest_optogenetic_sessions() -> None:
        """Ingest optogenetic session metadata via MATLAB.

        TODO: call run_matlab_batch(..., step="ingest_OptogeneticSessions()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "ingest_OptogeneticSessions()")
        pass

    @task(task_id="optogenetic_session")
    def optogenetic_session() -> None:
        """Populate OptogeneticSession via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_OptogeneticSession()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_OptogeneticSession()")
        pass

    # -------------------------------------------------------------------------
    # Pupillometry chain
    # sync reads acquisition.SessionVideo + behavior file on the mount
    # -------------------------------------------------------------------------

    @task(task_id="pupillometry_session")
    def pupillometry_session() -> None:
        """Populate PupillometrySession via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_PupillometrySession()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_PupillometrySession()")
        pass

    @task(task_id="pupillometry_sync_behavior")
    def pupillometry_sync_behavior() -> None:
        """Sync pupillometry to behavior via MATLAB.

        NOTE: this step reads acquisition.SessionVideo and the behavior file on
        the network mount — ensure the mount is available before this task runs.

        TODO: call run_matlab_batch(..., step="populate_PupilSyncBehavior()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_PupilSyncBehavior()")
        pass

    # -------------------------------------------------------------------------
    # Posture tracking chain
    # -------------------------------------------------------------------------

    @task(task_id="posture_tracking_session")
    def posture_tracking_session() -> None:
        """Populate PostureTrackingSession via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_PostureTrackingSession()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_PostureTrackingSession()")
        pass

    @task(task_id="posture_tracking_sync_behavior")
    def posture_tracking_sync_behavior() -> None:
        """Sync posture tracking to behavior via MATLAB.

        TODO: call run_matlab_batch(..., step="populate_PostureTrackingSyncBehavior()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "populate_PostureTrackingSyncBehavior()")
        pass

    # -------------------------------------------------------------------------
    # Subtasks
    # -------------------------------------------------------------------------

    @task(task_id="ingest_subtasks")
    def ingest_subtasks() -> None:
        """Ingest subtasks via MATLAB.

        TODO: call run_matlab_batch(..., step="ingest_Subtasks()")
        """
        # TODO: from u19.matlab import run_matlab_batch; run_matlab_batch(..., "ingest_Subtasks()")
        pass

    # -------------------------------------------------------------------------
    # Alert group — all wrap u19_pipeline.alert_system.main_alert_system
    # Each alert is independent; all get the Slack failure callback.
    # -------------------------------------------------------------------------

    @task(task_id="alert_live_monitor", on_failure_callback=slack_failure_callback)
    def alert_live_monitor() -> None:
        """Run live-monitor alert check.

        TODO: call the live_monitor sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.live_monitor_alert as a; a.main()
        pass

    @task(task_id="alert_schedule_check", on_failure_callback=slack_failure_callback)
    def alert_schedule_check() -> None:
        """Run schedule-check alert.

        TODO: call the schedule_check_alert sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.schedule_check_alert as a; a.main()
        pass

    @task(task_id="alert_water_weigh", on_failure_callback=slack_failure_callback)
    def alert_water_weigh() -> None:
        """Run water/weight alert.

        TODO: call the water_weigh_alert sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.water_weigh_alert as a; a.main()
        pass

    @task(task_id="alert_tech", on_failure_callback=slack_failure_callback)
    def alert_tech() -> None:
        """Run technician alert.

        TODO: call the tech_alert sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.tech_alert as a; a.main()
        pass

    @task(task_id="alert_locked_tables", on_failure_callback=slack_failure_callback)
    def alert_locked_tables() -> None:
        """Run locked-tables alert.

        TODO: call the locked_tables_alert sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.locked_tables_alert as a; a.main()
        pass

    @task(task_id="alert_rig_maintenance", on_failure_callback=slack_failure_callback)
    def alert_rig_maintenance() -> None:
        """Run rig-maintenance alert.

        TODO: call the rig_maintenance sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.rig_maintenance as a; a.main()
        pass

    @task(task_id="alert_live_session_stats_deletion", on_failure_callback=slack_failure_callback)
    def alert_live_session_stats_deletion() -> None:
        """Run live-session-stats-deletion alert.

        TODO: call the live_session_stats_deletion sub-module from u19_pipeline.alert_system.main_alert_system
        """
        # TODO: import u19_pipeline.alert_system.custom_alerts.live_session_stats_deletion as a; a.main()
        pass

    # =========================================================================
    # Instantiate tasks
    # =========================================================================

    t_reset_reweight = reset_reweight()

    t_sched_tomorrow = populate_schedule_for_tomorrow()
    t_sched_tech = populate_technician_schedule()
    t_protocol_level = update_protocol_level()
    t_training_profile = update_training_profile()

    t_session = session()
    t_session_block = session_block()
    t_towers_session = towers_session()
    t_towers_block = towers_block()
    t_spatial_time_blobs = spatial_time_blobs()
    t_towers_session_psych = towers_session_psych()
    t_towers_session_psych_task = towers_session_psych_task()
    t_towers_subject_cumulative_psych = towers_subject_cumulative_psych()
    t_towers_subject_cumulative_psych_level = towers_subject_cumulative_psych_level()

    t_ingest_opto = ingest_optogenetic_sessions()
    t_opto_session = optogenetic_session()

    t_pupil_session = pupillometry_session()
    t_pupil_sync = pupillometry_sync_behavior()

    t_posture_session = posture_tracking_session()
    t_posture_sync = posture_tracking_sync_behavior()

    t_ingest_subtasks = ingest_subtasks()

    t_alert_live_monitor = alert_live_monitor()
    t_alert_schedule_check = alert_schedule_check()
    t_alert_water_weigh = alert_water_weigh()
    t_alert_tech = alert_tech()
    t_alert_locked_tables = alert_locked_tables()
    t_alert_rig_maintenance = alert_rig_maintenance()
    t_alert_live_session_stats_deletion = alert_live_session_stats_deletion()

    # =========================================================================
    # Wire dependencies
    # =========================================================================

    # --- reset_reweight: independent ---
    # (no upstream dependency)

    # --- scheduling group: independent of behavior ---
    # (no upstream dependency; all four are independent of each other)

    # --- behavior chain ---
    # FK order: Session → SessionBlock
    t_session >> t_session_block

    # FK order: Session → TowersSession
    t_session >> t_towers_session

    # TowersBlock requires both TowersSession and SessionBlock
    [t_towers_session, t_session_block] >> t_towers_block

    # SpatialTimeBlobs only needs TowersSession
    t_towers_session >> t_spatial_time_blobs

    # TowersSessionPsych reads TowersSession only (FK is accurate here)
    t_towers_session >> t_towers_session_psych

    # IMPLICIT EDGE: TowersSessionPsychTask FK says only TowersSession, but
    # make() reads TowersBlock + TowersBlockTrial at runtime — must follow towers_block
    t_towers_block >> t_towers_session_psych_task

    # Batch barrier: cumulative tables must wait for the whole nightly
    # TowersSession batch, not just one session's worth of work
    t_towers_session >> towers_session_batch_complete
    towers_session_batch_complete >> t_towers_subject_cumulative_psych
    towers_session_batch_complete >> t_towers_subject_cumulative_psych_level

    # --- optogenetics chain ---
    t_session >> t_ingest_opto >> t_opto_session

    # --- pupillometry chain ---
    # sync reads acquisition.SessionVideo + behavior file on the mount
    t_session >> t_pupil_session >> t_pupil_sync

    # --- posture tracking chain ---
    t_session >> t_posture_session >> t_posture_sync

    # --- subtasks ---
    t_session >> t_ingest_subtasks

    # --- alerts: all independent (no explicit deps on behavior tasks) ---
    # t_alert_* are all independent of each other and of behavior ingest


u19_nightly_populate()

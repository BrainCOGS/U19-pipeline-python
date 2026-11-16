"""Shared live registration backend; planning reads only, commit requires intent.

No GUI or pipeline schemas are imported. DataJoint is loaded only by the live
adapter; fake adapters can exercise the full service without a database.
"""

from contextlib import contextmanager
import hashlib
import json
import operator
from pathlib import Path, PureWindowsPath


def parameter_id(value, label):
    try:
        result = operator.index(value)
    except TypeError as exc:
        raise ValueError(f"{label} must be an integer ID, not a demo profile.") from exc
    if isinstance(value, bool) or result < 0:
        raise ValueError(f"{label} must be a nonnegative integer ID.")
    return result


def safe_component(value):
    value = str(value)
    if (
        not value
        or value in (".", "..")
        or value.endswith((".", " "))
        or any(char in value for char in '<>:"/\\|?*')
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(f"Unsafe destination path component: {value!r}")
    return value


def local_key(value):
    return str(PureWindowsPath(value)).replace("\\", "/").rstrip("/").casefold()


def resolve_windows_root(official, paths):
    official = str(official).replace("\\", "/").strip("/")
    hits = []
    for row in paths:
        prefix = row["global_path"].replace("\\", "/").strip("/")
        if official == prefix or official.startswith(prefix + "/"):
            hits.append(
                (len(prefix), row["net_location"], official[len(prefix) :].strip("/"))
            )
    if not hits:
        raise ValueError(
            "No Windows lab.Path mapping covers the modality raw-data root."
        )
    longest = max(item[0] for item in hits)
    choices = {(location, rest) for length, location, rest in hits if length == longest}
    if len(choices) != 1:
        raise ValueError("Ambiguous Windows lab.Path mapping for raw-data root.")
    location, rest = choices.pop()
    if not PureWindowsPath(location).is_absolute():
        raise ValueError("lab.Path storage mapping must be absolute.")
    return (
        str(PureWindowsPath(location).joinpath(*rest.split("/"))) if rest else location
    )


def prepare(adapter, source, modality, system, behavior_rigs, preferences):
    """Revalidate selections against live tables and describe the exact operation."""
    from .app import inventory
    from .recording_options import matching_sessions

    source = Path(source).resolve()
    files = inventory(source)
    if modality not in ("imaging", "electrophysiology"):
        raise ValueError("Unsupported recording modality.")
    patterns = (
        (".ap.bin",) if modality == "electrophysiology" else (".tif", ".tiff", ".avi")
    )
    if not any(name.lower().endswith(patterns) for name in files):
        raise ValueError("No raw recording files for the selected modality were found.")
    selected = preferences.get("behavior_session")
    if not selected:
        raise ValueError("Select an ingested behavior session before registration.")
    if preferences.get("catalog_demo"):
        raise ValueError("Synthetic catalog selections cannot be registered.")
    if preferences.get("add_surgery"):
        raise ValueError(
            "Surgery creation is a separate form; register existing surgery metadata first."
        )
    if not system or not behavior_rigs:
        raise ValueError("Configure the source system and its allowed behavior rigs.")
    key = {
        field: selected[field]
        for field in ("subject_fullname", "session_date", "session_number")
    }
    key["session_number"] = parameter_id(key["session_number"], "Session number")
    session = adapter.session(key)
    if session["session_location"] not in behavior_rigs:
        raise ValueError("Selected session belongs to a different behavior rig.")
    adapter.validate_subject_and_system(key["subject_fullname"], system)
    from .rigs import validate_live_profile

    validate_live_profile(adapter, preferences.get("source_rig"), modality, system)
    user_id = adapter.subject_user(key["subject_fullname"])
    default_pre, default_processing = adapter.defaults(modality)
    ids = {}
    for kind, fallback in (
        ("processing", default_processing),
        ("preprocessing", default_pre),
    ):
        selection = preferences.get(kind)
        if selection and selection.get("modality", modality) != modality:
            raise ValueError("Selected parameters belong to another modality.")
        ids[kind] = parameter_id(selection["id"] if selection else fallback, kind)
    adapter.validate_parameters(modality, ids["processing"], ids["preprocessing"])
    from datetime import date

    day = date.fromisoformat(str(key["session_date"]))
    leaf = safe_component(source.name)
    relative = "/".join(
        [
            safe_component(user_id),
            safe_component(key["subject_fullname"]),
            f"{day:%Y%m%d}_g{key['session_number']}",
            leaf,
        ]
    )
    root = Path(adapter.raw_root(modality)).resolve()
    destination = root.joinpath(*relative.split("/"))
    if (
        source == destination
        or source.is_relative_to(root)
        or root.is_relative_to(source)
    ):
        raise ValueError(
            "Registration source and raw-data storage must be separate trees."
        )
    _, matched = matching_sessions(source, [session])
    warnings = (
        []
        if matched
        else [
            "Folder subject/date does not match the selected session; verify the selection."
        ]
    )
    if session.get("num_trials", 1) <= 0:
        warnings.append("Selected behavior session has no confirmed trials.")
    plan = dict(
        source=str(source),
        system=system,
        modality=modality,
        session=key,
        user_id=user_id,
        behavior_rigs=sorted(behavior_rigs),
        processing=ids["processing"],
        preprocessing=ids["preprocessing"],
        raw_root=str(root),
        recording_directory=relative,
        destination=str(destination),
        warnings=warnings,
        allow_duplicate_session=bool(preferences.get("allow_duplicate_session")),
        source_rig=preferences.get("source_rig"),
    )
    plan["existing_recording_id"] = adapter.check_duplicates(plan)
    return plan


def request_for(plan):
    """Freeze effective IDs so a default changing after preview cannot change intent."""
    return dict(
        behavior_session=plan["session"],
        processing=dict(id=plan["processing"]),
        preprocessing=dict(id=plan["preprocessing"]),
        add_surgery=False,
        allow_duplicate_session=plan["allow_duplicate_session"],
        source_rig=plan.get("source_rig"),
    )


def describe(plan):
    return (
        f"{plan['session']['subject_fullname']} • {plan['session']['session_date']} • "
        f"session {plan['session']['session_number']}\nFinal path: {plan['destination']}\n"
        f"Processing ID: {plan['processing']}\nPreprocessing list ID: {plan['preprocessing']}\n"
        f"Allow another recording for the session: {plan['allow_duplicate_session']}\n"
        "Registration sets status 2 and makes the recording eligible for pipeline ingestion.\n"
        "Source data is copied to the final path. Any existing incoming copy is retained.\n"
        + "\n".join(plan["warnings"])
    )


class DataJointAdapter:
    """Live adapter. All writes are confined to commit(), under advisory locks."""

    def __init__(self, config_path):
        from .credentials import connect_database

        dj, self.conn, config = connect_database(config_path)
        prefix = config.get("custom", {}).get("database.prefix", "u19_")
        for name in ("recording", "acquisition", "subject", "lab", "action"):
            setattr(
                self, name, dj.VirtualModule(name, prefix + name, connection=self.conn)
            )
        self.dj, self.prefix, self.elements = dj, prefix, {}

    def element_for(self, modality):
        if modality not in self.elements:
            schema = (
                "pipeline_imaging_element"
                if modality == "imaging"
                else "pipeline_ephys_element"
            )
            self.elements[modality] = self.dj.VirtualModule(
                schema, self.prefix + schema, connection=self.conn
            )
        return self.elements[modality]

    def session(self, key):
        relation = self.acquisition.Session & key
        if len(relation) != 1:
            raise ValueError(
                "Behavior session is missing or ambiguous in the live database."
            )
        return relation.fetch1()

    def validate_subject_and_system(self, subject, system):
        if not len(self.subject.Subject & dict(subject_fullname=subject)):
            raise ValueError("Subject does not exist.")
        if not len(self.lab.Location & dict(location=system)):
            raise ValueError("Source system does not exist in lab.Location.")
        if not len(self.action.Surgery & dict(subject_fullname=subject)):
            raise ValueError(
                "Surgery metadata is missing; complete the existing surgery form first."
            )

    def subject_user(self, subject):
        return (self.subject.Subject & dict(subject_fullname=subject)).fetch1("user_id")

    def defaults(self, modality):
        return (self.recording.Modality & dict(recording_modality=modality)).fetch1(
            "default_preprocess_param_steps_id", "default_paramset_idx"
        )

    def validate_parameters(self, modality, processing, preprocessing):
        element = self.element_for(modality)
        params = (
            element.ProcessingParamSet
            if modality == "imaging"
            else element.ClusteringParamSet
        )
        steps = (
            element.PreprocessParamSteps
            if modality == "imaging"
            else element.PreClusterParamSteps
        )
        field = (
            "preprocess_param_steps_id"
            if modality == "imaging"
            else "precluster_param_steps_id"
        )
        if not len(params & dict(paramset_idx=processing)):
            raise ValueError(
                "Processing parameter ID is missing from the live modality table."
            )
        if preprocessing != 0 and not len(steps & {field: preprocessing}):
            raise ValueError(
                "Preprocessing list is missing from the live modality table."
            )

    def raw_root(self, modality):
        variable = (
            "imaging_root_data_dir" if modality == "imaging" else "ephys_root_data_dir"
        )
        official = (
            self.lab.DjCustomVariables & dict(custom_variable=variable, index=0)
        ).fetch1("value")
        return resolve_windows_root(
            official, (self.lab.Path & dict(system="windows")).fetch(as_dict=True)
        )

    def check_duplicates(self, plan):
        recording = self.recording.Recording
        rows = (recording & dict(recording_modality=plan["modality"])).fetch(
            as_dict=True
        )
        matches = [
            row
            for row in rows
            if (
                row["location"] == plan["system"]
                and local_key(row["local_directory"]) == local_key(plan["source"])
            )
            or row["recording_directory"].replace("\\", "/").strip("/").casefold()
            == plan["recording_directory"].casefold()
        ]
        existing = None
        if matches:
            if len(matches) != 1:
                raise ValueError(
                    "Multiple recordings already claim this source or destination."
                )
            row = matches[0]
            identifier = int(row["recording_id"])
            associated = (
                recording.BehaviorSession
                & dict(recording_id=identifier)
                & plan["session"]
            )
            params = self.recording.DefaultParams & dict(
                recording_id=identifier,
                fragment_number=0,
                preprocess_param_steps_id=plan["preprocessing"],
                paramset_idx=plan["processing"],
                default_same_params_all=1,
                default_same_preparams_all=1,
            )
            if (
                row["location"] != plan["system"]
                or local_key(row["local_directory"]) != local_key(plan["source"])
                or row["recording_directory"].strip("/") != plan["recording_directory"]
                or row["status_recording_id"] not in (2, 3)
                or not len(associated)
                or not len(params)
                or len(self.recording.DefaultParams & dict(recording_id=identifier))
                != 1
            ):
                raise ValueError(
                    "Existing recording conflicts with the approved source/session/parameters."
                )
            existing = identifier
        session_ids = {
            int(value)
            for value in (recording.BehaviorSession & plan["session"]).fetch(
                "recording_id"
            )
        }
        if not plan["allow_duplicate_session"] and session_ids - (
            {existing} if existing else set()
        ):
            raise ValueError(
                "Behavior session already has a recording; explicitly allow another only if intended."
            )
        return existing

    @contextmanager
    def locks(self, plan):
        names = [
            json.dumps(plan["session"], sort_keys=True),
            plan["modality"] + ":" + plan["recording_directory"].casefold(),
            plan["system"] + ":" + local_key(plan["source"]),
        ]
        acquired = []
        try:
            for key in sorted(
                {
                    "u19-registration-" + hashlib.sha256(name.encode()).hexdigest()[:40]
                    for name in names
                }
            ):
                if (
                    self.conn.query("SELECT GET_LOCK(%s, %s)", (key, 0)).fetchone()[0]
                    != 1
                ):
                    raise RuntimeError(
                        "Another registration is active for this source, destination or session."
                    )
                acquired.append(key)
            yield
        finally:
            for key in reversed(acquired):
                self.conn.query("SELECT RELEASE_LOCK(%s)", (key,))

    def commit(self, plan):
        with self.locks(plan), self.conn.transaction:
            from .rigs import validate_live_profile

            validate_live_profile(
                self, plan.get("source_rig"), plan["modality"], plan["system"]
            )
            # Repeat live validation after locks and inside the transaction.
            session = self.session(plan["session"])
            if session["session_location"] not in plan["behavior_rigs"]:
                raise ValueError("Session rig changed after approval.")
            if (
                self.subject_user(plan["session"]["subject_fullname"])
                != plan["user_id"]
            ):
                raise ValueError("Subject ownership changed after approval.")
            if (
                Path(self.raw_root(plan["modality"])).resolve()
                != Path(plan["raw_root"]).resolve()
            ):
                raise ValueError("Raw-data storage root changed after approval.")
            self.validate_subject_and_system(
                plan["session"]["subject_fullname"], plan["system"]
            )
            self.validate_parameters(
                plan["modality"], plan["processing"], plan["preprocessing"]
            )
            existing = self.check_duplicates(plan)
            if existing is not None:
                return existing
            self.recording.Recording.insert1(
                dict(
                    recording_modality=plan["modality"],
                    location=plan["system"],
                    status_recording_id=2,
                    recording_directory=plan["recording_directory"],
                    local_directory=str(plan["source"]).replace("\\", "/"),
                )
            )
            identifier = int(self.conn.query("SELECT LAST_INSERT_ID()").fetchone()[0])
            self.recording.Recording.BehaviorSession.insert1(
                dict(recording_id=identifier, **plan["session"])
            )
            self.recording.DefaultParams.insert1(
                dict(
                    recording_id=identifier,
                    fragment_number=0,
                    default_same_preparams_all=1,
                    default_same_params_all=1,
                    preprocess_param_steps_id=plan["preprocessing"],
                    paramset_idx=plan["processing"],
                )
            )
            return identifier

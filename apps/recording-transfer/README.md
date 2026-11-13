# BrainCOGS U19 Recording Transfer

A lightweight Windows desktop application, independently packaged inside
`U19-pipeline-python`. It does not install the cluster pipeline or its scientific
processing dependencies. See [DEPLOYMENT.md](DEPLOYMENT.md) for installation,
release, update, recovery and rig rollout procedures.

Installers and application updates are distributed through public GitHub Releases
tagged `recording-transfer/v<version>`. Rigs select a stable or pilot channel and
need no GitHub token, Git checkout or software-release share on cup.

```powershell
uv run --project apps/recording-transfer u19-recording-transfer --demo
uv --directory apps/recording-transfer run pytest
```

The GUI copies selected recording folders into an incoming area, verifies hashes,
and preserves the source. Registration preferences travel in a completion manifest
for later website confirmation. It does not register datasets in DataJoint.

The first screen defaults surgery to unchecked, shows processing parameters before
preprocessing, selects `PNI182-1plane-30fs-PPC` when present in the loaded catalog,
and filters behavior sessions by subject and date with an all-sessions fallback.

Use `--catalog <snapshot.json>` for real session and parameter choices. A separate
read-only catalog exporter is available with `uv run --extra catalog
u19-recording-catalog --dj-config <config> --output <snapshot>` from this project.
Snapshots exclude credentials; store snapshots and DB configuration outside the
release directory. Demo IDs cannot be loaded as production profiles.

At 23:00, the scheduled copy command processes queued folders only. It does not
discover unqueued recordings. Quiet-time checks are a heuristic and do not prove
acquisition has stopped. Interrupted files restart from their beginning; verified
existing files are reused. Keep the GUI open while copying.

Optional local cleanup defaults to preview and 14-day retention. It requires exact
source/copy inventories and hashes, a matching completion manifest and PC identity,
and explicit allowed roots. It only handles recordings copied by this application.
Cleanup remains disabled in the machine configuration unless deliberately enabled.
Website registration and moving incoming data to final paths remain separate work.

The BrainCOGS logo is reused from the lab's existing Streamlit application.

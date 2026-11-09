# U19-pipeline-python

The DataJoint pipeline for the U19 projects: schemas in `u19_pipeline/`, the
automation that runs on the u19proc host (cron jobs, Slack alerts, cluster job
handling) and the ephys/imaging element pipelines. Python >= 3.12, managed with uv.

## Setup and tests

```bash
uv sync                       # base deps + dev group
uv run pytest -rs             # what CI's base legs run (3.12, 3.14)
```

- The `pipeline` extra (suite2p, cellpose, element-* git deps) is heavy. CI
  installs it on 3.13 without torch/CUDA and runs `pytest -rs --require-extras`,
  which turns `pytest.importorskip` skips into failures.
- Tests marked `db` skip when no DataJoint database is reachable (`tests/dbskip.py`).
  `--run-db` makes them fail instead. This dev host has no database.
- Lint: `uvx prek run --all-files` (the same as CI's Lint workflow).

## Pins and gotchas

- `datajoint==0.14.10` exactly: 2.x reads `datajoint.json` and ignores the
  `dj_local_conf.json` every deployment uses. Dependabot ignores datajoint >= 2.
- `suite2p==1.1.0`. Imaging paramset names must not contain `_`.
- `exclude-newer = "14 days"` in pyproject.toml (stricter than the 7 days of the
  other BrainCOGS repos). Dependabot's uv cooldown must stay >= it.
- Git history holds old Slack webhooks; never add new secrets (gitleaks runs on
  commit and in CI).

## Agent workflow

- Bugs: always log them. Before fixing an error, whether the user reported it in Slack or you found it while working, file an agent bug report issue from the user's account (`gh issue create -R BrainCOGS/U19-pipeline-python`, template `.github/ISSUE_TEMPLATE/agent_bug_report.md`). Put the error exactly as the user pasted it under **Reported error**; don't ask for or invent a Slack link. Then fix it with the `bugfix-from-slack` skill (`.claude/skills/bugfix-from-slack/`, vendored from [agent-tooling](https://github.com/BrainCOGS/agent-tooling)); the PR says `Fixes #<issue>`. Commits, pushes, issues and PRs all go through the user's account.
- Commits end with one `Assisted-by: <Harness>:<model>` trailer per agent, e.g. `Assisted-by: ClaudeCode:claude-opus-5-5`. No AI co-authors, and no `Generated with` lines.
- PRs target `master` on `BrainCOGS/U19-pipeline-python`, opened from the user's own account.

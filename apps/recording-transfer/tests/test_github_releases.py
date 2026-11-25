"""GitHub-only discovery uses public app-specific releases and fails closed."""

import importlib.util
import json
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "github_bootstrap", PROJECT / "deployment/bootstrap.py"
)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)
REPO = "BrainCOGS/U19-pipeline-python"


def record(version, *, prerelease=False, draft=False):
    tag = "recording-transfer/v" + version
    base = f"https://github.com/{REPO}/releases/download/{tag}/"
    names = [f"release-{version}.json", f"u19-recording-transfer-{version}.zip"]
    return dict(
        tag_name=tag,
        prerelease=prerelease,
        draft=draft,
        assets=[
            dict(name=name, state="uploaded", browser_download_url=base + name)
            for name in names
        ],
    )


def responses(monkeypatch, pages, manifests=None):
    calls = []

    def fetch(source, limit, headers=None):
        calls.append(source)
        if source.startswith("https://api.github.com/"):
            page = int(source.rsplit("=", 1)[-1])
            assert headers["Accept"] == "application/vnd.github+json"
            return json.dumps(pages[page - 1]).encode()
        version = source.rsplit("release-", 1)[-1].removesuffix(".json")
        manifest = dict(
            version=version,
            bootstrap_api=1,
            state_schema=1,
            archive=f"u19-recording-transfer-{version}.zip",
            sha256="a" * 64,
        )
        if manifests:
            manifest.update(manifests)
        return json.dumps(manifest).encode()

    monkeypatch.setattr(bootstrap, "source_bytes", fetch)
    return calls


@pytest.mark.parametrize("channel,expected", [("stable", "0.2.0"), ("pilot", "0.3.0")])
def test_channel_filtering_ignores_other_apps_drafts_and_prereleases(
    monkeypatch, channel, expected
):
    unrelated = record("99.0.0")
    unrelated["tag_name"] = "pipeline/v99.0.0"
    responses(
        monkeypatch,
        [
            [
                record("0.1.0"),
                unrelated,
                record("0.3.0", prerelease=True),
                record("0.9.0", draft=True),
                record("0.2.0"),
            ]
        ],
    )
    manifest, url = bootstrap.github_release_manifest(
        dict(github=dict(channel=channel))
    )
    assert manifest["version"] == expected
    assert url.endswith(f"u19-recording-transfer-{expected}.zip")


def test_paginated_discovery_and_pinned_version(monkeypatch):
    unrelated = [dict(tag_name=f"pipeline/v{i}", draft=False) for i in range(100)]
    calls = responses(monkeypatch, [unrelated, [record("0.1.0"), record("0.2.0")]])
    result, _ = bootstrap.github_release_manifest(
        dict(github=dict(pinned_version="0.1.0"))
    )
    assert result["version"] == "0.1.0"
    assert any("page=2" in url for url in calls)


def test_empty_or_unpublished_channel_is_an_explicit_error(monkeypatch):
    responses(monkeypatch, [[record("0.1.0", prerelease=True)]])
    with pytest.raises(ValueError, match="No published"):
        bootstrap.github_release_manifest({})


@pytest.mark.parametrize(
    "change", ["host", "other_release", "duplicate", "missing", "not_uploaded"]
)
def test_asset_identity_validation(monkeypatch, change):
    release = record("0.1.0")
    if change == "host":
        release["assets"][0]["browser_download_url"] = (
            "https://untrusted.example/release-0.1.0.json"
        )
    elif change == "other_release":
        release["assets"][0]["browser_download_url"] = release["assets"][0][
            "browser_download_url"
        ].replace("v0.1.0", "v9.0.0")
    elif change == "duplicate":
        release["assets"].append(dict(release["assets"][0]))
    elif change == "missing":
        release["assets"] = release["assets"][1:]
    else:
        release["assets"][0]["state"] = "new"
    responses(monkeypatch, [[release]])
    with pytest.raises(ValueError):
        bootstrap.github_release_manifest({})


@pytest.mark.parametrize(
    "manifest",
    [
        dict(version="9.0.0"),
        dict(archive="unexpected.zip"),
        dict(state_schema=2),
        dict(sha256="not-a-checksum"),
    ],
)
def test_release_tag_and_manifest_must_agree(monkeypatch, manifest):
    responses(monkeypatch, [[record("0.1.0")]], manifest)
    with pytest.raises(ValueError):
        bootstrap.github_release_manifest({})


@pytest.mark.parametrize(
    "settings",
    [
        dict(repository="../other"),
        dict(channel="latest"),
        dict(tag_prefix="v"),
        dict(pinned_version="../0.1.0"),
    ],
)
def test_configuration_validation(settings):
    with pytest.raises(ValueError):
        bootstrap.github_settings(dict(github=settings))

"""Build a versioned source release with its frozen lockfile and SHA-256 manifest."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import tomllib
import zipfile


def build(project, output):
    metadata = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))
    version = metadata["project"]["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("Use major.minor.patch release versions.")
    init = (project / "src/u19_recording_transfer/__init__.py").read_text(
        encoding="utf-8"
    )
    if f'__version__ = "{version}"' not in init:
        raise ValueError("pyproject.toml and __version__ disagree.")
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"u19-recording-transfer-{version}.zip"
    files = [project / name for name in ("pyproject.toml", "uv.lock", "README.md")]
    files += [
        path
        for path in (project / "src").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    ]
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(files):
            info = zipfile.ZipInfo(
                path.relative_to(project).as_posix(), date_time=(2020, 1, 1, 0, 0, 0)
            )
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            bundle.writestr(info, path.read_bytes())
    manifest = dict(
        version=version,
        bootstrap_api=1,
        state_schema=1,
        archive=archive.name,
        sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
    )
    (output / f"release-{version}.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    setup_archive = output / f"u19-recording-transfer-setup-{version}.zip"
    with zipfile.ZipFile(
        setup_archive, "w", compression=zipfile.ZIP_DEFLATED
    ) as bundle:
        for name in (
            "bootstrap.py",
            "launch.ps1",
            "setup.ps1",
            "tasks.ps1",
            "machine.example.json",
        ):
            bundle.writestr(name, (project / "deployment" / name).read_bytes())
        bundle.writestr("DEPLOYMENT.md", (project / "DEPLOYMENT.md").read_bytes())
    setup_hash = hashlib.sha256(setup_archive.read_bytes()).hexdigest()
    (output / f"{setup_archive.name}.sha256").write_text(
        f"{setup_hash}  {setup_archive.name}\n", encoding="utf-8"
    )
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--out", type=Path, default=Path("dist"))
    args = parser.parse_args()
    print(json.dumps(build(args.project.resolve(), args.out.resolve()), indent=2))

"""Validate release identity before building or publishing application assets."""

import argparse
from pathlib import Path
import re
import tomllib


def release_tag(project, version):
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", version):
        raise ValueError("Version must be major.minor.patch.")
    metadata = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))
    if metadata["project"]["version"] != version:
        raise ValueError("Requested release version does not match pyproject.toml.")
    init = (project / "src/u19_recording_transfer/__init__.py").read_text(
        encoding="utf-8"
    )
    if f'__version__ = "{version}"' not in init:
        raise ValueError(
            "Requested release version does not match application version."
        )
    return "recording-transfer/v" + version


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--project", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args()
    print(release_tag(args.project, args.version))

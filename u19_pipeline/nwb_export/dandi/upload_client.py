"""
DANDI upload client adapter (T066 / US7).

Thin wrapper around ``neuroconv.tools.data_transfers.automatic_dandi_upload``
for uploading NWB files to DANDI Archive.  All retry logic is delegated to
:mod:`u19_pipeline.nwb_export.dandi.retry_policy`.

``automatic_dandi_upload`` expects:
- A **directory** containing NWB files (not a bare file path).
- The API key set as the ``DANDI_API_KEY`` environment variable.

This adapter handles both of those details transparently:
it creates a temporary staging directory, copies the single NWB file into it,
sets the env var for the duration of the call, then cleans up.

Usage::

    from u19_pipeline.nwb_export.dandi.upload_client import DandiUploadClient

    client = DandiUploadClient(api_key="...", dandiset_id="000123")
    organized_paths = client.upload(local_path="/path/to/output.nwb")
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional

from neuroconv.tools.data_transfers import automatic_dandi_upload  # type: ignore  # noqa: E402


class DandiUploadClient:
    """Adapter for uploading NWB files to DANDI Archive via neuroconv.

    Args:
        api_key:     Decrypted DANDI API key.
        dandiset_id: Target dandiset identifier (e.g. ``'000123'``).
        sandbox:     If ``True``, upload to the DANDI sandbox instance and
                     expect ``DANDI_SANDBOX_API_KEY`` to be the env var name.
                     Defaults to ``False``.
    """

    #: Environment variable name used by neuroconv for the main DANDI instance.
    API_KEY_ENV_VAR = "DANDI_API_KEY"
    #: Environment variable name used by neuroconv for the sandbox instance.
    SANDBOX_API_KEY_ENV_VAR = "DANDI_SANDBOX_API_KEY"

    def __init__(
        self,
        api_key: str,
        dandiset_id: str,
        sandbox: bool = False,
    ) -> None:
        if not api_key or not api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if not dandiset_id or not dandiset_id.strip():
            raise ValueError("dandiset_id must be a non-empty string")

        self._api_key = api_key
        self._dandiset_id = dandiset_id
        self._sandbox = sandbox

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload(self, local_path: str | Path) -> List[str]:
        """Upload a local NWB file to DANDI and return the organized file paths.

        Neuroconv's ``automatic_dandi_upload`` organises NWB files into the
        DANDI folder convention (subject/session naming) before uploading.
        The returned list contains the paths of the organised files as strings.

        Args:
            local_path: Path to the local ``.nwb`` file to upload.

        Returns:
            List of organised NWB file path strings (post-DANDI rename).

        Raises:
            FileNotFoundError: If *local_path* does not exist.
            RuntimeError:      If the neuroconv / DANDI upload fails.
        """
        path = Path(local_path)
        if not path.exists():
            raise FileNotFoundError(f"NWB file not found: {path}")

        return self._do_upload(path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _do_upload(self, path: Path) -> List[str]:
        """Copy *path* into a temp staging dir and call automatic_dandi_upload."""
        env_var = self.SANDBOX_API_KEY_ENV_VAR if self._sandbox else self.API_KEY_ENV_VAR
        prev_value = os.environ.get(env_var)

        staging_dir: Optional[Path] = None
        try:
            # Set API key env var for the duration of the upload.
            os.environ[env_var] = self._api_key

            # neuroconv expects a *directory*; copy the single file into one.
            staging_dir = Path(tempfile.mkdtemp())
            shutil.copy2(path, staging_dir / path.name)

            result = automatic_dandi_upload(
                dandiset_id=self._dandiset_id,
                nwb_folder_path=staging_dir,
                sandbox=self._sandbox,
                cleanup=False,  # we clean up staging_dir ourselves below
            )
            return result  # list[str] of organised NWB paths
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"DANDI upload failed: {exc}") from exc
        finally:
            # Restore previous env state.
            if prev_value is None:
                os.environ.pop(env_var, None)
            else:
                os.environ[env_var] = prev_value
            # Clean up staging directory.
            if staging_dir is not None and staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)

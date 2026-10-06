"""
DANDI upload eligibility check — pure logic, no DataJoint dependency (T056 / US5).

``is_eligible_for_upload`` determines whether a user can upload to DANDI based
solely on whether both credentials are non-empty.  The DataJoint-backed wrapper
``can_upload_to_dandi`` in ``nwb_production.py`` delegates to this function.

Usage::

    from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload

    if is_eligible_for_upload(api_key, dandiset_id):
        upload(...)
"""

from __future__ import annotations

from typing import Optional


def is_eligible_for_upload(api_key: Optional[str], dandiset_id: Optional[str]) -> bool:
    """Return ``True`` only when both *api_key* and *dandiset_id* are present.

    Accepts the direct output of ``get_dandi_credentials(user_id)``.

    Args:
        api_key:     Decrypted DANDI API key string, or ``None``.
        dandiset_id: Dandiset identifier string (e.g. ``'000123'``), or ``None``.

    Returns:
        ``True`` when both values are non-``None`` and non-empty strings.
    """
    return bool(api_key and api_key.strip()) and bool(dandiset_id and dandiset_id.strip())

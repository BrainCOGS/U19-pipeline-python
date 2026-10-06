"""
AES-256-GCM encryption for DANDI API credentials (FR-021).

The master key is loaded from the ``NWB_DANDI_KEY_HEX`` environment variable
(64 hex characters = 32 bytes = 256 bits).  The nonce is randomly generated
per encryption call and stored as a prefix of the ciphertext blob.

Encoding layout (base64-encoded, stored as a single VARCHAR):

    [ 12-byte nonce ][ N-byte ciphertext ][ 16-byte GCM tag ]

Usage::

    import os
    os.environ["NWB_DANDI_KEY_HEX"] = "..." # 64 hex chars

    from u19_pipeline.nwb_export.credentials_crypto import encrypt_api_key, decrypt_api_key

    blob = encrypt_api_key("my-dandi-token")
    original = decrypt_api_key(blob)   # == "my-dandi-token"
"""

from __future__ import annotations

import base64
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------

_ENV_VAR = "NWB_DANDI_KEY_HEX"
_NONCE_BYTES = 12  # 96-bit nonce — recommended for AES-GCM
_KEY_BYTES = 32  # 256 bits

# Per-encryption AAD is omitted here; the ciphertext is self-contained.
_AAD: Optional[bytes] = None


# -------------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------------


def _get_master_key() -> bytes:
    """Load and validate the master key from the environment.

    Raises:
        EnvironmentError: If the env variable is absent or has wrong length.
    """
    hex_key = os.environ.get(_ENV_VAR)
    if not hex_key:
        raise EnvironmentError(
            f"AES-256-GCM master key not set. "
            f"Export environment variable '{_ENV_VAR}' "
            f"as 64 hex characters (256-bit key)."
        )
    try:
        raw = bytes.fromhex(hex_key)
    except ValueError as exc:
        raise EnvironmentError(f"'{_ENV_VAR}' is not valid hex: {exc}") from exc

    if len(raw) != _KEY_BYTES:
        raise EnvironmentError(
            f"'{_ENV_VAR}' must encode exactly {_KEY_BYTES} bytes ({_KEY_BYTES * 2} hex chars). Got {len(raw)} bytes."
        )
    return raw


# -------------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------------


def encrypt_api_key(plaintext: Optional[str]) -> Optional[str]:
    """Encrypt *plaintext* with AES-256-GCM.

    Args:
        plaintext: The DANDI API key string to protect.  Pass ``None`` to
                   represent an unset key; returns ``None`` unchanged.

    Returns:
        Base64-encoded ``nonce || ciphertext+tag`` string, or ``None``.

    Raises:
        EnvironmentError: If ``NWB_DANDI_KEY_HEX`` is missing or malformed.
    """
    if plaintext is None:
        return None

    key = _get_master_key()
    nonce = os.urandom(_NONCE_BYTES)
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode(), _AAD)
    blob = nonce + ciphertext
    return base64.b64encode(blob).decode()


def decrypt_api_key(ciphertext_b64: Optional[str]) -> Optional[str]:
    """Decrypt a blob previously produced by :func:`encrypt_api_key`.

    Args:
        ciphertext_b64: Base64-encoded ``nonce || ciphertext+tag``.
                        Pass ``None`` to represent an unset key; returns
                        ``None`` unchanged.

    Returns:
        Original plaintext API key string, or ``None``.

    Raises:
        EnvironmentError: If ``NWB_DANDI_KEY_HEX`` is missing or malformed.
        ValueError: If the blob is truncated or corrupt.
    """
    if ciphertext_b64 is None:
        return None

    key = _get_master_key()
    blob = base64.b64decode(ciphertext_b64.encode())

    if len(blob) < _NONCE_BYTES:
        raise ValueError("Encrypted blob is too short to contain a valid nonce.")

    nonce = blob[:_NONCE_BYTES]
    ciphertext = blob[_NONCE_BYTES:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ciphertext, _AAD).decode()

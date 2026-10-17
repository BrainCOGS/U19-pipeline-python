"""
Tests for AES-256-GCM encryption of DANDI API credentials (FR-021 / T010).

TDD: Written before credentials_crypto.py exists — all import-dependent tests
will raise ImportError confirming red state.
"""

import os
import pytest


@pytest.mark.no_db
class TestCredentialsCryptoImport:
    """Module must be importable from the nwb_export package."""

    def test_module_importable(self):
        from u19_pipeline.nwb_export import credentials_crypto  # noqa: F401

    def test_encrypt_callable(self):
        from u19_pipeline.nwb_export.credentials_crypto import encrypt_api_key
        assert callable(encrypt_api_key)

    def test_decrypt_callable(self):
        from u19_pipeline.nwb_export.credentials_crypto import decrypt_api_key
        assert callable(decrypt_api_key)


@pytest.mark.no_db
class TestAesGcmRoundTrip:
    """encrypt_api_key → decrypt_api_key must be a perfect inverse."""

    @pytest.fixture(autouse=True)
    def _set_env(self, monkeypatch, tmp_path):
        # 32-byte (256-bit) hex key  →  64 hex chars
        monkeypatch.setenv(
            "NWB_DANDI_KEY_HEX",
            "0" * 64,  # 32 zero bytes — valid AES-256 key for tests
        )
        from u19_pipeline.nwb_export.credentials_crypto import encrypt_api_key, decrypt_api_key
        self.encrypt = encrypt_api_key
        self.decrypt = decrypt_api_key

    def test_round_trip_short_key(self):
        plaintext = "short-api-key"
        ciphertext = self.encrypt(plaintext)
        assert self.decrypt(ciphertext) == plaintext

    def test_round_trip_long_key(self):
        plaintext = "a" * 200  # DANDI keys can be long
        ciphertext = self.encrypt(plaintext)
        assert self.decrypt(ciphertext) == plaintext

    def test_ciphertext_is_not_plaintext(self):
        plaintext = "secret-dandi-api-token-12345"
        ciphertext = self.encrypt(plaintext)
        assert plaintext not in ciphertext

    def test_ciphertext_is_string(self):
        ciphertext = self.encrypt("any-key")
        assert isinstance(ciphertext, str)

    def test_each_encryption_produces_different_ciphertext(self):
        """AES-GCM uses a random nonce per encryption; two encryptions differ."""
        plaintext = "same-api-key"
        ct1 = self.encrypt(plaintext)
        ct2 = self.encrypt(plaintext)
        # Nonces differ, so ciphertexts should differ
        assert ct1 != ct2

    def test_decrypt_none_returns_none(self):
        """Decrypting None (unset key) returns None without raising."""
        assert self.decrypt(None) is None

    def test_encrypt_none_returns_none(self):
        """Encrypting None (unset key) returns None without raising."""
        assert self.encrypt(None) is None


@pytest.mark.no_db
class TestMissingEnvVar:
    """Calls without the master-key env var must raise a clear error."""

    @pytest.fixture(autouse=True)
    def _clear_env(self, monkeypatch):
        monkeypatch.delenv("NWB_DANDI_KEY_HEX", raising=False)
        from u19_pipeline.nwb_export.credentials_crypto import encrypt_api_key, decrypt_api_key
        self.encrypt = encrypt_api_key
        self.decrypt = decrypt_api_key

    def test_encrypt_raises_without_env(self):
        with pytest.raises(EnvironmentError):
            self.encrypt("some-key")

    def test_decrypt_raises_without_env(self):
        with pytest.raises(EnvironmentError):
            self.decrypt("some-encrypted-blob")

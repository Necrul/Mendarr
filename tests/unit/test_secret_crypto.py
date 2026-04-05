from app.crypto import decrypt_secret, encrypt_secret, is_encrypted_secret


def test_secret_round_trip_encryption():
    stored = encrypt_secret("super-secret-api-key")

    assert stored != "super-secret-api-key"
    assert is_encrypted_secret(stored) is True
    assert decrypt_secret(stored) == "super-secret-api-key"


def test_legacy_plaintext_secret_can_still_be_read():
    assert decrypt_secret("legacy-plaintext-key") == "legacy-plaintext-key"
    assert is_encrypted_secret("legacy-plaintext-key") is False

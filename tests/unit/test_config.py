import pytest

from app.config import DEFAULT_SECRET_KEY, validate_runtime_secret_key


def test_validate_runtime_secret_key_rejects_builtin_fallback():
    with pytest.raises(RuntimeError, match="built-in fallback"):
        validate_runtime_secret_key(DEFAULT_SECRET_KEY)


def test_validate_runtime_secret_key_rejects_short_values():
    with pytest.raises(RuntimeError, match="at least 16 characters"):
        validate_runtime_secret_key("too-short")


def test_validate_runtime_secret_key_accepts_custom_value():
    validate_runtime_secret_key("unit-test-secret-key-min-32-characters-long!")

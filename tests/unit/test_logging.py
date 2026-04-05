from app.logging import _mask_secrets


def test_mask_secrets_redacts_common_secret_fields():
    message = (
        'api_key=abc123 password="hunter2" token:xyz '
        'Authorization=Bearer jwt-token-value mendarr_session=session-cookie-value'
    )

    masked = _mask_secrets(message)

    assert "abc123" not in masked
    assert "hunter2" not in masked
    assert "jwt-token-value" not in masked
    assert "session-cookie-value" not in masked
    assert masked.count("***") >= 4

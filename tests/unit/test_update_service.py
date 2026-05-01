from app.services import update_service


def test_update_status_marks_current_release(monkeypatch):
    monkeypatch.setattr(
        update_service,
        "_fetch_latest_release",
        lambda repo: _async_result(
                {
                    "latest_version": "v1.0.2",
                    "release_url": f"https://github.com/{repo}/releases/tag/v1.0.2",
                    "error": None,
                }
            ),
    )
    cache_path = update_service._update_cache_path()
    if cache_path.exists():
        cache_path.unlink()

    status = _run(update_service.get_update_status())

    assert status["current_version"] == "v1.0.2"
    assert status["update_available"] is False
    assert status["status"] == "current"


def test_update_status_marks_update_available(monkeypatch):
    monkeypatch.setattr(
        update_service,
        "_fetch_latest_release",
        lambda repo: _async_result(
            {
                "latest_version": "v1.1.0",
                "release_url": f"https://github.com/{repo}/releases/tag/v1.1.0",
                "error": None,
            }
        ),
    )
    cache_path = update_service._update_cache_path()
    if cache_path.exists():
        cache_path.unlink()

    status = _run(update_service.get_update_status())

    assert status["update_available"] is True
    assert status["latest_version"] == "v1.1.0"
    assert status["status"] == "update_available"


def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


async def _async_result(value):
    return value

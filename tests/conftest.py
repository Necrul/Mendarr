import os
import tempfile
from pathlib import Path

# Ensure isolated settings before app import in tests
_tmp = Path(tempfile.mkdtemp(prefix="mendarr-test-"))
_db_path = _tmp / "test.db"
os.environ["MENDARR_DATA_DIR"] = str(_tmp)
os.environ["MENDARR_DATABASE_URL"] = f"sqlite+aiosqlite:///{_db_path.as_posix()}"
os.environ["MENDARR_SECRET_KEY"] = "unit-test-secret-key-min-32-characters-long!"
os.environ["MENDARR_ADMIN_PASSWORD"] = "test-admin-password-secure-123"

try:
    from app.config import get_settings

    get_settings.cache_clear()
except ImportError:
    pass


def extract_csrf_token(html: str) -> str:
    marker = 'name="csrf_token" value="'
    start = html.find(marker)
    if start == -1:
        raise AssertionError("CSRF token not found in HTML")
    start += len(marker)
    end = html.find('"', start)
    if end == -1:
        raise AssertionError("CSRF token terminator not found in HTML")
    return html[start:end]

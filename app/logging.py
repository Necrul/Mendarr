import logging
import re
import sys

from app.config import get_settings


_KEY_VALUE_PATTERNS = [
    re.compile(r"(api[_-]?key|ApiKey|X-Api-Key)([\"']?\s*[:=]\s*[\"']?)([^\"'\s&]+)", re.I),
    re.compile(r"(authorization)([\"']?\s*[:=]\s*[\"']?)(Bearer\s+[^\"'\s,;]+)", re.I),
    re.compile(r"(password|passwd|pwd|token|secret|cookie)([\"']?\s*[:=]\s*[\"']?)([^\"'\s&]+)", re.I),
]
_SESSION_COOKIE_PATTERN = re.compile(r"(mendarr_session=)([^;,\s]+)", re.I)


def _mask_secrets(msg: str) -> str:
    for pattern in _KEY_VALUE_PATTERNS:
        msg = pattern.sub(r"\1\2***", msg)
    msg = _SESSION_COOKIE_PATTERN.sub(r"\1***", msg)
    return msg


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _mask_secrets(record.msg)
        if record.args:
            record.args = tuple(_mask_secrets(str(a)) if isinstance(a, str) else a for a in record.args)
        return True


class IgnoreNoisyAccessLogsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        return "/api/scans/latest" not in message


def setup_logging() -> None:
    s = get_settings()
    level = getattr(logging, s.log_level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(level)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    h.addFilter(RedactingFilter())
    root.handlers.clear()
    root.addHandler(h)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").addFilter(IgnoreNoisyAccessLogsFilter())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

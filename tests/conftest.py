# Here exclusively to auto-modify pytest's python path
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest


# Configure logging
@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item):
    def sanitize_windows(s: str) -> str:
        REPLACABLE = '<>:"\\/|?*'
        REPLACEMENT = "-"

        for c in REPLACABLE:
            s = s.replace(c, REPLACEMENT)

        s = s.replace("\0", "")

        while True:
            old_s = s
            s = s.replace(REPLACEMENT * 2, REPLACEMENT)
            if old_s == s:
                break

        return s

    clean_name = sanitize_windows(item._request.node.name)
    filename = Path(
        __file__,
        "..",
        "__pytest-logs__",
        clean_name,
        f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}",
    ).resolve()
    safe_name = str(filename) + ".log"
    Path(safe_name).parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    loglevel_name: str = os.getenv("PYTEST-LOGLEVEL", "warning")
    loglevel: int = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }.get(loglevel_name.lower(), logging.WARNING)

    logger.setLevel(loglevel)
    f = logging.Formatter(
        fmt="%(levelname)s::%(asctime)s::%(name)s::%(filename)s:L%(lineno)d:\t%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    h = RotatingFileHandler(
        safe_name,
        encoding="utf8",
        maxBytes=100000,
        backupCount=-1,
    )
    h.setFormatter(f)
    h.setLevel(logging.DEBUG)
    logger.addHandler(h)
    # raise NotImplementedError((str(filename), str(safename)))
    yield

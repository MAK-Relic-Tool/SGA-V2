r"""TestCases for more explicit errors when providing invalid path arguments.

https://github.com/MAK-Relic-Tool/Issue-Tracker/issues/40
"""

import io
from argparse import ArgumentError
from contextlib import redirect_stderr
from pathlib import Path, PurePath
from typing import Iterable

import pytest


def _ArgumentError(name: str, message: str):
    _ = ArgumentError(None, message)
    _.argument_name = name
    return _


_F = Path(__file__)
_DIR = _F.parent
_DIR_THAT_DOESNT_EXIST = _DIR / "nonexistant_directory"
_ILLEGAL_PATH = _F / "illegal.sga"
_SGA_CREATE = _DIR / "created.sga"
_PH_SGA = "placeholder.sga"

_ARGS = [
    (
        ["sga", "pack", "v2", str(_DIR_THAT_DOESNT_EXIST), _PH_SGA],
        _ArgumentError(
            "manifest",
            f"The given path '{str(_DIR_THAT_DOESNT_EXIST)}' does not exist!",
        ),
    ),
    (
        ["sga", "pack", "v2", str(_DIR), "dummy"],
        _ArgumentError("manifest", f"The given path '{str(_DIR)}' is not a file!"),
    ),
    (
        ["sga", "pack", "v2", str(_DIR)],
        _ArgumentError("manifest", f"The given path '{str(_DIR)}' is not a file!"),
    ),
    (
        ["sga", "pack", "v2", str(_ILLEGAL_PATH)],
        _ArgumentError(
            "manifest", f"The given path '{str(_ILLEGAL_PATH)}' does not exist!"
        ),
    ),
]
_ARGS2 = [
    (
        [av.replace("\\", "/") for av in a],
        b,
    )  # _ArgumentError(b.argument_name,b.message.replace("\\", "/")))
    for (a, b) in _ARGS
]

_ = _ARGS2


@pytest.mark.parametrize(["args", "err"], [*_ARGS, *_ARGS2])
def test_argparse_error(args: Iterable[str], err: ArgumentError):
    from relic.core.cli import CLI

    try:
        _ = CLI.run_with(*args)
    except ArgumentError as exc:
        assert exc.argument_name == err.argument_name
        assert exc.message == err.message
    else:
        pytest.fail("No Error Raised")

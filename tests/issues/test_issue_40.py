r"""TestCases for more explicit errors when providing invalid path arguments.

https://github.com/MAK-Relic-Tool/Issue-Tracker/issues/40
"""

import io
from contextlib import redirect_stderr
from pathlib import Path, PurePath
from typing import Iterable

import pytest

_F = Path(__file__)
_DIR = _F.parent
_DIR_THAT_DOESNT_EXIST = _DIR / "nonexistant_directory"
_ILLEGAL_PATH = _F / "illegal.sga"
_SGA_CREATE = _DIR / "created.sga"
_PH_SGA = "placeholder.sga"

_ARGS = [
    (
        ["sga", "pack", "v2", str(_DIR_THAT_DOESNT_EXIST), _PH_SGA],
        f"error: argument manifest: The given path '{str(_DIR_THAT_DOESNT_EXIST)}' does not exist!",
    ),
    (
        ["sga", "pack", "v2", str(_DIR), "dummy"],
        f"error: argument manifest: The given path '{str(_DIR)}' is not a file!",
    ),
    (
        ["sga", "pack", "v2", str(_DIR)],
        f"error: argument manifest: The given path '{str(_DIR)}' is not a file!",
    ),
    (
        ["sga", "pack", "v2", str(_ILLEGAL_PATH)],
        f"error: argument manifest: The given path '{str(_ILLEGAL_PATH)}' does not exist!",
    ),
]
_ARGS2 = [
    ([av.replace("\\", "/") for av in a], str(PurePath(b.replace("\\", "/"))))
    for (a, b) in _ARGS
]

_ = _ARGS2


@pytest.mark.parametrize(["args", "msg"], [*_ARGS, *_ARGS2])
def test_argparse_error(args: Iterable[str], msg: str):
    from relic.core.cli import CLI

    with io.StringIO() as f:
        with redirect_stderr(f):
            status = CLI.run_with(*args)
            assert status == 2
        f.seek(0)
        err = f.read()

        if msg not in err:  # Dumb, but helps avoid bloat
            print(err)
        assert msg in err

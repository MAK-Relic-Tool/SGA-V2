r"""
TestCases for more explicit errors when providing invalid path arguments.
https://github.com/MAK-Relic-Tool/Issue-Tracker/issues/40
"""
import io
from contextlib import redirect_stderr
from pathlib import Path
from typing import Iterable

import pytest

_F = Path(__file__)
_DIR = _F.parent
_DIR_THAT_DOESNT_EXIST = _DIR / "nonexistant_directory"
_SGA_CREATE = _DIR / "created.sga"
_PH_SGA = "placeholder.sga"
_PH_CONFIG = "placeholder_config.json"

_ARGS = [
    (
        ["sga", "pack", "v2", str(_DIR_THAT_DOESNT_EXIST), _PH_SGA, _PH_CONFIG],
        f"error: argument src_dir: The given path '{str(_DIR_THAT_DOESNT_EXIST)}' does not exist!",
    ),
    (
        ["sga", "pack", "v2", str(_F), "dummy", "dummy"],
        f"error: argument src_dir: The given path '{str(_F)}' is not a directory!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            str(_DIR),
            str(_DIR),
            _PH_CONFIG,
        ],
        f"error: argument out_sga: The given path '{str(_DIR)}' is not a file!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            str(_DIR),
            str(_SGA_CREATE),
            _PH_CONFIG,
        ],
        f"error: argument config_file: The given path '{str(_PH_CONFIG)}' does not exist!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            str(_DIR),
            str(_SGA_CREATE),
            str(_DIR),
        ],
        f"error: argument config_file: The given path '{str(_DIR)}' is not a file!",
    ),
]
_ARGS2 = [
    ([av.replace("\\", "/") for av in a], b.replace("\\", "/")) for (a, b) in _ARGS
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

        if msg not in err:  # Dumb, but helps avoid blaot
            print(err)
        assert msg in err

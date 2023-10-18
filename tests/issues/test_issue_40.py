r"""
TestCases for more explicit errors when providing invalid path arguments.
https://github.com/MAK-Relic-Tool/Issue-Tracker/issues/40
"""
import io
import os.path
from collections import Sequence
from contextlib import redirect_stderr

import pytest

_ARGS = [
    (
        ["sga", "pack", "v2", f"{__file__}/nonexistant_dir", "dummy", "dummy"],
        f"error: argument src_dir: The given path '{__file__}/nonexistant_dir' does not exist!",
    ),
    (
        ["sga", "pack", "v2", __file__, "dummy", "dummy"],
        f"error: argument src_dir: The given path '{__file__}' is not a directory!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            f"{__file__}/..",
            os.path.abspath(f"{__file__}/.."),
            "dummy",
        ],
        f"error: argument out_sga: The given path '{os.path.abspath(f'{__file__}/..')}' is not a file!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            f"{__file__}/..",
            os.path.abspath(f"{__file__}/../newsga.sga"),
            "dummy",
        ],
        f"error: argument config_file: The given path '{'dummy'}' does not exist!",
    ),
    (
        [
            "sga",
            "pack",
            "v2",
            f"{__file__}/..",
            os.path.abspath(f"{__file__}/../newsga.sga"),
            os.path.abspath(f"{__file__}/.."),
        ],
        f"error: argument config_file: The given path '{os.path.abspath(f'{__file__}/..')}' is not a file!",
    ),
]


@pytest.mark.parametrize(["args", "msg"], _ARGS)
def test_argparse_error(args: Sequence[str], msg: str):
    from relic.core.cli import cli_root

    with io.StringIO() as f:
        with redirect_stderr(f):
            status = cli_root.run_with(*args)
            assert status == 2
        f.seek(0)
        err = f.read()
        print(err)
        assert msg in err

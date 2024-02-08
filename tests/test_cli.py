import io
import os.path
import subprocess

# Local testing requires running `pip install -e "."`
import tempfile
from contextlib import redirect_stdout
from typing import Sequence

import fs
import pytest
from fs.base import FS
from fs.info import Info

from relic.core.cli import CLI


class CommandTests:
    def test_run(self, args: Sequence[str], output: str, exit_code: int):
        _args = ["relic", *args]
        cmd = subprocess.run(_args, capture_output=True, text=True)
        result = cmd.stdout
        status = cmd.returncode
        print(f"'{result}'")  # Visual Aid for Debugging
        assert output in result
        assert status == exit_code

    def test_run_with(self, args: Sequence[str], output: str, exit_code: int):

        with io.StringIO() as f:
            with redirect_stdout(f):
                status = CLI.run_with(*args)
            f.seek(0)
            result = f.read()
            print(f"'{result}'")  # Visual Aid for Debugging
            assert output in result
            assert status == exit_code


_SGA_PACK_HELP = ["sga", "pack", "-h"], """usage: relic sga pack [-h] {v2} ...""", 0
_SGA_PACK_v2_HELP = (
    ["sga", "pack", "v2", "-h"],
    """sage: relic sga pack v2 [-h] src_dir out_sga config_file""",
    0,
)

_TESTS = [_SGA_PACK_HELP, _SGA_PACK_v2_HELP]
_TEST_IDS = [" ".join(_[0]) for _ in _TESTS]


@pytest.mark.parametrize(["args", "output", "exit_code"], _TESTS, ids=_TEST_IDS)
class TestRelicSgaCli(CommandTests):
    ...


def _get_sample_file(path: str):
    return os.path.abspath(os.path.join(__file__, "../data", path))


_SAMPLE_V2 = _get_sample_file("SampleSGA-v2.sga")
_SAMPLE_V2_OCT_15_2023 = _get_sample_file("SampleSGA-v2-Oct-15-2023.sga")
_SAMPLES = [_SAMPLE_V2, _SAMPLE_V2_OCT_15_2023]


@pytest.mark.parametrize("src", argvalues=_SAMPLES, ids=_SAMPLES)
@pytest.mark.skip(reason="Old Manifest syntax")
def test_cli_unpack_pack_one_to_one(src: str):
    cfg = """{
    "test": {
      "name": "Sample Data",
      "solvers": [
        {
          "match": "STORE.txt",
          "storage": "store"
        },
        {
          "match": "BUFFER.txt",
          "storage": "buffer"
        },
        {
          "match": "STREAM.txt",
          "storage": "stream"
        }
      ]
    }
}"""

    with tempfile.TemporaryDirectory() as temp_dir:
        CLI.run_with("sga", "unpack", src, temp_dir)
        cfg_file_name = None
        repacked_file_name = None
        try:
            with tempfile.NamedTemporaryFile("w+", delete=False) as config_file:
                config_file.write(cfg)
                cfg_file_name = config_file.name
            with tempfile.NamedTemporaryFile("rb", delete=False) as repacked:
                repacked_file_name = repacked.name

            status = CLI.run_with(
                "sga", "pack", "v2", temp_dir, repacked_file_name, cfg_file_name
            )
            assert status == 0

            def check_against(src: FS, dest: FS):
                for root, _, files in src.walk():
                    assert dest.exists(root)
                    with dest.opendir(root) as root_folder:
                        for file in files:
                            file: Info
                            assert root_folder.exists(file.name)

                    for file in files:
                        file: Info
                        path = fs.path.join(root, file.name)
                        with src.openbin(path) as src_file:
                            with dest.openbin(path) as dest_file:
                                src_data = src_file.read()
                                dest_data = dest_file.read()
                                assert dest_data == src_data

            with fs.open_fs(f"sga://{src}") as src_sga:
                with fs.open_fs(f"sga://{repacked_file_name}") as dst_sga:
                    check_against(dst_sga, src_sga)

        finally:
            try:
                os.unlink(cfg_file_name)
            except:
                ...
            try:
                os.unlink(repacked_file_name)
            except:
                ...


@pytest.mark.parametrize("src", argvalues=_SAMPLES, ids=_SAMPLES)
def test_cli_repack(src: str):

    repacked_file_name = None
    try:
        with tempfile.NamedTemporaryFile("w+", delete=False) as config_file:
            repacked_file_name = config_file.name

        status = CLI.run_with("sga", "repack", "v2", src, repacked_file_name)
        assert status == 0
    finally:
        try:
            os.unlink(repacked_file_name)
        except:
            ...

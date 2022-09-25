import json
from io import BytesIO
from pathlib import Path
from typing import List, Iterable, BinaryIO

import pytest
from fs.base import FS
from relic.sga.core import MagicWord, Version
from relic.sga.core.filesystem import SGAFS
from relic.sga import v2
from relic.sga.v2 import SGAFSIO, ArchiveIO

_path = Path(__file__).parent
try:
    path = _path / "sources.json"
    with path.open() as stream:
        file_sources = json.load(stream)
except IOError as e:
    file_sources = {}

if "dirs" not in file_sources:
    file_sources["dirs"] = []

__implicit_test_data = str(_path / "data")

if __implicit_test_data not in file_sources["dirs"]:
    file_sources["dirs"].append(__implicit_test_data)


def v2_scan_directory(root_dir: str) -> Iterable[str]:
    root_directory = Path(root_dir)
    for path_object in root_directory.glob('**/*.sga'):
        with path_object.open("rb") as handle:
            if not MagicWord.check_magic_word(handle, advance=True):
                continue
            version = Version.unpack(handle)
            if version != v2.version:
                continue
            # if path_object.with_suffix(".json").exists():  # ensure expected results file is also present
            yield str(path_object)


v2_test_files: List[str] = []

for dir in file_sources.get("dirs", []):
    results = v2_scan_directory(dir)
    v2_test_files.extend(results)
v2_test_files.extend(file_sources.get("files", []))

v2_test_files = list(set(v2_test_files))  # Get unique paths


class TestArchive:
    @pytest.fixture(params=v2_test_files)
    def v2_file_stream(self, request) -> BinaryIO:
        v2_file: str = request.param
        # p = Path(v2_file)
        # p = p.with_suffix('.json')

        # with open(p, "r") as data:
        #     lookup: Dict[str, str] = json.load(data)
        #     coerced_lookup: Dict[int, str] = {int(key): value for key, value in lookup.items()}

        with open(v2_file, "rb") as v2_handle:
            data = v2_handle.read()

        return BytesIO(data)

    def test_read(self, v2_file_stream):
        v2_stream = v2_file_stream
        ArchiveIO.read(v2_stream)


class TestSGAFS:
    @pytest.fixture(params=v2_test_files)
    def v2_file_stream(self, request) -> BinaryIO:
        v2_file: str = request.param
        # p = Path(v2_file)
        # p = p.with_suffix('.json')

        # with open(p, "r") as data:
        #     lookup: Dict[str, str] = json.load(data)
        #     coerced_lookup: Dict[int, str] = {int(key): value for key, value in lookup.items()}

        with open(v2_file, "rb") as v2_handle:
            data = v2_handle.read()

        return BytesIO(data)

    def test_read(self, v2_file_stream):
        v2_stream = v2_file_stream
        with SGAFSIO.read(v2_stream) as sga:
            pass
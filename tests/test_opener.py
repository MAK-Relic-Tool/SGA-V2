import json
import zlib
from pathlib import Path
from typing import List, Iterable, Any, Dict

import fs
import pytest
from relic.sga.core import MagicWord, Version, StorageType
from relic.sga.core.filesystem import EssenceFS

from relic.sga import v2

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
    for path_object in root_directory.glob("**/*.sga"):
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


class TestEssenceFSOpener:
    @pytest.fixture(params=v2_test_files)
    def v2_file_path(self, request) -> str:
        v2_file: str = request.param
        return v2_file

    def test_read(self, v2_file_path):
        with fs.open_fs(f"sga://{v2_file_path}") as sga:
            pass


# Hack to get "SampleSGA-v2" from the sample data
_sample_path_on_disk = [f for f in v2_test_files if "SampleSGA-v2.sga" in f][0]
_sample_drives = ["test"]
_MODIFIED = 1697739416
_sample_data = b"Ready to unleash 11 barrels of lead.\nWhere's that artillery?!?!\nOrks are da biggust and da strongest.\nFix bayonets!\nFear me, but follow!\nCall for an earth-shaker?\nMy mind is too weary to fight on...\nWe'll be off as soon as the fuel arrives.\nWhere are those tech priests.\nFire until they see the glow of our barrels!"
_CRC32 = zlib.crc32(_sample_data).to_bytes(4, "little", signed=False)
_store_txt = {
    "path": "test:/String Samples/STORE.txt",
    "namespaces": ["basic", "essence"],
    "info": {
        "basic": {"is_dir": False, "name": "STORE.txt"},
        "essence": {
            "storage_type": int(StorageType.STORE),
            "name": "STORE.txt",
            "modified": _MODIFIED,
            "crc32": _CRC32,
        },
    },
    "data": _sample_data,
}
_buffer_txt = {
    "path": "test:/String Samples/BUFFER.txt",
    "namespaces": ["basic", "essence"],
    "info": {
        "basic": {"is_dir": False, "name": "BUFFER.txt"},
        "essence": {
            "storage_type": int(StorageType.BUFFER_COMPRESS),
            "name": "BUFFER.txt",
            "modified": _MODIFIED,
            "crc32": _CRC32,
        },
    },
    "data": _sample_data,
}
_stream_txt = {
    "path": "test:/String Samples/STREAM.txt",
    "namespaces": ["basic", "essence"],
    "info": {
        "basic": {"is_dir": False, "name": "STREAM.txt"},
        "essence": {
            "storage_type": int(StorageType.STREAM_COMPRESS),
            "name": "STREAM.txt",
            "modified": _MODIFIED,
            "crc32": _CRC32,
        },
    },
    "data": _sample_data,
}

# Non-Exhaustive
_sample_paths = [
    "test:/String Samples",
    "test:/String Samples/STORE.txt",
    "test:/String Samples/BUFFER.txt",
    "test:/String Samples/STREAM.txt",
]
_sample_file_descriptions = [_store_txt]
_sample_meta = [
    {
        "name": "SampleSGA-v2",
        "file_md5": "facecd6df030c790e8bee02bcd7728a6",
        "header_md5": "ba4d641eb08a4c9fc95e4b3bf61cf390",
        "version": {"major": 2, "minor": 0},
    }
]


class TestSampleSGAv2:
    @pytest.fixture(params=[_sample_path_on_disk])
    def sga_path(self, request) -> str:
        return request.param

    @pytest.fixture(params=[_sample_drives])
    def drives(self, request) -> List[str]:
        return request.param

    @pytest.fixture(params=[*_sample_paths])
    def file_path(self, request) -> str:
        return request.param

    @pytest.fixture(params=[*_sample_file_descriptions])
    def file_descriptor(self, request) -> Dict[str, Any]:
        return request.param

    @pytest.fixture(params=[*_sample_meta])
    def meta(self, request) -> Dict[str, Any]:
        return request.param

    @staticmethod
    def _open_fs(_path: str) -> EssenceFS:
        sga: EssenceFS
        sga = fs.open_fs(f"sga://{_path}")
        return sga

    def test_open_fs(self, sga_path):
        with self._open_fs(sga_path) as _:
            pass

    def test_drives(self, sga_path, drives: List[str]):
        with self._open_fs(sga_path) as sga:
            unique_drives_in_fs = set([name for (name, _) in sga.iterate_fs()])
            unique_drives = set(drives)
            assert unique_drives_in_fs == unique_drives

    def test_path_exists(self, sga_path: str, file_path: str):
        with self._open_fs(sga_path) as sga:
            if not sga.exists(file_path):
                raise FileNotFoundError(file_path)

    def test_file_data(self, sga_path: str, file_descriptor: Dict[str, Any]):
        file_path = file_descriptor["path"]
        expected_data = file_descriptor["data"]
        with self._open_fs(sga_path) as sga:
            with sga.open(file_path, "rb") as handle:
                data = handle.read()
                assert expected_data == data

    def test_file_info(self, sga_path: str, file_descriptor: Dict[str, Any]):
        file_path = file_descriptor["path"]
        namespaces = file_descriptor["namespaces"]
        expected_info = file_descriptor["info"]

        with self._open_fs(sga_path) as sga:
            info = sga.getinfo(file_path, namespaces)
            assert info.raw == expected_info

    def test_sga_info(self, sga_path: str, meta: Dict[str, Any]):
        with self._open_fs(sga_path) as sga:
            info = sga.getmeta("essence")
            assert info == meta

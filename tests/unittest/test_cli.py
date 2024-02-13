from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Collection
from typing import Dict, Any

import fs
import pytest
from fs.base import FS
from fs.errors import ResourceNotFound
from fs.glob import GlobMatch
from fs.osfs import OSFS
from fs.subfs import SubFS
from relic.core import CLI
from relic.sga.core.hashtools import crc32

from relic.sga.v2 import EssenceFSV2
from relic.sga.v2.serialization import RelicDateTimeSerializer
from utils import create_temp_dataset_fs, get_datasets

_DATASETS = get_datasets()


@dataclass
class ManifestFileInfo:
    modified: datetime | None
    crc: int | None
    drive: str | None
    archive_path: str | None

    @classmethod
    def parse(cls, **kwargs: Any) -> ManifestFileInfo:
        _MODIFIED = "modified"

        if _MODIFIED in kwargs:
            kwargs[_MODIFIED] = RelicDateTimeSerializer.unix2datetime(kwargs[_MODIFIED])

        return cls(**kwargs)


@dataclass
class Manifest:
    toc: Dict[str, Dict[str, ManifestFileInfo]]

    @classmethod
    def parse(cls, **kwargs: Any) -> Manifest:
        _FILES = "files"

        if _FILES in kwargs:
            kwargs[_FILES] = {
                drive: {
                    path: ManifestFileInfo.parse(**info)
                    for path, info in file_manifest.items()
                }
                for drive, file_manifest in kwargs[_FILES].items()
            }

        return cls(**kwargs)


def load_manifest(dataset: str, manifest: str) -> Manifest | None:
    with OSFS(dataset) as fs:
        try:
            with fs.opendir("Meta") as meta_dir:
                if not meta_dir.exists(manifest):
                    return None
                data = json.loads(meta_dir.gettext(manifest))
                return Manifest.parse(**data)
        except ResourceNotFound:
            return None


@pytest.mark.parametrize("dataset", _DATASETS)
class TestCLI:
    _FILE_MANIFEST = "manifest.json"

    def _pack(self, filesystem: FS, arciv: GlobMatch):
        sys_path = filesystem.getsyspath(arciv.path)
        packed_name = "Packed"
        sga_name, _ = os.path.splitext(arciv.path)
        packed_path = fs.path.join(packed_name, sga_name + ".sga")
        packed_sys_path = os.path.join(os.path.dirname(sys_path), packed_path[1:])
        CLI.run_with("relic", "sga", "pack", "v2", sys_path, packed_sys_path)
        return packed_path, packed_sys_path

    def _unpack(self, filesystem: FS, sga: GlobMatch):
        sys_path = filesystem.getsyspath(sga.path)
        dump_name = "Unpacked"
        sga_name, _ = os.path.splitext(sga.path)
        dump_path = fs.path.join(sga_name, dump_name)
        dump_sys_path = os.path.join(os.path.dirname(sys_path), dump_path[1:])
        CLI.run_with("relic", "sga", "unpack", sys_path, dump_sys_path)
        return dump_path, dump_sys_path

    def _validate_osfs_equal(self, src: FS, dst: FS, manifest: Dict[str, Any]):
        for file, meta in manifest.get("files", {}).items():
            assert dst.exists(file), f"File `{file}` doesn't exist"

            src_data = src.getbytes(file)
            dst_data = dst.getbytes(file)

            assert dst_data == src_data, "Unpacking data mismatch!"

            # TODO
            # My pipeline for modified seems to be super broken
            # Probably due to git management?
            # src_info = src.getinfo(file, ["details"])
            # dst_info = dst.getinfo(file, ["details"])
            #
            # src_mod_time = src_info.modified
            # dst_mod_time = dst_info.modified
            # print(file, RelicDateTimeSerializer.datetime2unix(dst_mod_time))
            # if "modified" not in meta:
            #     # We assume that the FS has not touched modtime
            #     assert dst_mod_time == src_mod_time, "Modified Time mismatch"
            # else:
            #     # We assume that the FS has touched modtime, so we use the manifest AND the FS
            #     #   This allows us to have 'stale' manifest files in the event the SGA is recreated
            #     #   But the manifest was not updated
            #     #   This shouldn't be a major issue; since files typically shouldn't be added to a test case
            #     #   after the dataset is created
            #     man_mod_time = RelicDateTimeSerializer.unix2datetime(meta["modified"])
            #     assert dst_mod_time in [
            #         man_mod_time.replace(microsecond=0),
            #         src_mod_time.replace(microsecond=0),
            #     ], "Modified Time mismatch"

            if "crc" in meta:
                dst_buf = dst.getbytes(file)
                man_crc = meta["crc"]
                dst_crc = crc32(dst_buf)
                assert dst_crc == man_crc, "CRC32 Mismatch"

    def _validate_sgafs_equal_osfs_onedrive(
        self, osfs: FS, sgafs: EssenceFSV2, manifest: Collection[str]
    ):
        for file in manifest:
            assert sgafs.exists(file) is True, f"Missing `{file}` in archive"
            assert osfs.exists(file) is True, f"Missing `{file}` on disk"

            src_data = sgafs.getbytes(file)
            dst_data = osfs.getbytes(file)

            assert src_data == dst_data, "Data mismatch!"

            src_info = sgafs.getinfo(file, ["details"])
            dst_info = osfs.getinfo(file, ["details"])
            src_mod = src_info.modified
            dst_mod = dst_info.modified

            assert src_mod.replace(microsecond=0) == dst_mod.replace(
                microsecond=0
            ), "MTime Mismatch"

    @contextmanager
    def _open_folder(self, fs: FS, folder_name: str) -> SubFS[FS]:
        if not fs.exists(folder_name):
            pytest.skip(f"'{folder_name}' folder was not found; skipping validation")

        with fs.opendir(folder_name) as dir:
            yield dir

    def test_pack(self, dataset: str) -> None:
        """Tests that the CLI Pack function runs without error."""
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            for arciv in tmp_fs.glob("**/*.arciv"):
                self._pack(tmp_fs, arciv)

    def test_unpack(self, dataset: str) -> None:
        """Tests that the CLI Unpack function runs without error."""
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            for sga in tmp_fs.glob("**/*.sga"):
                self._unpack(tmp_fs, sga)

    def test_pack_validity(self, dataset: str) -> None:
        """Tests that the CLI Pack function runs without error."""
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            manifest = self.load_manifest(tmp_fs, self._FILE_MANIFEST)

            for arciv in tmp_fs.glob("**/*.arciv"):
                _, packed_sys_path = self._pack(tmp_fs, arciv)

                try:
                    with tmp_fs.opendir("Root") as src:
                        with fs.open_fs(f"sga://{packed_sys_path}") as sgafs:
                            self._validate_sgafs_equal_osfs_onedrive(
                                src, sgafs, manifest.get("files", {}).keys()
                            )
                except:
                    tmp_fs.tree()
                    raise

    def load_manifest(self, fs: FS, manifest: str) -> Dict[str, Dict[str, Any]]:
        with self._open_folder(fs, "Meta") as meta_dir:
            if not meta_dir.exists(manifest):
                pytest.skip(
                    f"'Meta/{manifest}' file was not found; skipping validation"
                )
            return json.loads(meta_dir.gettext(manifest))

    def test_unpack_validity(self, dataset: str) -> None:
        """Tests that the CLI Unpack properly extracts all files in the SGA."""
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            manifest = self.load_manifest(tmp_fs, self._FILE_MANIFEST)

            for sga in tmp_fs.glob("**/*.sga"):
                dump_path, _ = self._unpack(tmp_fs, sga)
                try:
                    with tmp_fs.opendir("Root") as src:
                        with tmp_fs.opendir(dump_path) as dst:
                            self._validate_osfs_equal(src, dst, manifest)
                except:
                    tmp_fs.tree()
                    raise

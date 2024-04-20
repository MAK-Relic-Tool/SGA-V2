from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Dict, Any

import fs
import pytest
from fs.base import FS
from fs.glob import GlobMatch
from fs.subfs import SubFS
from relic.core import CLI

from tests.unittest.assert_helpers import (
    validate_osfs_equal,
    validate_sgafs_equal_osfs_onedrive,
)
from utils import create_temp_dataset_fs, get_datasets, Manifest, safe_open_folder

_DATASETS = get_datasets()


def test_pack_withlocal_path():
    result = CLI.run_with("relic", "sga", "pack", "v2", "test.arciv", "test.sga")


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
                            validate_sgafs_equal_osfs_onedrive(
                                src, sgafs, manifest.get("files", {}).keys()
                            )
                except:
                    tmp_fs.tree()
                    raise

    def load_manifest(self, fs: FS, manifest: str) -> Manifest:
        with safe_open_folder(fs, "Meta") as meta_dir:
            if not meta_dir.exists(manifest):
                pytest.skip(
                    f"'Meta/{manifest}' file was not found; skipping validation"
                )
            data = json.loads(meta_dir.gettext(manifest))
            return Manifest.parse(**data)

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
                            validate_osfs_equal(src, dst, manifest)
                except:
                    tmp_fs.tree()
                    raise

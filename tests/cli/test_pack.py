import json
import os
from typing import List

import fs
from fs.base import FS
from fs.glob import GlobMatch

from relic.sga.v2.sgafs import SgaFsV2
from utils import create_temp_dataset_fs, get_dataset_path
import pytest
from relic.core.cli import cli_root


_DATASETS = [
    get_dataset_path("sample-10-26-2023")
]

@pytest.mark.parametrize(
    "dataset",_DATASETS
)
class TestCLI:
    def _pack(self, filesystem:FS,arciv:GlobMatch):
        sys_path = filesystem.getsyspath(arciv.path)
        packed_name = "Packed"
        sga_name, _ = os.path.splitext(arciv.path)
        packed_path = fs.path.join(packed_name, sga_name + ".sga")
        packed_sys_path = os.path.join(os.path.dirname(sys_path), packed_path[1:])
        cli_root.run_with("relic", "sga", "pack", "v2", sys_path, packed_sys_path)
        return packed_path, packed_sys_path

    def _unpack(self, filesystem:FS, sga:GlobMatch):
        sys_path = filesystem.getsyspath(sga.path)
        dump_name = "Unpacked"
        sga_name, _ = os.path.splitext(sga.path)
        dump_path = fs.path.join(sga_name, dump_name)
        dump_sys_path = os.path.join(os.path.dirname(sys_path), dump_path[1:])
        cli_root.run_with("relic", "sga", "unpack", sys_path, dump_sys_path)
        return dump_path, dump_sys_path


    def _validate_osfs_equal(self, src:FS, dst:FS, manifest:List[str]):
        for file in manifest:
            assert dst.exists(file)

            src_data = src.getbytes(file)
            dst_data = dst.getbytes(file)

            assert src_data == dst_data, "Unpacking data mismatch!"

            src_info = src.getinfo(file, ["details"])
            dst_info = dst.getinfo(file, ["details"])

            assert src_info.modified.replace(microsecond=0) == dst_info.modified.replace(microsecond=0)

    def _validate_sgafs_equal_osfs_onedrive(self, osfs:FS,sgafs:SgaFsV2, manifest:List[str]):
        for file in manifest:
            assert sgafs.exists(file)
            assert osfs.exists(file)

            src_data = sgafs.getbytes(file)
            dst_data = osfs.getbytes(file)

            assert src_data == dst_data, "Data mismatch!"

            src_info = sgafs.getinfo(file, ["details"])
            dst_info = osfs.getinfo(file, ["details"])
            src_mod = src_info.modified
            dst_mod = dst_info.modified

            assert src_mod.replace(microsecond=0) == dst_mod.replace(microsecond=0)


    def test_pack(self, dataset: str) -> None:
        """
        Tests that the CLI Pack function runs without error
        """
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            for arciv in tmp_fs.glob("**/*.arciv"):
                self._pack(tmp_fs,arciv)
    def test_unpack(self, dataset: str) -> None:
        """
        Tests that the CLI Unpack function runs without error
        """
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            for sga in tmp_fs.glob("**/*.sga"):
                self._unpack(tmp_fs,sga)


    def test_pack_validity(self, dataset: str) -> None:
        """
        Tests that the CLI Pack function runs without error
        """
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            with tmp_fs.opendir("Meta") as meta_dir:
                files = json.loads(meta_dir.gettext("file_manifest.json"))

            for arciv in tmp_fs.glob("**/*.arciv"):
                _, packed_sys_path = self._pack(tmp_fs,arciv)

                try:
                    with tmp_fs.opendir("Root") as src:
                        with fs.open_fs(f"sga://{packed_sys_path}") as sgafs:
                            self._validate_sgafs_equal_osfs_onedrive(src, sgafs, files)
                except:
                    tmp_fs.tree()
                    raise



    def test_unpack_validity(self, dataset: str) -> None:
        """
        Tests that the CLI Unpack properly extracts all files in the SGA
        """
        tmp_fs: FS
        with create_temp_dataset_fs(dataset) as tmp_fs:
            with tmp_fs.opendir("Meta") as meta_dir:
                files = json.loads(meta_dir.gettext("file_manifest.json"))

            for sga in tmp_fs.glob("**/*.sga"):
                dump_path, _ = self._unpack(tmp_fs,sga)
                try:
                    with tmp_fs.opendir("Root") as src:
                        with tmp_fs.opendir(dump_path) as dst:
                            self._validate_osfs_equal(src,dst, files)
                except:
                    tmp_fs.tree()
                    raise
from __future__ import annotations

from typing import Dict, Any, Collection

from fs.base import FS
from fs.osfs import OSFS
from relic.sga.core.hashtools import crc32

from relic.sga.v2 import EssenceFSV2
from tests.unittest.utils import Manifest


def validate_osfs_equal(src: OSFS, dst: OSFS, manifest: Manifest):
    for drive, files in manifest.toc.items():
        for file, meta in files.items():

            archive_file = meta.archive_path or file

            assert src.exists(file), f"File `{file}` doesn't exist on disk!"
            assert dst.exists(
                archive_file
            ), f"File `{archive_file}` doesn't exist in drive ({drive})!"

            src_data = src.getbytes(file)
            dst_data = dst.getbytes(file)

            assert dst_data == src_data, "Unpacking data mismatch!"

            if "crc" in meta:
                man_crc = meta["crc"]
                dst_crc = crc32(dst_data)
                assert dst_crc == man_crc, "CRC32 Mismatch"

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


def validate_sgafs_equal_osfs_onedrive(
    osfs: FS, sgafs: EssenceFSV2, manifest: Collection[str]
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

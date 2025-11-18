import logging
from contextlib import contextmanager
from typing import BinaryIO

from relic.sga.v2.fsspec_.main import SgaV2 as SgaFsSpec, _Directory, _File
from relic.sga.v2.native.writer import _SgaWriter
from relic.sga.v2.serialization import SgaV2GameFormat


class FsSpecWriter(_SgaWriter[SgaFsSpec]):
    def __init__(
        self,
        out: BinaryIO,
        game_format: SgaV2GameFormat = SgaV2GameFormat.Unknown,
        logger: logging.Logger | None = None,
    ):
        super().__init__(out, game_format, logger)

    @contextmanager
    def _writeback(self, ptr: int, stream: BinaryIO):
        previous = stream.tell()
        stream.seek(ptr)
        yield
        stream.seek(previous)

    def _add_fs_names(self, drive: _Directory, cur_folder: _Directory):
        folders = sorted(cur_folder.sub_folders.values(), key=lambda x: x.name)
        for folder in folders:
            self._add_name(drive.name, folder.absolute_path)
        for folder in folders:
            self._add_fs_names(drive, folder)

    def _add_fs_file(
        self, drive: _Directory, file: _File, game_format: SgaV2GameFormat
    ):
        name = file.name
        modified = file.modified
        storage_type = file.storage_type
        name_offset = self._add_name(drive.name, name)
        full_path = file.absolute_path

        if file._lazy is not None:
            raise NotImplementedError  # ugh... we gotta handle this!
        data = file._mem

        data_offset, (decomp_size, comp_size) = self._add_data(
            full_path, data, storage_type, modified
        )
        self._add_toc_file(
            name_offset, storage_type, data_offset, comp_size, decomp_size, game_format
        )

    def _add_fs_folder(
        self,
        drive: _Directory,
        folder: _Directory,
        game_format: SgaV2GameFormat,
        writeback_ptr: int | None = None,
    ):
        full_path = folder.absolute_path
        name_offset = self._add_name(drive.name, full_path)

        if writeback_ptr is None:  # write a faker!
            writeback_ptr = self._folder_stream.tell()
            self._add_toc_folder(name_offset, 0, 0, 0, 0)

        folder_start = self._folder_count

        sub_folders: list[tuple[int, _Directory]] = []
        for sub_folder in folder.sub_folders.values():
            sub_folder_wb = self._folder_stream.tell()
            self._add_toc_folder(0, 0, 0, 0, 0)  # blank folder
            _info = sub_folder_wb, sub_folder
            sub_folders.append(_info)

        folder_end = self._folder_count

        for sub_writeback, sub_folder in sub_folders:
            self._add_fs_folder(drive, sub_folder, game_format, sub_writeback)

        file_start = self._file_count
        for file in folder.files.values():
            self._add_fs_file(drive, file, game_format)
        file_end = self._file_count

        with self._writeback(writeback_ptr, self._folder_stream):
            self._add_toc_folder(
                name_offset, folder_start, folder_end, file_start, file_end
            )

    def _add_fs_drive(self, drive: _Directory, game_format: SgaV2GameFormat):

        # Write all names to init cache & to mimic modpackager's name layout
        self._add_fs_names(drive)
        # Collect known arguments
        name = drive.name
        alias = drive.alias
        folder_root = folder_start = self._folder_count
        file_start = self._file_count
        folder_name_offset = 0  # self._get_name_offset(name,)

        # Create blank root folder
        root_folder_ptr = self._folder_stream.tell()
        self._add_toc_folder(folder_name_offset, folder_start, 0, file_start, 0)

        # Walk Tree
        self._add_fs_folder(
            drive, drive, game_format
        )  # will not have the correct END arguments due to nesting

        # Collect new arguments
        folder_end = self._folder_count
        file_end = self._file_count

        # Fix blank root folder
        with self._writeback(root_folder_ptr, self._folder_stream):
            self._add_toc_folder(
                folder_name_offset, folder_start, folder_end, file_start, file_end
            )

        # Finish by writing TOC Entry
        self._add_toc_drive(
            name, alias, folder_start, folder_end, file_start, file_end, folder_root
        )

    def _add_toc(self, sga: SgaFsSpec):
        self._logger.debug("Determining DoW/IC")
        game_format = self._resolve_desired_format(sga._meta)

        self._logger.debug("Adding entries to TOC")
        for drive in sga._iter_drives():
            self._add_fs_drive(drive, game_format)

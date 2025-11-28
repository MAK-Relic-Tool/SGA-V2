import logging
from contextlib import contextmanager
from dataclasses import dataclass
from os import PathLike
from typing import BinaryIO

from relic.sga.v2.fsspec_.main import SgaV2 as SgaFsSpec, _Directory, _File
from relic.sga.v2.native.writer import _SgaWriter
from relic.sga.v2.serialization import SgaV2GameFormat


class FsSpecWriter(_SgaWriter[SgaFsSpec]):
    def __init__(
        self,
        sga:SgaFsSpec,
        logger: logging.Logger | None = None,
    ):
        super().__init__(sga, logger)
        sga._unlazy()

    @contextmanager
    def _writeback(self, ptr: int, stream: BinaryIO):
        previous = stream.tell()
        stream.seek(ptr)
        yield
        stream.seek(previous)

    def _add_fs_names(self, drive: _Directory, cur_folder: _Directory, is_root:bool=False):
        if is_root:
            self._add_name(drive.name,"") # root folder uses empty string
        folders = sorted(cur_folder.sub_folders.values(), key=lambda x: x.name)
        for folder in folders:
            self._add_name(drive.name, self._get_full_path(drive, folder))
        for folder in folders:
            self._add_fs_names(drive, folder, is_root=False)

    def _get_full_path(self, drive:_Directory, file_or_folder:_File|_Directory) -> str:
        full_path = file_or_folder.absolute_path
        alias = drive.name
        self._logger.info(f"full_path={full_path}, alias={alias}")
        parts = full_path.replace("\\","/").split("/")
        if parts[0] == alias:
            full_path = "/".join(parts[1:])
            self._logger.info(f"\tnew full_path={full_path}")
        return full_path


    def _add_fs_file(
        self, drive: _Directory, file: _File, game_format: SgaV2GameFormat
    ):
        name = file.name
        modified = file.modified
        storage_type = file.storage_type
        name_offset = self._add_name(drive.name, name)
        full_path = self._get_full_path(drive, file)

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
        folder_index: int | None = None,
    ):
        full_path = self._get_full_path(drive, folder)
        name_offset = self._add_name(drive.name, full_path)

        if writeback_ptr is None:  # write a faker!
            writeback_ptr = self._folder_stream.tell()
            folder_index = self._add_toc_folder(name_offset, 0, 0, 0, 0, folder_index, increment_count=True)

        folder_start = self._folder_count

        sub_folders: list[tuple[int, _Directory, int]] = []
        for sub_folder in folder.sub_folders.values():
            sub_folder_wb = self._folder_stream.tell()
            sub_folder_index = self._add_toc_folder(0, 0, 0, 0, 0, increment_count=True)  # blank folder
            _info = sub_folder_wb, sub_folder, sub_folder_index
            sub_folders.append(_info)

        folder_end = self._folder_count

        for sub_writeback, sub_folder, sub_index in sub_folders:
            self._add_fs_folder(drive, sub_folder, game_format, sub_writeback, sub_index)

        file_start = self._file_count
        for file in folder.files.values():
            self._add_fs_file(drive, file, game_format)
        file_end = self._file_count

        with self._writeback(writeback_ptr, self._folder_stream):
            self._add_toc_folder(
                name_offset, folder_start, folder_end, file_start, file_end,  folder_index, increment_count=False
            )

    def _add_fs_drive(self, drive: _Directory, game_format: SgaV2GameFormat):
        self._logger.debug(f"Adding FS Drive (alias={drive.name}, name={drive.drive_name})")
        # Write all names to init cache & to mimic modpackager's name layout
        self._add_fs_names(drive,drive,is_root=True)
        # Collect known arguments
        name = drive.drive_name
        alias = drive.name
        drive_folder_root = drive_folder_start = self._folder_count
        file_start = self._file_count
        folder_name_offset = 0  # self._get_name_offset(name,)

        # Create blank root folder
        root_folder_ptr = self._folder_stream.tell()
        wb_index = self._add_toc_folder(folder_name_offset, 0, 0, 0, 0, increment_count=True)

        folder_start = self._folder_count

        # Walk Tree
        self._add_fs_folder(
            drive, drive, game_format, root_folder_ptr, wb_index
        )  # will not have the correct END arguments due to nesting



        # Collect new arguments
        folder_end = self._folder_count
        file_end = self._file_count

        # # Fix blank root folder
        # with self._writeback(root_folder_ptr, self._folder_stream):
        #     self._add_toc_folder(
        #         folder_name_offset, folder_start, folder_end, file_start, file_end, wb_index, increment_count=False
        #     )

        # Finish by writing TOC Entry
        self._add_toc_drive(
            name, alias, drive_folder_start, folder_end, file_start, file_end, drive_folder_root
        )

    def _add_toc(self, game_format: SgaV2GameFormat):
        self._logger.debug("Determining DoW/IC")
        game_format = self._resolve_desired_format(game_format, self._sga._meta)

        self._logger.debug("Adding entries to TOC")
        for drive in self._sga._iter_drives():
            self._add_fs_drive(drive, game_format)

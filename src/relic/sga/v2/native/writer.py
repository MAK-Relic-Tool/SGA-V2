from __future__ import annotations

import datetime
import logging
import zlib
from io import BytesIO
from os import PathLike
from typing import BinaryIO, Generic

from relic.sga.core import StorageType, MAGIC_WORD
from relic.sga.core.hashtools import crc32, md5
from relic.sga.core.native.handler import VERSION_LAYOUT

from relic.sga.v2.native._util import SgaVerifierV2
from relic.sga.v2.native.models import (
    ArchiveMeta,
    TocPointers,
    TocPointer,
    FileMetadata,
)
from relic.sga.v2.native.parser import (
    ARCHIVE_HEADER,
    TOC_PTR,
    DRIVE_ENTRY,
    FOLDER_ENTRY,
    DOW_FILE_ENTRY,
    IC_FILE_ENTRY,
    FILE_METADATA,
)
from relic.sga.v2.serialization import (
    SgaV2GameFormat,
    RelicDateTimeSerializer,
    _T,
)


# Doesn't support writing from disk; .arciv/.sgaconfig should handle that

# Data structures to get from essencefs/fsspec
# @dataclass(slots=True)
# class WriterDriveEntry:
#     alias:str
#     name:str
#     subfolders:list[WriterFolderEntry]
#
# @dataclass(slots=True)
# class WriterFolderEntry:
#     full_name_from_drive:str
#     subfolders:list[WriterFolderEntry]
#     files:list[WriterFileEntry]
#
# @dataclass(slots=True)
# class WriterFileEntry:
#     file_name:str
#     storage_type:StorageType
#     data:bytes = None
#     modified:datetime.datetime = None
#
# # Data structures to generate as we write the file
# class WriteableDriveEntry:
#     alias:str
#     name:str
#     folders:list[WriteableFolderEntry]
#     files:list[WritableFileEntry]
#     root_folder:int = 0
#
# class WritableFolderEntry:
#     name_pos:int
#     subfolder_start:int
#     subfolder_end:int
#     file_start:int
#     file_end:int
#
# class WritableFileEntry:
#     name_pos:int
#     name:str # for metadata
#     data:bytes = None


class _SgaWriter(Generic[_T]):
    def __init__(
        self,
        sga:_T,
        logger: logging.Logger | None = None,
    ):
        self._sga = sga
        self._name_tables: dict[str, dict[str, int]] = {}
        self._name_stream = BytesIO()
        self._drives = 0
        self._drive_stream = BytesIO()
        self._folder_count = 0
        self._folder_stream = BytesIO()
        self._file_count = 0
        self._file_stream = BytesIO()
        # self._desired_format = game_format
        self._out = BytesIO() # for safety, we write to a separate handle
        self._data_stream = BytesIO()
        self._logger = logger or logging.getLogger(
            self.__class__.__qualname__
        )  # hack; use a null logger instead

    def close(self):
        self._out.close()
        self._name_stream.close()
        self._drive_stream.close()
        self._folder_stream.close()
        self._file_stream.close()
        self._data_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def write(self,
        path: BinaryIO | str | PathLike,
        name:str|None = None,
        game_format: SgaV2GameFormat = SgaV2GameFormat.Unknown,
    ):
        self._write(name, game_format)
        if isinstance(path,(str,PathLike)):
            with open(path, "wb") as file:
                file.write(self._out.getbuffer())
        else:
            path.write(self._out.getbuffer())

    def _add_name(self, drive: str, name: str):
        safe_name = name.rstrip("\0")
        self._logger.debug(f"Adding name '{safe_name}' to drive {drive}")
        if drive not in self._name_tables:
            name_table = self._name_tables[drive] = {}
        else:
            name_table = self._name_tables[drive]

        if safe_name in name_table:
            return name_table[safe_name]
        name_pos = name_table[safe_name] = self._name_stream.tell()
        name_buffer = safe_name.encode("utf8") + b"\0"  # todo; check that utf8 works
        self._name_stream.write(name_buffer)
        self._logger.debug(f"Added name '{safe_name}' to drive {drive} @{name_pos}")
        return name_pos

    def _write_magic_and_version(self) -> None:
        self._logger.debug("Writing Magic and Version")
        MAGIC_WORD.write(self._out)
        VERSION = (2, 0)
        self._out.write(VERSION_LAYOUT.pack(*VERSION))

    def _write_header(self, meta: ArchiveMeta, overwrite_name:str|None) -> None:
        self._logger.debug("Writing V2 Metadata")
        self._out.seek(12)
        name = overwrite_name if overwrite_name is not None else meta.name
        buffer = ARCHIVE_HEADER.pack(
            meta.file_md5, name.encode("utf-16le"), meta.toc_md5, meta.toc_size, meta.data_offset
        )
        self._out.write(buffer)

    def _write_toc_ptrs(self, ptrs: TocPointers):
        self._logger.debug("Writing TOC Header")
        self._out.seek(180)
        for ptr in [ptrs.drive, ptrs.folder, ptrs.file, ptrs.name]:
            buffer = TOC_PTR.pack(ptr.offset, ptr.count)
            self._out.write(buffer)

    @staticmethod
    def _resolve_desired_format(specified_format:SgaV2GameFormat, meta: ArchiveMeta):
        if specified_format == SgaV2GameFormat.Unknown:
            if meta.game_format != SgaV2GameFormat.Unknown:
                return meta.game_format
            raise NotImplementedError
        else:
            return specified_format

    def _create_ptrs(self) -> TocPointers:
        self._logger.debug("Calculating TOC Pointers")
        drives = TocPointer(
            offset=TOC_PTR.size * 4,
            count=self._drives,
            size=len(self._drive_stream.getvalue()),
        )
        folders = TocPointer(
            offset=drives.offset + drives.size,
            count=self._folder_count,
            size=len(self._folder_stream.getvalue()),
        )
        files = TocPointer(
            offset=folders.offset + folders.size,
            count=self._file_count,
            size=len(self._file_stream.getvalue()),
        )
        names = TocPointer(
            offset=files.offset + files.size,
            count=sum(len(t) for t in self._name_tables),
            size=len(self._name_stream.getvalue()),
        )
        return TocPointers(drives, folders, files, names)

    def _write_toc(self):
        self._logger.debug("Writing TOC Data")

        self._out.seek(180 + 4 * TOC_PTR.size)  # skip the pointers
        self._out.write(self._drive_stream.getvalue())
        self._out.write(self._folder_stream.getvalue())
        self._out.write(self._file_stream.getvalue())
        self._out.write(self._name_stream.getvalue())
        end = self._out.tell()
        return end - 180  # toc_size

    def _write_data(self, toc_size: int):
        self._logger.debug("Writing Blob Data")
        self._out.seek(180 + toc_size)
        self._out.write(self._data_stream.getvalue())

    def _add_metadata(self, meta: FileMetadata):
        self._logger.debug("Writing File's Metadata")
        safe_name = meta.name.encode("utf8")
        safe_modified = int(RelicDateTimeSerializer.datetime2unix(meta.modified))
        buffer = FILE_METADATA.pack(safe_name, meta.crc32, safe_modified)
        self._out.write(buffer)

    def _add_data(
        self,
        full_path: str,
        raw: bytes,
        storage: StorageType,
        modified: datetime.datetime,
    ) -> tuple[int, tuple[int, int]]:
        self._logger.debug("Writing File")
        crc_hash = crc32(raw)
        self._add_metadata(FileMetadata(full_path, modified, crc_hash))

        buffer = raw
        if storage in [StorageType.BUFFER_COMPRESS, StorageType.STREAM_COMPRESS]:
            buffer = zlib.compress(raw)

        now = self._data_stream.tell()
        self._data_stream.write(buffer)
        return now, (len(raw), len(buffer))

    def _add_toc_drive(
        self,
        name: str,
        alias: str,
        first_folder: int,
        last_folder: int,
        first_file: int,
        last_file: int,
        root_folder: int,
    ):
        if alias is None:
            self._logger.warning(f"drive '{name}' was missing an alias; defaulting to name")
            alias = name

        self._logger.debug("Writing Drive Entry")
        buffer = DRIVE_ENTRY.pack(
            alias.encode("utf-8"), name.encode("utf-8"), first_folder, last_folder, first_file, last_file, root_folder
        )
        self._drives += 1
        self._drive_stream.write(buffer)

    def _add_toc_file(
        self,
        name_offset: int,
        storage_type: StorageType,
        data_offset: int,
        compressed_size: int,
        decompressed_size: int,
        game_format: SgaV2GameFormat,
    ):
        self._logger.debug("Writing File Entry")
        ENTRY = {
            SgaV2GameFormat.DawnOfWar: DOW_FILE_ENTRY,
            SgaV2GameFormat.ImpossibleCreatures: IC_FILE_ENTRY,
        }[game_format]
        buffer = ENTRY.pack(
            name_offset,
            int(storage_type),
            data_offset,
            compressed_size,
            decompressed_size,
        )
        self._file_count += 1
        self._file_stream.write(buffer)

    def _add_toc_folder(
        self,
        name_offset: int,
        first_folder: int,
        last_folder: int,
        first_file: int,
        last_file: int,
        folder_index:int|None=None,
        increment_count:bool=False,
    ):
        if folder_index is None: # assume its a faker
            self._logger.debug(
                f"Writing Folder[{self._folder_count}] Entry: Placeholder")
            folder_index = self._folder_count
        else:
            self._logger.debug(f"Writing Folder[{folder_index}] Entry: (name={name_offset}, first_folder={first_folder}, last_folder={last_folder}, first_file={first_file}, last_file={last_file})")
        if increment_count:
            self._folder_count += 1
        buffer = FOLDER_ENTRY.pack(
            name_offset, first_folder, last_folder, first_file, last_file
        )
        self._folder_stream.write(buffer)
        return folder_index

    def _calculate_header(self, name: str, toc_size: int):
        self._logger.debug("Calculating Header")
        TOC_START = 180
        toc_hash = md5.hash(
            self._out,
            start=TOC_START,
            size=toc_size,
            eigen=SgaVerifierV2._TOC_MD5_EIGEN,
        )
        file_hash = md5.hash(
            self._out, start=TOC_START, eigen=SgaVerifierV2._FILE_MD5_EIGEN
        )
        return ArchiveMeta(file_hash, name, toc_hash, toc_size, TOC_START + toc_size)

    def _add_toc(self, game_format:SgaV2GameFormat):
        self._logger.debug("Adding entries to TOC")
        raise NotImplementedError

    def _write(self, name:str|None, game_format:SgaV2GameFormat):
        # Initial pass/first stage; writes blanks
        self._logger.debug("Writing SGA - First Stage [Defaults / Blanks]")
        self._write_magic_and_version()
        self._write_header(ArchiveMeta.default(), overwrite_name=name)
        self._write_toc_ptrs(TocPointers.default())

        self._logger.debug("Writing SGA - Second Stage [Table Of Contents]")
        # Parse the SGA model
        self._add_toc(game_format)
        # Write the parsed TOC / Blob
        toc_size = self._write_toc()
        self._write_data(toc_size)

        self._logger.debug("Writing SGA - Third Stage [Updating Headers]")
        # Second pass/third stage; out stream is now complete
        # Write the correct pointers
        toc_ptrs = self._create_ptrs()
        self._write_toc_ptrs(toc_ptrs)
        # Write the correct header
        header = self._calculate_header(name, toc_size)
        self._write_header(header,overwrite_name=name)
        self._logger.debug("Writing SGA - Done")

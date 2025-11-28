from __future__ import annotations

import datetime
import logging
import struct
from os import PathLike
from typing import Any, Dict, Callable, BinaryIO

from relic.core.errors import RelicToolError, MismatchError
from relic.core.logmsg import BraceMessage
from relic.sga.core import StorageType, Version
from relic.sga.core.native.definitions import ReadonlyMemMapFile, FileEntry
from relic.sga.core.native.handler import NativeParserHandler, SharedHeaderParser

from relic.sga.v2._util import _OmniHandle, _OmniHandleAccepts
from relic.sga.v2.native.models import (
    FileEntryV2,
    ArchiveMeta,
    TocPointer,
    TocPointers,
    FileMetadata,
)
from relic.sga.v2.serialization import SgaV2GameFormat

TOC_PTR = struct.Struct("<IH")
DRIVE_ENTRY = struct.Struct("<64s64s5H")
FOLDER_ENTRY = struct.Struct("<IHHHH")
DOW_FILE_ENTRY = struct.Struct("<5I")
IC_FILE_ENTRY = struct.Struct("<IB3I")
ARCHIVE_HEADER = struct.Struct("<16s128s16sII")
FILE_METADATA = struct.Struct("<256sII")


class NativeParserV2(NativeParserHandler[FileEntryV2]):
    """REAL native SGA reader - parses binary format directly.

    This completely bypasses fs library by:
    1. Manually parsing SGA V2 binary header
    2. Manually parsing TOC to get file entries
    3. Extracting TRUE byte offsets for each file
    4. Using mmap for zero-copy access
    5. Parallel zlib decompression

    Target: 3-4 seconds for 7,815 files!
    """

    _TOC_OFFSET = 180

    def __init__(
        self,
        sga_path: str|PathLike[str]|BinaryIO,
        logger: logging.Logger | None = None,
        read_metadata: bool = True,
        prefer_drive_alias:bool=False
    ):
        """Parse SGA file.

        Args:
            sga_path: Path to SGA archive
            logger:
        """
        super().__init__(sga_path) # dont create

        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG) # HACK; TODO remove
        self._files: list[dict[str, Any]] = []
        self._folders: list[dict[str, Any]] = []
        self._drives: list[dict[str, Any]] = []
        self._entries: list[FileEntryV2] = []
        self._data_block_start = 0
        # Parse the binary format
        self._parsed = False
        self._meta: ArchiveMeta = None  # type: ignore
        self.read_metadata = read_metadata
        self.prefer_drive_alias = prefer_drive_alias

    def _log(self, msg: str) -> None:  # TODO; use logger directly and use BraceMessages
        """Log if verbose."""
        if self.logger:
            self.logger.debug(f"[Parser] {msg}")

    def _parse_toc_pair(self, block_index: int, base_offset: int) -> TocPointer:
        """
        Parse a single TOC Pointer, getting the absolute offset and entry count.
        Size is not included, and must be calculated/cached separately
        """
        buffer = self._read(self._TOC_OFFSET + block_index * TOC_PTR.size, TOC_PTR.size)
        offset, count = TOC_PTR.unpack(buffer)
        return TocPointer(offset + base_offset, count, None)

    def _parse_toc_header(self) -> TocPointers:
        self.logger.debug("Parsing Table Of Contents Header")
        TOC_SIZE = self._meta.toc_size
        # We cheat and make parse_toc_pair use a relative offset to make logic for calculating size easier
        (drives, folders, files, names) = ptrs = [
            self._parse_toc_pair(_, 0) for _ in range(4)
        ]

        # ensure pointers are ordered IN REVERSE
        # official packer always has the TOC's data block in drive/folder/files/names order
        # but other fan packers might not do that
        # So we sort the TOC Pointers, we sort for safety
        current_terminal = TOC_SIZE
        for ptr in sorted(ptrs, key=lambda _: _.offset, reverse=True):
            # Calculate size
            ptr.size = current_terminal - ptr.offset
            current_terminal = ptr.offset

            # Also fix pointer to use absolute offset
            ptr.offset += self._TOC_OFFSET

        def _log(name:str,_ptr:TocPointer):
            self.logger.debug(BraceMessage("{name}: (offset={offset}, count={count}, size={size}B)",
                              name=name,
                                           offset=_ptr.offset,
                                           size=_ptr.size,
                                           count=_ptr.count))

        _log("Drives",drives)
        _log("Folders",folders)
        _log("Files",files)
        _log("Names",names)

        # objects updated in place; no
        return TocPointers(drives, folders, files, names)

    def _parse_names(
        self,
        ptr: TocPointer,
    ) -> Dict[int, str]:

        # Parse string table FIRST (names are stored here!)
        self.logger.debug("Parsing string table...")
        # well formatted TOC; we can determine the size of the name table using the TOC size (name size is always last)
        string_table_data = self._read(ptr.offset, ptr.size)
        names = {}
        running_index = 0
        for i, name in enumerate(string_table_data.split(b"\0")):
            safe_name = names[running_index] = name.decode("utf-8")
            self.logger.debug(BraceMessage("Name[{i}] @{running_index} = {name}", i=i, running_index=running_index,name=safe_name))
            running_index += len(name) + 1
        return names

    def _parse_drives(self, ptr: TocPointer) -> list[dict[str, Any]]:
        # Parse drives (138 bytes each)
        self.logger.debug("Parsing drives...")
        drives = []

        for drive_index in range(ptr.count):
            offset = ptr.offset + drive_index * DRIVE_ENTRY.size
            buffer = self._read(offset, DRIVE_ENTRY.size)
            (
                alias,
                name,
                first_folder,
                last_folder,
                first_file,
                last_file,
                root_folder,
            ) = DRIVE_ENTRY.unpack(buffer)
            # Drive structure: alias(64), name(64), first_folder(2), last_folder(2),
            #                  first_file(2), last_file(2), root_folder(2)
            alias = alias.rstrip(b"\x00").decode("utf-8", errors="ignore")
            name = name.rstrip(b"\x00").decode("utf-8", errors="ignore")

            drives.append(
                {
                    "alias": alias,
                    "name": name,
                    "root_folder": root_folder,
                    "first_folder": first_folder,
                    "last_folder": last_folder,
                    "first_file": first_file,
                    "last_file": last_file,
                }
            )
            self.logger.debug(f"Drive: {name} (root_folder= {root_folder}, folders=[{first_folder}, {last_folder}], files=[{first_file}, files={last_file}])")
        return drives

    def _parse_folders(
        self,
        ptr: TocPointer,
        string_table: dict[int, str],
    ) -> list[dict[str, Any]]:
        # Parse folders (12 bytes each)
        self.logger.debug("Parsing folders...")
        folders = []
        base_offset = ptr.offset
        for folder_index in range(ptr.count):
            offset = base_offset + folder_index * FOLDER_ENTRY.size
            self.logger.debug(f"Reading folder[{folder_index}] @{offset}")
            buffer = self._read(
                offset, FOLDER_ENTRY.size
            )
            # Folder: name_offset(4), subfolder_start(2), subfolder_stop(2), first_file(2), last_file(2)
            name_off, subfolder_start, subfolder_stop, first_file, last_file = (
                FOLDER_ENTRY.unpack(buffer)
            )
            self.logger.debug(f"Folder[{folder_index}]: (name_offset={name_off}, subfolder_start={subfolder_start}, subfolder_stop={subfolder_stop}, first_file={first_file}, last_file={last_file})")
            if name_off not in string_table:
                self.logger.error(
                    BraceMessage("Cannot find name in name table @{name_off}",name_off=name_off)
                )
                DELTA = 256 * 2 # meta max size is 256, assume we must be in that range
                _VIEW = {n:v for n,v in string_table.items() if name_off - DELTA <= n <= name_off + DELTA}
                self.logger.error(string_table if len(_VIEW) == 0 else _VIEW)

            folder_name = string_table[name_off]
            folders.append(
                {
                    "name": folder_name,
                    "subfolder_start": subfolder_start,
                    "subfolder_stop": subfolder_stop,
                    "first_file": first_file,
                    "last_file": last_file,
                }
            )
        return folders

    def _parse_files(
        self,
        ptr: TocPointer,
        string_table: dict[int, str],
        game: SgaV2GameFormat | None = None,
    ) -> list[dict[str, Any]]:
        # File: name_offset(4), flags(dow=4,ic=1), data_offset(4), compressed_size(4), decompressed_size(4)

        def _parse_dow_storge_type(v: int) -> StorageType:
            # Storage type is in upper nibble of flags
            # 1/2 is buffer/stream compression;
            # supposedly they mean different things to the engine; to us, they are the same
            return StorageType((v & 0xF0) >> 4)

        def _parse_ic_storge_type(v: int) -> StorageType:
            return StorageType(v)

        def _determine_game_format() -> SgaV2GameFormat:
            expected_dow_size = DOW_FILE_ENTRY.size * ptr.count
            expected_ic_size = IC_FILE_ENTRY.size * ptr.count

            if ptr.count == 0:
                return SgaV2GameFormat.DawnOfWar  # Doesn't matter, no entries

            options = {
                expected_ic_size: SgaV2GameFormat.ImpossibleCreatures,
                expected_dow_size: SgaV2GameFormat.DawnOfWar,
            }

            if ptr.size not in options:
                raise RelicToolError(
                    f"Game format could not be determined; expected one of '{list(options.keys())}', received `{ptr.size}."
                )

            return options[ptr.size]

        self.logger.debug(f"Parsing {ptr.count} files...")

        if game is None:
            game = _determine_game_format()
        self._meta.game_format = game
        self.logger.debug(f"Using game format: {game}")
        s: struct.Struct = {
            SgaV2GameFormat.DawnOfWar: DOW_FILE_ENTRY,
            SgaV2GameFormat.ImpossibleCreatures: IC_FILE_ENTRY,
        }[game]
        parse_storage_type: Callable[[int], StorageType] = {
            SgaV2GameFormat.DawnOfWar: _parse_dow_storge_type,
            SgaV2GameFormat.ImpossibleCreatures: _parse_ic_storge_type,
        }[game]

        files = []
        base_offset = ptr.offset
        for file_index in range(ptr.count):
            offset = base_offset + file_index * s.size
            self.logger.debug(f"Reading file[{file_index}] @{offset}")
            buffer = self._read(offset, s.size)
            name_off, flags, data_offset, compressed_size, decompressed_size = s.unpack(
                buffer
            )
            file_name = string_table[name_off]
            storage_type = parse_storage_type(flags)

            files.append(
                {
                    "name": file_name,
                    "data_offset": data_offset,
                    "compressed_size": compressed_size,
                    "decompressed_size": decompressed_size,
                    "storage_type": storage_type,
                }
            )

            if file_index < 5:  # Debug first 5
                self._log(
                    f" File[{file_index}]: {file_name}, offset={data_offset},"
                    f" comp={compressed_size}, decomp={decompressed_size}, type={storage_type}"
                )
        return files

    def _parse_magic_and_version(self) -> None:
        # hack
        self.logger.debug("Parsing Magic")
        NativeParserV2.read_magic = SharedHeaderParser.read_magic
        SharedHeaderParser.validate_magic(self)
        del NativeParserV2.read_magic

        self.logger.debug("Parsing Version")
        NativeParserV2.read_version = SharedHeaderParser.read_version
        SharedHeaderParser.validate_version(self, Version(2, 0))
        del NativeParserV2.read_version

    def _parse_header(self) -> ArchiveMeta:
        # Read header (SGA V2 header is 180 bytes total, TOC starts at 180)
        # The actual offsets are 12 bytes later than documented:
        # toc_size at offset 172, data_pos at offset 176
        buffer = self._read_range(12, 180)
        self.logger.debug("Parsing Header")
        file_hash, _archive_name, toc_hash, toc_size, data_offset = (
            ARCHIVE_HEADER.unpack(buffer)
        )
        archive_name: str = _archive_name.rstrip(b"\0").decode(
            "utf-16", errors="ignore"
        )

        self.logger.debug(BraceMessage("Header(file_hash={file_hash}, archive_name={archive_name}, toc_hash={toc_hash}, toc_size={toc_size}, data_offset={data_offset})",
                                       file_hash=file_hash,
                                       archive_name=archive_name,
                                       toc_hash=toc_hash,
                                       toc_size=toc_size,
                                       data_offset=data_offset))
        self._data_block_start = data_offset
        self._meta = ArchiveMeta(
            file_hash,
            archive_name,
            toc_hash,
            toc_size,
            data_offset,
            SgaV2GameFormat.Unknown,
        )
        return self._meta

    def _parse_sga_binary(self) -> None:  # TODO; move to v2
        """Parse SGA V2 binary format manually."""
        self._log(f"Opening {self._handle}")
        self._parse_magic_and_version()
        self._parse_header()

        # Parse TOC Header
        # Format: drive_pos(4), drive_count(2), folder_pos(4), folder_count(2),
        #         file_pos(4), file_count(2), name_pos(4), name_count(2)
        toc_ptrs = self._parse_toc_header()

        self._log(
            f"TOC: {toc_ptrs.drive.count} drives, {toc_ptrs.folder.count} folders,"
            f" {toc_ptrs.file.count} files, {toc_ptrs.name.count} strings"
        )

        string_table = self._parse_names(toc_ptrs.name)
        self._drives = self._parse_drives(toc_ptrs.drive)
        self._folders = self._parse_folders(toc_ptrs.folder, string_table)
        self._files = self._parse_files(toc_ptrs.file, string_table)

        # Build file map
        self._log("Building file map...")
        self._log(f"Data block starts at offset: {self._data_block_start}")
        for drive in self._drives:

            # an interesting problem; name is correct with .sgaconfig
            #   but alias makes merging directories easy
            #   we also *usually* don't want the name when creating an abstract fs but the alias
            #       E.G. a game engine doesn't care that the drive is 'w40k-whm-assets' but that it's 'data'
            drive_name = drive["alias"] if self.prefer_drive_alias else drive["name"]
            self._build_file_paths(drive["root_folder"], drive_name, "")

        if self.read_metadata:
            self._log("Parsing metadata...")
            self._parse_metadata()

        self._log(f"Successfully parsed {len(self._entries)} files!")

    def _parse_metadata(
        self,
    ) -> None:
        for entry in self._entries:
            entry.metadata = _read_metadata(self, entry)
            entry.modified = entry.metadata.modified

    def _build_file_paths(
        self,
        folder_idx: int,
        drive_name: str,
        current_path: str,
    ) -> None:
        """Recursively build full file paths."""
        if folder_idx >= len(self._folders):
            return
        folder = self._folders[folder_idx]
        folder_name = folder["name"]
        self.logger.debug(f"Building Folder[{folder_idx}] (files=[{folder['first_file']}, {folder['last_file']}], folders=[{folder['subfolder_start']}, {folder['subfolder_stop']}])")

        # Normalize folder name (remove backslashes)
        folder_name = folder_name.replace("\\", "/")

        # Build folder path - folder names are often full paths from root, not relative
        # So we just use the folder_name directly
        full_folder_path = folder_name if folder_name else current_path

        # Add files in this folder

        for file_idx in range(folder["first_file"], folder["last_file"]):
            if file_idx < len(self._files):
                file = self._files[file_idx]

                # Build full path

                # Create entry - data_offset is RELATIVE to data block!
                # Absolute offset = data_block_start + data_offset
                entry = FileEntryV2(
                    drive=drive_name,
                    folder_path=full_folder_path,
                    name=file["name"],
                    data_offset=self._data_block_start
                    + file["data_offset"],  # Make it absolute!
                    compressed_size=file["compressed_size"],
                    decompressed_size=file["decompressed_size"],
                    storage_type=file["storage_type"],
                )
                self._entries.append(entry)

        # Recurse into subfolders
        for subfolder_idx in range(folder["subfolder_start"], folder["subfolder_stop"]):
            self._build_file_paths(subfolder_idx, drive_name, full_folder_path)
        self._parsed = True

    def parse(self) -> list[FileEntryV2]:
        if not self._parsed:
            self._parse_sga_binary()

        return self.get_file_entries()

    def get_file_entries(self) -> list[FileEntryV2]:
        return list(self._entries)

    def get_drive_count(self) -> int:
        return len(self._drives)

    def get_metadata(self) -> ArchiveMeta:
        return self._meta


def _read_metadata(reader: ReadonlyMemMapFile, entry: FileEntry) -> FileMetadata:
    buffer = reader._read_range(
        entry.data_offset - FILE_METADATA.size, entry.data_offset
    )
    _name, unix_timestamp, crc32_hash = FILE_METADATA.unpack(buffer)
    name = _name.rstrip(b"\0").decode("utf-8", errors="ignore")
    modified = datetime.datetime.fromtimestamp(unix_timestamp, datetime.timezone.utc)
    return FileMetadata(name, modified, crc32_hash)

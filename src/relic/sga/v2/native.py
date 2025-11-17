from __future__ import annotations

import datetime
import logging
import struct
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import BytesIO
from os.path import dirname
from typing import (
    Dict,
    Any,
    List,
    TypeVar,
    Generic,
    Iterable,
    Type,
    Sequence,
    Generator,
    Callable,
    Tuple,
)

from relic.core.entrytools import EntrypointRegistry
from relic.core.errors import RelicToolError
from relic.sga.core.definitions import StorageType, Version, MAGIC_WORD
from relic.sga.core.errors import VersionNotSupportedError, VersionMismatchError
from relic.sga.core.hashtools import crc32, md5
from relic.sga.core.native.definitions import (
    FileEntry,
    ReadonlyMemMapFile,
    ReadResult,
    Result,
)
from relic.sga.core.native.handler import (
    SharedHeaderParser,
    NativeParserHandler,
    SgaReader,
)

from relic.sga.v2.serialization import SgaTocFileV2ImpCreatures, SgaV2GameFormat


@dataclass(slots=True)
class FileEntryV2(FileEntry):
    # __slots__ = [*FileEntry.__slots__, "metadata"] # mypy hack; related to manually define slots
    metadata: FileMetadata | None = None


@dataclass(slots=True)
class TocPointer:
    offset: int
    count: int  # number of entries
    size: int | None  # size of partial toc block in bytes


@dataclass(slots=True)
class TocPointers:
    drive: TocPointer
    folder: TocPointer
    file: TocPointer
    name: TocPointer


@dataclass(slots=True)
class ArchiveMeta:
    file_md5: bytes
    name: str
    toc_md5: bytes
    toc_size: int


@dataclass(slots=True)
class FileMetadata:
    name: str
    modified: datetime.datetime
    crc32: int


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
        sga_path: str,
        logger: logging.Logger | None = None,
        read_metadata: bool = False,
    ):
        """Parse SGA file.

        Args:
            sga_path: Path to SGA archive
            logger:
        """
        super().__init__(sga_path)
        self.logger = logger
        self._files: list[dict[str, Any]] = []
        self._folders: list[dict[str, Any]] = []
        self._drives: list[dict[str, Any]] = []
        self._entries: list[FileEntryV2] = []
        self._data_block_start = 0
        # Parse the binary format
        self._parsed = False
        self._meta: ArchiveMeta = None  # type: ignore
        self.read_metadata = read_metadata

    def _log(self, msg: str) -> None:  # TODO; use logger directly and use BraceMessages
        """Log if verbose."""
        if self.logger:
            self.logger.debug(f"[Parser] {msg}")

    def _parse_toc_pair(self, block_index: int, base_offset: int) -> TocPointer:
        """
        Parse a single TOC Pointer, getting the absolute offset and entry count.
        Size is not included, and must be calculated/cached separately
        """
        s = struct.Struct("<IH")
        buffer = self._read(self._TOC_OFFSET + block_index * s.size, s.size)
        offset, count = s.unpack(buffer)
        return TocPointer(offset + base_offset, count, None)

    def _parse_toc_header(self) -> TocPointers:
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
            ptr.offset += self._TOC_OFFSET + TOC_SIZE

        # objects updated in place; no
        return TocPointers(drives, folders, files, names)

    def _parse_names(
        self,
        ptr: TocPointer,
    ) -> Dict[int, str]:

        # Parse string table FIRST (names are stored here!)
        self._log("Parsing string table...")
        self.logger.warning(f"Names: {ptr.offset}:{ptr.offset+ptr.size}")
        # well formatted TOC; we can determine the size of the name table using the TOC size (name size is always last)
        string_table_data = self._read(ptr.offset, ptr.size)
        names = {}
        running_index = 0
        for name in string_table_data.split(b"\0"):
            names[running_index] = name.decode("utf-8")
            running_index += len(name) + 1
        return names

    def _parse_drives(self, ptr: TocPointer) -> list[dict[str, Any]]:
        # Parse drives (138 bytes each)
        self._log("Parsing drives...")
        drives = []
        s = struct.Struct("<64s64s5H")

        for drive_index in range(ptr.count):
            offset = ptr.offset + drive_index * s.size
            buffer = self._read(offset, s.size)
            (
                alias,
                name,
                first_folder,
                last_folder,
                first_file,
                last_file,
                root_folder,
            ) = s.unpack(buffer)
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
            self._log(f"  Drive: {name} (root folder: {root_folder})")
        return drives

    def _parse_folders(
        self,
        ptr: TocPointer,
        string_table: dict[int, str],
    ) -> list[dict[str, Any]]:
        s = struct.Struct("<IHHHH")
        # Parse folders (12 bytes each)
        self._log("Parsing folders...")
        folders = []
        base_offset = ptr.offset
        for folder_index in range(ptr.count):
            buffer = self._read(base_offset + folder_index * s.size, s.size)
            # Folder: name_offset(4), subfolder_start(2), subfolder_stop(2), first_file(2), last_file(2)
            name_off, subfolder_start, subfolder_stop, first_file, last_file = s.unpack(
                buffer
            )

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
        dow_struct = struct.Struct("<5I")
        ic_struct = struct.Struct("<IB3I")

        def _parse_dow_storge_type(v: int) -> StorageType:
            # Storage type is in upper nibble of flags
            # 1/2 is buffer/stream compression;
            # supposedly they mean different things to the engine; to us, they are the same
            return StorageType((v & 0xF0) >> 4)

        def _parse_ic_storge_type(v: int) -> StorageType:
            return StorageType(v)

        def _determine_game_format() -> SgaV2GameFormat:
            expected_dow_size = dow_struct.size * ptr.count
            expected_ic_size = ic_struct.size * ptr.count

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

        self._log(f"Parsing {ptr.count} files...")

        if game is None:
            game = _determine_game_format()

        s: struct.Struct = {
            SgaV2GameFormat.DawnOfWar: dow_struct,
            SgaV2GameFormat.ImpossibleCreatures: ic_struct,
        }[game]
        parse_storage_type: Callable[[int], StorageType] = {
            SgaV2GameFormat.DawnOfWar: _parse_dow_storge_type,
            SgaV2GameFormat.ImpossibleCreatures: _parse_ic_storge_type,
        }[game]

        files = []
        base_offset = ptr.offset
        for file_index in range(ptr.count):
            buffer = self._read(base_offset + file_index * s.size, s.size)
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
                    f"  File[{file_index}]: {file_name}, offset={data_offset},"
                    f" comp={compressed_size}, decomp={decompressed_size}, type={storage_type}"
                )
        return files

    def _parse_magic_and_version(self) -> None:
        with SharedHeaderParser(self._file_path) as subparser:
            subparser.validate_version(Version(2, 0))

    def _parse_header(self) -> ArchiveMeta:
        # Read header (SGA V2 header is 180 bytes total, TOC starts at 180)
        # The actual offsets are 12 bytes later than documented:
        # toc_size at offset 172, data_pos at offset 176
        header_struct = struct.Struct("<16s128s16sII")
        buffer = self._read_range(12, 180)
        file_hash, _archive_name, toc_hash, toc_size, data_offset = (
            header_struct.unpack(buffer)
        )
        archive_name: str = _archive_name.rstrip(b"\0").decode(
            "utf-16", errors="ignore"
        )

        self._log(f"TOC size: {toc_size} bytes")
        self._log(f"Data starts at offset: {data_offset}")

        self._data_block_start = data_offset
        self._meta = ArchiveMeta(file_hash, archive_name, toc_hash, toc_size)
        return self._meta

    def _parse_sga_binary(self) -> None:  # TODO; move to v2
        """Parse SGA V2 binary format manually."""
        self._log(f"Opening {self._file_path}...")
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
            drive_name = drive["name"]
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
            with self:  # ensure we are open
                self._parse_sga_binary()

        return self.get_file_entries()

    def get_file_entries(self) -> list[FileEntryV2]:
        return list(self._entries)

    def get_drive_count(self) -> int:
        return len(self._drives)

    def get_metadata(self) -> ArchiveMeta:
        return self._meta


METADATA_STRUCT = struct.Struct("<256sII")


def _read_metadata(reader: ReadonlyMemMapFile, entry: FileEntry) -> FileMetadata:
    buffer = reader._read_range(
        entry.data_offset - METADATA_STRUCT.size, entry.data_offset
    )
    _name, unix_timestamp, crc32_hash = METADATA_STRUCT.unpack(buffer)
    name = _name.rstrip(b"\0").decode("utf-8", errors="ignore")
    modified = datetime.datetime.fromtimestamp(unix_timestamp, datetime.timezone.utc)
    return FileMetadata(name, modified, crc32_hash)


_T = TypeVar("_T")


def walk_entries_as_tree(
    entries: Sequence[_T],
    include_drive: bool = True,
    key_func: Callable[[_T], FileEntry] | None = None,
) -> Generator[Tuple[str, Sequence[_T]], None, None]:
    # We can ALMOST cheat by sorting; we just have to sort by directories, not files
    folders = {}

    def _key_func(item: _T) -> FileEntry:
        if isinstance(item, FileEntry):
            return item
        raise NotImplementedError

    if key_func is None:
        key_func = _key_func

    for item in entries:
        entry = key_func(item)
        folder = dirname(entry.full_path(include_drive=include_drive))
        if folder not in folders:
            folders[folder] = []
        folders[folder].append(item)

    for folder, folder_entries in sorted(folders.items(), key=lambda _: _[0]):
        yield folder, sorted(folder_entries, key=lambda _: key_func(_).name)


class SgaVerifierV2(ReadonlyMemMapFile):
    _FILE_MD5_EIGEN = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
    _TOC_MD5_EIGEN = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"

    def read_metadata(self, entry: FileEntry) -> FileMetadata:
        return _read_metadata(self, entry)

    def read_metadata_parallel(
        self, file_paths: Sequence[FileEntry], num_workers: int
    ) -> List[Result[FileEntry, FileMetadata]]:
        """Read and decompress files in PARALLEL."""

        def read(entry: FileEntry) -> Result[FileEntry, FileMetadata]:
            try:
                data = self.read_metadata(entry)
                return Result(entry, data)
            except Exception as e:
                return Result.create_error(entry, e)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(read, file_paths))

        return results

    def calc_crc32(self, entry: FileEntry) -> int:
        file_data = SgaReader.read_file(self, entry, decompress=True)
        # file_data = self._read(entry.data_offset, entry.compressed_size)
        return crc32.hash(file_data)

    def verify_file(self, entry: FileEntry, metadata: FileMetadata) -> bool:
        file_data = SgaReader.read_file(self, entry, decompress=True)
        # file_data = self._read(entry.data_offset, entry.compressed_size)
        return crc32.check(file_data, metadata.crc32)

    def verify_file_parallel(
        self, entries: List[FileEntryV2], num_workers: int
    ) -> List[Result[FileEntryV2, bool]]:
        def read(entry: FileEntryV2) -> Result[FileEntryV2, bool]:
            try:
                verified = self.verify_file(entry, entry.metadata)
                return Result(entry, verified)
            except Exception as e:
                return Result.create_error(entry, e)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(read, entries))

        return results

    @staticmethod
    def update_modified(entry: FileEntry, metadata: FileMetadata) -> FileEntry:
        entry.modified = metadata.modified
        return entry

    def update_modified_parallel(
        self, entries: List[FileEntry], metas: List[FileMetadata], num_workers: int
    ) -> List[Result[FileEntry, FileEntry]]:
        def update(
            entry: FileEntry, meta: FileMetadata
        ) -> Result[FileEntry, FileEntry]:
            try:
                updated = self.update_modified(entry, meta)
                return Result(entry, updated)
            except Exception as e:
                return Result.create_error(entry, e)

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(update, zip(entries, metas)))

        return results

    def verify_toc(self, meta: ArchiveMeta) -> bool:
        buffer = self._read(180, meta.toc_size)
        return md5.check(buffer, meta.toc_md5, eigen=self._TOC_MD5_EIGEN)

    def verify_archive(self, meta: ArchiveMeta) -> bool:
        terminal = len(self._mmap_handle)
        buffer = self._read_range(180, terminal)
        return md5.check(buffer, meta.file_md5, eigen=self._FILE_MD5_EIGEN)

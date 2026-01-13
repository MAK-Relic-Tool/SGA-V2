from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from os.path import dirname
from typing import (
    List,
    TypeVar,
    Sequence,
    Generator,
    Callable,
    Tuple,
)

from relic.sga.core.hashtools import crc32, md5
from relic.sga.core.native.definitions import (
    FileEntry,
    ReadonlyMemMapFile,
    Result,
)
from relic.sga.core.native.handler import (
    SgaReader,
)

from relic.sga.v2.native.models import FileEntryV2, ArchiveMeta, FileMetadata
from relic.sga.v2.native.parser import _read_metadata

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

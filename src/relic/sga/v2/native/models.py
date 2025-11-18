from __future__ import annotations

import datetime
from dataclasses import dataclass

from relic.sga.core.native.definitions import FileEntry

from relic.sga.v2.serialization import SgaV2GameFormat


@dataclass(slots=True)
class FileEntryV2(FileEntry):
    # __slots__ = [*FileEntry.__slots__, "metadata"] # mypy hack; related to manually define slots
    metadata: FileMetadata | None = None


@dataclass(slots=True)
class TocPointer:
    offset: int
    count: int  # number of entries
    size: int | None  # size of partial toc block in bytes

    @classmethod
    def default(cls) -> TocPointer:
        return cls(0, 0, None)


@dataclass(slots=True)
class TocPointers:
    drive: TocPointer
    folder: TocPointer
    file: TocPointer
    name: TocPointer

    @classmethod
    def default(cls):
        return TocPointers(*[TocPointer.default() for _ in range(4)])


@dataclass(slots=True)
class ArchiveMeta:
    file_md5: bytes
    name: str
    toc_md5: bytes
    toc_size: int
    data_offset: int
    game_format: SgaV2GameFormat = SgaV2GameFormat.Unknown

    @classmethod
    def default(cls):
        return ArchiveMeta(
            b"\0" * 16, "", b"\0" * 16, 0, 0, game_format=SgaV2GameFormat.Unknown
        )


@dataclass(slots=True)
class FileMetadata:
    name: str
    modified: datetime.datetime
    crc32: int

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
    game_format:SgaV2GameFormat


@dataclass(slots=True)
class FileMetadata:
    name: str
    modified: datetime.datetime
    crc32: int

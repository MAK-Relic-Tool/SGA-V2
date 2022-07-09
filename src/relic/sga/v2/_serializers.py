"""
Binary Serializers for Relic's SGA-V2
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Dict, ClassVar

from serialization_tools.structx import Struct

from relic.sga.core import _abc, _serializers as _s
from relic.sga.core._abc import FileDefABC as FileDef, ArchivePtrs, DriveDef, FolderDef
from relic.sga.core._core import MagicWord, Version, StorageType
from relic.sga.core._serializers import read_toc, load_lazy_data, TocHeaderSerializer
from relic.sga.core.errors import VersionMismatchError
from relic.sga.core.protocols import StreamSerializer
from relic.sga.v2._core import Archive, version, ArchiveMetadata


class FileDefSerializer(StreamSerializer[FileDef]):
    """
    Serializes File information using the V2 format.
    """

    STORAGE2INT: Dict[StorageType, int] = {
        StorageType.STORE: 0,
        StorageType.BUFFER_COMPRESS: 16,
        StorageType.STREAM_COMPRESS: 32,
    }
    INT2STORAGE: Dict[int, StorageType] = {
        value: key for key, value in STORAGE2INT.items()
    }  # reverse the dictionary

    def __init__(self, layout: Struct):
        self.layout = layout

    def unpack(self, stream: BinaryIO) -> FileDef:
        storage_type_val: int
        (
            name_pos,
            storage_type_val,
            data_pos,
            length_on_disk,
            length_in_archive,
        ) = self.layout.unpack_stream(stream)
        storage_type: StorageType = self.INT2STORAGE[storage_type_val]
        return FileDef(
            name_pos, data_pos, length_on_disk, length_in_archive, storage_type
        )

    def pack(self, stream: BinaryIO, value: FileDef) -> int:
        storage_type = self.STORAGE2INT[value.storage_type]
        args = (
            value.name_pos,
            storage_type,
            value.data_pos,
            value.length_on_disk,
            value.length_in_archive,
        )
        packed: int = self.layout.pack_stream(stream, *args)
        return packed


@dataclass
class ArchiveHeader:
    """
    Container for header information used by V2
    """

    name: str
    ptrs: ArchivePtrs
    file_md5: bytes
    header_md5: bytes


@dataclass
class ArchiveHeaderSerializer(StreamSerializer[ArchiveHeader]):
    """
    Serializer to convert header information to it's dataclass; ArchiveHeader
    """

    layout: Struct

    ENCODING = "utf-16-le"

    def unpack(self, stream: BinaryIO) -> ArchiveHeader:
        (
            file_md5,
            encoded_name,
            header_md5,
            header_size,
            data_pos,
        ) = self.layout.unpack_stream(stream)
        header_pos = stream.tell()
        name = encoded_name.rstrip(b"").decode(self.ENCODING)
        ptrs = ArchivePtrs(header_pos, header_size, data_pos)
        return ArchiveHeader(name, ptrs, file_md5=file_md5, header_md5=header_md5)

    def pack(self, stream: BinaryIO, value: ArchiveHeader) -> int:
        encoded_name = value.name.encode(self.ENCODING)
        args = (
            value.file_md5,
            encoded_name,
            value.header_md5,
            value.ptrs.header_size,
            value.ptrs.data_pos,
        )
        written: int = self.layout.pack_stream(stream, *args)
        return written


@dataclass
class ArchiveSerializer(_abc.ArchiveSerializer[Archive]):
    """
    Serializer to read/write an SGA file to/from a stream
    """

    version: Version
    drive_serializer: StreamSerializer[DriveDef]
    folder_serializer: StreamSerializer[FolderDef]
    file_serializer: StreamSerializer[FileDef]
    toc_serializer: TocHeaderSerializer
    archive_header_serializer: ArchiveHeaderSerializer

    FILE_MD5_EIGEN: ClassVar = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
    HEADER_MD5_EIGEN: ClassVar = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"

    def read(
        self, stream: BinaryIO, lazy: bool = False, decompress: bool = True
    ) -> Archive:
        MagicWord.read_magic_word(stream)
        stream_version = Version.unpack(stream)
        if stream_version != self.version:
            raise VersionMismatchError(stream_version, self.version)
        archive_header = self.archive_header_serializer.unpack(stream)

        # Seek to header; but we skip that because we are already there
        toc_header = self.toc_serializer.unpack(stream)
        drives, files = read_toc(
            stream=stream,
            toc_header=toc_header,
            ptrs=archive_header.ptrs,
            drive_def=self.drive_serializer,
            file_def=self.file_serializer,
            folder_def=self.folder_serializer,
            decompress=decompress,
            build_file_meta=lambda _: None,  # V2 has no metadata
            name_toc_is_count=True,
        )

        if not lazy:
            load_lazy_data(files)

        file_md5_helper = _s.Md5ChecksumHelper(
            expected=archive_header.file_md5,
            stream=stream,
            start=archive_header.ptrs.header_pos,
            eigen=self.FILE_MD5_EIGEN,
        )
        header_md5_helper = _s.Md5ChecksumHelper(
            expected=archive_header.header_md5,
            stream=stream,
            start=archive_header.ptrs.header_pos,
            size=archive_header.ptrs.header_size,
            eigen=self.HEADER_MD5_EIGEN,
        )
        metadata = ArchiveMetadata(file_md5_helper, header_md5_helper)

        return Archive(archive_header.name, metadata, drives)

    def write(self, stream: BinaryIO, archive: Archive) -> int:
        raise NotImplementedError


folder_layout = Struct("<I 4H")
folder_serializer = _s.FolderDefSerializer(folder_layout)

drive_layout = Struct("<64s 64s 5H")
drive_serializer = _s.DriveDefSerializer(drive_layout)

file_layout = Struct("<5I")
file_serializer = FileDefSerializer(file_layout)

toc_layout = Struct("<IH IH IH IH")
toc_header_serializer = _s.TocHeaderSerializer(toc_layout)

archive_header_layout = Struct("<16s 128s 16s 2I")
archive_header_serializer = ArchiveHeaderSerializer(archive_header_layout)

archive_serializer = ArchiveSerializer(
    version=version,
    archive_header_serializer=archive_header_serializer,
    toc_serializer=toc_header_serializer,
    file_serializer=file_serializer,
    drive_serializer=drive_serializer,
    folder_serializer=folder_serializer,
)

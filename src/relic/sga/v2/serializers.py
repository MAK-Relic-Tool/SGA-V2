"""
Binary Serializers for Relic's SGA-V2
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Dict, Tuple

from relic.sga.core import serializers as _s
from relic.sga.core.abstract import FileDef, ArchivePtrs, TocBlock
from relic.sga.core.definitions import StorageType
from relic.sga.core.protocols import StreamSerializer
from relic.sga.core.serializers import TOCSerializationInfo
from serialization_tools.structx import Struct

from relic.sga.v2.definitions import version, ArchiveMetadata


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
            length_in_archive,
            length_on_disk,
        ) = self.layout.unpack_stream(stream)
        storage_type: StorageType = self.INT2STORAGE[storage_type_val]
        return FileDef(
            name_pos=name_pos,
            data_pos=data_pos,
            length_on_disk=length_on_disk,
            length_in_archive=length_in_archive,
            storage_type=storage_type,
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
class MetaBlock(_s.MetaBlock):
    """
    Container for header information used by V2
    """

    name: str
    ptrs: ArchivePtrs
    file_md5: bytes
    header_md5: bytes

    @classmethod
    def default(cls) -> MetaBlock:
        default_md5: bytes = b"default hash.   "
        return cls(
            "Default Meta Block", ArchivePtrs.default(), default_md5, default_md5
        )


@dataclass
class ArchiveHeaderSerializer(StreamSerializer[MetaBlock]):
    """
    Serializer to convert header information to it's dataclass; ArchiveHeader
    """

    layout: Struct

    ENCODING = "utf-16-le"

    def unpack(self, stream: BinaryIO) -> MetaBlock:
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
        return MetaBlock(name, ptrs, file_md5=file_md5, header_md5=header_md5)

    def pack(self, stream: BinaryIO, value: MetaBlock) -> int:
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


file_md5_eigen = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
header_md5_eigen = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"


def assemble_meta(stream: BinaryIO, header: MetaBlock, _: None) -> ArchiveMetadata:
    file_md5_helper = _s.Md5ChecksumHelper(
        expected=header.file_md5,
        stream=stream,
        start=header.ptrs.header_pos,
        eigen=file_md5_eigen,
    )
    header_md5_helper = _s.Md5ChecksumHelper(
        expected=header.header_md5,
        stream=stream,
        start=header.ptrs.header_pos,
        size=header.ptrs.header_size,
        eigen=header_md5_eigen,
    )
    metadata = ArchiveMetadata(file_md5_helper, header_md5_helper)
    return metadata


def disassemble_meta(
        stream: BinaryIO, header: ArchiveMetadata
) -> Tuple[MetaBlock, None]:
    meta = MetaBlock(
        None,  # type: ignore
        None,  # type: ignore
        header_md5=header.header_md5,
        file_md5=header.file_md5,
    )
    return meta, None


def recalculate_md5(stream: BinaryIO, meta: MetaBlock) -> None:
    file_md5_helper = _s.Md5ChecksumHelper(
        expected=None,
        stream=stream,
        start=meta.ptrs.header_pos,
        eigen=file_md5_eigen,
    )
    header_md5_helper = _s.Md5ChecksumHelper(
        expected=None,
        stream=stream,
        start=meta.ptrs.header_pos,
        size=meta.ptrs.header_size,
        eigen=header_md5_eigen,
    )
    meta.file_md5 = file_md5_helper.read()
    meta.header_md5 = header_md5_helper.read()


def meta2def(_: None) -> FileDef:
    return FileDef(None, None, None, None, None)  # type: ignore


class ArchiveSerializer(
    _s.ArchiveSerializer[ArchiveMetadata, None, FileDef, MetaBlock, None]
):
    """
    Serializer to read/write an SGA file to/from a stream
    """

    def __init__(
            self,
            toc_serializer: StreamSerializer[TocBlock],
            meta_serializer: StreamSerializer[MetaBlock],
            toc_serialization_info: TOCSerializationInfo,
    ):
        super().__init__(
            version=version,
            meta_serializer=meta_serializer,
            toc_serializer=toc_serializer,
            toc_meta_serializer=None,
            toc_serialization_info=toc_serialization_info,
            assemble_meta=assemble_meta,
            disassemble_meta=disassemble_meta,
            build_file_meta=lambda _: None,
            gen_empty_meta=MetaBlock.default,
            finalize_meta=recalculate_md5,
            meta2def=meta2def,
        )


class SGAFSSerializer(
    _s.SGAFSSerializer[ArchiveMetadata, None, FileDef, MetaBlock, None]
):
    """
    Serializer to read/write an SGA file to/from a stream from/to a SGA File System
    """

    def __init__(
            self,
            toc_serializer: StreamSerializer[TocBlock],
            meta_serializer: StreamSerializer[MetaBlock],
            toc_serialization_info: TOCSerializationInfo,
    ):
        super().__init__(
            version=version,
            meta_serializer=meta_serializer,
            toc_serializer=toc_serializer,
            toc_meta_serializer=None,
            toc_serialization_info=toc_serialization_info,
            assemble_meta=assemble_meta,
            disassemble_meta=disassemble_meta,
            build_file_meta=lambda _: None,
            gen_empty_meta=MetaBlock.default,
            finalize_meta=recalculate_md5,
            meta2def=meta2def,
        )


_folder_layout = Struct("<I 4H")
_folder_serializer = _s.FolderDefSerializer(_folder_layout)

_drive_layout = Struct("<64s 64s 5H")
_drive_serializer = _s.DriveDefSerializer(_drive_layout)

_file_layout = Struct("<5I")
_file_serializer = FileDefSerializer(_file_layout)

_toc_layout = Struct("<IH IH IH IH")
_toc_header_serializer = _s.TocHeaderSerializer(_toc_layout)

_meta_header_layout = Struct("<16s 128s 16s 2I")
_meta_header_serializer = ArchiveHeaderSerializer(_meta_header_layout)

archive_serializer = ArchiveSerializer(
    # version=version,
    meta_serializer=_meta_header_serializer,
    toc_serializer=_toc_header_serializer,
    toc_serialization_info=TOCSerializationInfo(
        file=_file_serializer,
        drive=_drive_serializer,
        folder=_folder_serializer,
        name_toc_is_count=True,
    ),
)

sgafs_serializer = SGAFSSerializer(
    meta_serializer=_meta_header_serializer,
    toc_serializer=_toc_header_serializer,
    toc_serialization_info=TOCSerializationInfo(
        file=_file_serializer,
        drive=_drive_serializer,
        folder=_folder_serializer,
        name_toc_is_count=True,
    ),
)

__all__ = [
    "FileDefSerializer",
    "MetaBlock",
    "ArchiveHeaderSerializer",
    "ArchiveSerializer",
    "archive_serializer",
    "sgafs_serializer"
]


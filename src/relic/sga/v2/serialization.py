"""
Binary Serializers for Relic's SGA-V2
"""
from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from typing import BinaryIO, Dict, Tuple, cast, Any

from fs.base import FS
from relic.core.errors import MismatchError
from relic.sga.core import serialization as _s
from relic.sga.core.definitions import StorageType
from relic.sga.core.filesystem import registry
from relic.sga.core.protocols import StreamSerializer
from relic.sga.core.serialization import (
    FileDef,
    ArchivePtrs,
    TocBlock,
    TOCSerializationInfo,
    FSDisassembler,
    FSAssembler,
    FileLazyInfo,
    ESSENCE_NAMESPACE,
    _get_or_write_name,
    _write_data,
)

from relic.sga.v2.definitions import version
from serialization_tools.structx import Struct


class FileDefSerializer(StreamSerializer[FileDef]):
    """
    Serializes File information using the V2 format.
    """

    STORAGE2INT: Dict[StorageType, int] = {
        StorageType.STORE: 0,
        StorageType.BUFFER_COMPRESS: 16,  # 0x10
        StorageType.STREAM_COMPRESS: 32,  # 0x20
    }
    INT2STORAGE: Dict[int, StorageType] = {
        value: key for key, value in STORAGE2INT.items()
    }  # reverse the dictionary

    def __init__(self, layout: Struct):
        self.layout = layout

    def unpack(self, stream: BinaryIO) -> FileDef:
        """Unpacks a File Definition from the stream."""
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
        """Packs a File Definition into the stream."""
        storage_type = self.STORAGE2INT[value.storage_type]
        args = (
            value.name_pos,
            storage_type,
            value.data_pos,
            value.length_in_archive,
            value.length_on_disk,
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
        """Returns a Default, 'garbage' instance which can be used as a placeholder for write-backs."""
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
        """Unpacks a MetaBlock from the stream."""
        (
            file_md5,
            encoded_name,
            header_md5,
            header_size,
            data_pos,
        ) = self.layout.unpack_stream(stream)
        header_pos = stream.tell()
        name = encoded_name.decode(self.ENCODING).rstrip("\0")
        ptrs = ArchivePtrs(header_pos, header_size, data_pos)
        return MetaBlock(name, ptrs, file_md5=file_md5, header_md5=header_md5)

    def pack(self, stream: BinaryIO, value: MetaBlock) -> int:
        """Packs a MetaBlock into the stream."""
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


FILE_MD5_EIGEN = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
HEADER_MD5_EIGEN = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"


def assemble_meta(_: BinaryIO, header: MetaBlock, __: None) -> Dict[str, object]:
    """Extracts information from the meta-block to a dictionary the FS can store."""
    return {"file_md5": header.file_md5.hex(), "header_md5": header.header_md5.hex()}


def disassemble_meta(
        _: BinaryIO, metadata: Dict[str, object]
) -> Tuple[MetaBlock, None]:
    """Converts the archive's metadata dictionary into a MetaBlock class the Serializer can use."""
    meta = MetaBlock(
        None,  # type: ignore
        None,  # type: ignore
        header_md5=bytes.fromhex(cast(str, metadata["header_md5"])),
        file_md5=bytes.fromhex(cast(str, metadata["file_md5"])),
    )
    return meta, None


def recalculate_md5(stream: BinaryIO, meta: MetaBlock) -> None:
    """
    Recalculates file and header
    """
    file_md5_helper = _s.Md5ChecksumHelper(
        expected=None,
        stream=stream,
        start=meta.ptrs.header_pos,
        eigen=FILE_MD5_EIGEN,
    )
    header_md5_helper = _s.Md5ChecksumHelper(
        expected=None,
        stream=stream,
        start=meta.ptrs.header_pos,
        size=meta.ptrs.header_size,
        eigen=HEADER_MD5_EIGEN,
    )
    meta.file_md5 = file_md5_helper.read()
    meta.header_md5 = header_md5_helper.read()


def meta2def(meta: Dict[str, object]) -> FileDef:
    """
    Converts metadata to a File Definitions

    V2.0 only stores 'storage_type', which should be overridden later in the pipeline.
    """
    return FileDef(None, None, None, None, meta["storage_type"])  # type: ignore


class _AssemblerV2(FSAssembler[FileDef]):
    def assemble_file(self, parent_dir: FS, file_def: FileDef) -> None:
        super().assemble_file(parent_dir, file_def)

        # Still hate this, but might as well reuse it
        _HEADER_SIZE = (
                256 + 8
        )  # 256 string buffer (likely 256 cause 'max path' on windows used to be 256), and 4 byte unk, and 4 byte checksum (crc32)
        lazy_data_header = FileLazyInfo(
            jump_to=self.ptrs.data_pos + file_def.data_pos - _HEADER_SIZE,
            packed_size=_HEADER_SIZE,
            unpacked_size=_HEADER_SIZE,
            stream=self.stream,
            decompress=False,  # header isn't zlib compressed
        )

        lazy_info_decomp = FileLazyInfo(
            jump_to=self.ptrs.data_pos + file_def.data_pos,
            packed_size=file_def.length_in_archive,
            unpacked_size=file_def.length_on_disk,
            stream=self.stream,
            decompress=file_def.storage_type
                       != StorageType.STORE,  # self.decompress_files,
        )

        def _generate_crc32() -> bytes:
            return zlib.crc32(lazy_info_decomp.read()).to_bytes(
                4, "little", signed=False
            )

        def _set_info(_name: str, _modified: int, _crc32: bytes) -> None:
            essence_info: Dict[str, Any] = dict(
                parent_dir.getinfo(_name, [ESSENCE_NAMESPACE]).raw[ESSENCE_NAMESPACE]
            )
            essence_info["name"] = _name
            essence_info["modified"] = _modified
            essence_info["crc32"] = _crc32
            info = {ESSENCE_NAMESPACE: essence_info}
            parent_dir.setinfo(_name, info)

        def _generate_metadata() -> None:
            name = self.names[file_def.name_pos]

            info = parent_dir.getinfo(name, ["details"])
            timestamp = info.get("details", "modified", time.time())
            modified = int(timestamp)  # .to_bytes(4, "little", signed=False)

            crc32 = _generate_crc32()
            _set_info(name, modified, crc32)

        if (
                lazy_data_header.jump_to < 0
                or lazy_data_header.jump_to >= lazy_info_decomp.jump_to
        ):
            # Ignore checksum / name ~ Archive does not have this metadata
            # Recalculate it
            _generate_metadata()
        else:
            try:
                data_header = lazy_data_header.read()
                if len(data_header) != _HEADER_SIZE:
                    _generate_metadata()
                else:
                    name = data_header[:256].rstrip(b"\0").decode("ascii")
                    expected_name = self.names[file_def.name_pos]
                    if name != expected_name:
                        _generate_metadata()  # assume invalid metadata block
                    else:
                        modified_buffer: bytes = data_header[256:260]
                        modified = int.from_bytes(
                            modified_buffer, "little", signed=False
                        )
                        crc32 = data_header[260:264]
                        crc32_generated = _generate_crc32()

                        if crc32 != crc32_generated:
                            raise MismatchError("CRC Checksum", crc32_generated, crc32)

                        _set_info(name, modified, crc32)
            except UnicodeDecodeError:
                _generate_metadata()


class _DisassassemblerV2(FSDisassembler[FileDef]):
    _HEADER_SIZE = (
            256 + 8
    )  # 256 string buffer (likely 256 cause 'max path' on windows used to be 256), and 8 byte checksum

    def disassemble_file(self, container_fs: FS, file_name: str) -> FileDef:
        with container_fs.open(file_name, "rb") as handle:
            data = handle.read()

        metadata = dict(container_fs.getinfo(file_name, ["essence"]).raw["essence"])

        file_def: FileDef = self.meta2def(metadata)
        _storage_type_value: int = metadata["storage_type"]  # type: ignore
        storage_type = StorageType(_storage_type_value)
        if storage_type == StorageType.STORE:
            store_data = data
        elif storage_type in [
            StorageType.BUFFER_COMPRESS,
            StorageType.STREAM_COMPRESS,
        ]:
            store_data = zlib.compress(
                data, level=9
            )  # TODO process in chunks for large files
        else:
            raise NotImplementedError

        file_def.storage_type = storage_type
        file_def.length_on_disk = len(data)
        file_def.length_in_archive = len(store_data)

        file_def.name_pos = _get_or_write_name(
            file_name, self.name_stream, self.flat_names
        )

        name_buffer = bytearray(b"\0" * 256)
        name_buffer[0: len(file_name)] = file_name.encode("ascii")
        _name_buffer_pos = _write_data(name_buffer, self.data_stream)
        uncompressed_crc = zlib.crc32(data)
        # compressed_crc = zlib.crc32(store_data)
        if "modified" in metadata and metadata["modified"] != int.from_bytes(
                b"UNK\0", "little", signed=False
        ):  # handle my unknown case ~ UNK\0 resolves to 1970, so I don't think we need to worry about that
            timestamp: int = metadata["modified"]  # type: ignore
            timestamp_buffer = timestamp.to_bytes(4, "little", signed=True)

            # if creation/modification are different, use the new timestamp
            # Cumbersome, but allows header MD5s to invalidate
            info = container_fs.getinfo(file_name, ["details"])
            modified = info.get("details", "modified", None)
            created = info.get("details", "created", None)
            if modified is not None and created is not None:
                if int(modified) - int(created) != 0:
                    timestamp_buffer = int(modified).to_bytes(4, "little", signed=False)

        else:
            info = container_fs.getinfo(file_name, ["details"])
            timestamp: float = info.get("details", "modified", time.time())  # type: ignore
            timestamp_buffer = int(timestamp).to_bytes(4, "little", signed=False)

        _unk_buffer_pos = _write_data(timestamp_buffer, self.data_stream)

        _crc_buffer_pos = _write_data(
            uncompressed_crc.to_bytes(4, "little", signed=False), self.data_stream
        )  # should always recalc the crc, regardless of the cached value in metadata
        file_def.data_pos = _write_data(store_data, self.data_stream)

        return file_def


class EssenceFSSerializer(_s.EssenceFSSerializer[FileDef, MetaBlock, None]):
    """
    Serializer to read/write an SGA file to/from a stream from/to a SGA File System
    """

    def __init__(
            self,
            toc_serializer: StreamSerializer[TocBlock],
            meta_serializer: StreamSerializer[MetaBlock],
            toc_serialization_info: TOCSerializationInfo[FileDef],
    ):
        super().__init__(
            version=version,
            meta_serializer=meta_serializer,
            toc_serializer=toc_serializer,
            toc_meta_serializer=None,
            toc_serialization_info=toc_serialization_info,
            assemble_meta=assemble_meta,
            disassemble_meta=disassemble_meta,
            build_file_meta=lambda _: {},
            gen_empty_meta=MetaBlock.default,
            finalize_meta=recalculate_md5,
            meta2def=meta2def,
            assembler=_AssemblerV2,
            disassembler=_DisassassemblerV2,
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

essence_fs_serializer = EssenceFSSerializer(
    meta_serializer=_meta_header_serializer,
    toc_serializer=_toc_header_serializer,
    toc_serialization_info=TOCSerializationInfo(
        file=_file_serializer,
        drive=_drive_serializer,
        folder=_folder_serializer,
        name_toc_is_count=True,
    ),
)

registry.auto_register(essence_fs_serializer)

__all__ = [
    "FileDefSerializer",
    "MetaBlock",
    "ArchiveHeaderSerializer",
    # "ArchiveSerializer",
    # "archive_serializer",
    "essence_fs_serializer",
]

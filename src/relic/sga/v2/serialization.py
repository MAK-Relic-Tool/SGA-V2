"""
Binary Serializers for Relic's SGA-V2
"""
from __future__ import annotations

import hashlib
from io import BytesIO
from typing import BinaryIO, Optional, Tuple, Union

from relic.sga.v2.definitions import version as local_version
from relic.sga.core._proxyfs import SgaFs, LazySgaFs
from relic.sga.core.definitions import Version, StorageType
from relic.sga.core.essencesfs import (
    EssenceFS,
    LazyEssenceFS,
    _ns_supports,
    _ns_essence,
)
from relic.sga.core.lazyio import BinaryWindow, LazyBinary, tell_end, read_chunks
from relic.sga.core.opener import registry, _FakeSerializer
from relic.sga.core.serialization import (
    SgaMetaBlock,
    SgaTocHeader,
    SgaTocDrive,
    SgaTocFolder,
    SgaNameWindow,
    SgaTocInfoArea,
    SgaToc,
    SgaFile,
    SgaTocFile,
)


def _next(offset, size):
    return offset + size


_FILE_MD5_EIGEN = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
_HEADER_MD5_EIGEN = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"


class SgaMetaBlockV2(SgaMetaBlock):
    _FILE_MD5 = (0, 16)
    _NAME = (_next(*_FILE_MD5), 128)
    _HEADER_MD5 = (_next(*_NAME), 16)
    _HEADER_SIZE = (_next(*_HEADER_MD5), 4)
    _DATA_POS = (_next(*_HEADER_SIZE), 4)
    _SIZE = _next(*_DATA_POS)

    @property
    def file_md5(self) -> bytes:  # TODO
        return self._read_bytes(*self._FILE_MD5)

    @property
    def name(self) -> str:
        buffer = self._read_bytes(*self._NAME)
        terminated_str = self._unpack_str(buffer, "utf-16-le")
        result = terminated_str.rstrip("\0")
        return result

    @property
    def header_md5(self) -> bytes:  # TODO
        return self._read_bytes(*self._HEADER_MD5)

    @property
    def header_pos(self) -> int:
        result = self._SIZE + SgaFileV2._MAGIC_VERSION_SIZE  # 184 | 0xB8
        return result
        # pass

    @property
    def header_size(self) -> int:
        buffer = self._read_bytes(*self._HEADER_SIZE)
        return self._unpack_int(buffer)

    @property
    def data_pos(self) -> int:
        buffer = self._read_bytes(*self._DATA_POS)
        return self._unpack_int(buffer)

    @property
    def data_size(self) -> None:
        return None


class SgaTocHeaderV2(SgaTocHeader):
    _DRIVE_POS = (0, 4)
    _DRIVE_COUNT = (4, 2)
    _FOLDER_POS = (6, 4)
    _FOLDER_COUNT = (10, 2)
    _FILE_POS = (12, 4)
    _FILE_COUNT = (16, 2)
    _NAME_POS = (18, 4)
    _NAME_COUNT = (22, 2)
    _SIZE = 24


class SgaTocDriveV2(SgaTocDrive):
    _PATH = (0, 64)
    _NAME = (_next(*_PATH), 64)
    _FIRST_FOLDER = (_next(*_NAME), 2)
    _LAST_FOLDER = (_next(*_FIRST_FOLDER), 2)
    _FIRST_FILE = (_next(*_LAST_FOLDER), 2)
    _LAST_FILE = (_next(*_FIRST_FILE), 2)
    _ROOT_FOLDER = (_next(*_LAST_FILE), 2)
    _SIZE = _next(*_ROOT_FOLDER)


class SgaTocFolderV2(SgaTocFolder):
    _NAME_OFFSET = (0, 4)
    _SUB_FOLDER_START = (_next(*_NAME_OFFSET), 2)
    _SUB_FOLDER_STOP = (_next(*_SUB_FOLDER_START), 2)
    _FIRST_FILE = (_next(*_SUB_FOLDER_STOP), 2)
    _LAST_FILE = (_next(*_FIRST_FILE), 2)
    _SIZE = _next(*_LAST_FILE)


class _SgaTocFileV2(SgaTocFile, LazyBinary):
    _NAME_OFFSET = None
    _FLAGS = None
    _DATA_OFFSET = None
    _COMP_SIZE = None
    _DECOMP_SIZE = None
    _SIZE = None
    _STORAGE_TYPE_MASK = 0xF0  # 00, 10, 20
    _STORAGE_TYPE_SHIFT = 4

    def __init__(self, parent: BinaryIO):
        super().__init__(parent)

    @property
    def name_offset(self):  # name_rel_pos
        buffer = self._read_bytes(*self._NAME_OFFSET)
        return self._unpack_int(buffer)

    @property
    def data_offset(self):  # data_rel_pos
        buffer = self._read_bytes(*self._DATA_OFFSET)
        return self._unpack_int(buffer)

    @property
    def compressed_size(self):  # length_in_archive
        buffer = self._read_bytes(*self._COMP_SIZE)
        return self._unpack_int(buffer)

    @property
    def decompressed_size(self):  # length_on_disk
        buffer = self._read_bytes(*self._DECOMP_SIZE)
        return self._unpack_int(buffer)

    @property
    def storage_type(self):
        buffer = self._read_bytes(*self._FLAGS)
        value = (
            self._unpack_int(buffer) & self._STORAGE_TYPE_MASK
        ) >> self._STORAGE_TYPE_SHIFT
        return StorageType(value)  # V2 uses


class SgaTocFileV2DoW(_SgaTocFileV2):
    _NAME_OFFSET = (0, 4)
    _FLAGS = (_next(*_NAME_OFFSET), 4)
    _DATA_OFFSET = (_next(*_FLAGS), 4)
    _COMP_SIZE = (_next(*_DATA_OFFSET), 4)
    _DECOMP_SIZE = (_next(*_COMP_SIZE), 4)
    _SIZE = _next(*_DECOMP_SIZE)


class SgaTocFileV2ImpCreatures(_SgaTocFileV2):
    _NAME_OFFSET = (0, 4)
    _FLAGS = (_next(*_NAME_OFFSET), 1)
    _DATA_OFFSET = (_next(*_FLAGS), 4)
    _COMP_SIZE = (_next(*_DATA_OFFSET), 4)
    _DECOMP_SIZE = (_next(*_COMP_SIZE), 4)
    _SIZE = _next(*_DECOMP_SIZE)


class SgaTocV2(SgaToc):
    def __init__(self, parent: BinaryIO):
        super().__init__(parent)
        self._header = SgaTocHeaderV2(parent)
        self._drives = SgaTocInfoArea(
            parent, *self._header.drive_info, cls=SgaTocDriveV2
        )
        self._folders = SgaTocInfoArea(
            parent, *self._header.folder_info, cls=SgaTocFolderV2
        )

        # Unfortunately DoW and IC (Steam) have a slightly different file layout
        # DoW is 20 and IC is 17
        # We can determine which via comparing the size of the full block
        if self._header.file_info[1] == 0:
            # Just pick one, shouldn't matter; no files present
            self._files = SgaTocInfoArea(
                parent, *self._header.file_info, cls=SgaTocFileV2DoW
            )
        else:
            # TODO properly determine the start of the next block (which may actual be the end of the header)
            # We currently assume that it is the name block, because it always 'should' be next
            file_block_size = self._header.name_info[0] - self._header.file_info[0]
            toc_file_size = file_block_size / self._header.file_info[1]
            variants = [SgaTocFileV2DoW, SgaTocFileV2ImpCreatures]
            self._files = None
            for variant in variants:
                if variant._SIZE == toc_file_size:
                    self._files = SgaTocInfoArea(
                        parent, *self._header.file_info, cls=variant
                    )
                    break
            if self._files is None:
                raise NotImplementedError(toc_file_size)
        self._names = SgaNameWindow(parent, *self._header.name_info)

    @property
    def header(self) -> SgaTocHeader:
        return self._header

    @property
    def drives(self) -> SgaTocInfoArea[SgaTocDrive]:
        return self._drives

    @property
    def folders(self) -> SgaTocInfoArea[SgaTocFolder]:
        return self._folders

    @property
    def files(self) -> SgaTocInfoArea[SgaTocFile]:
        return self._files

    @property
    def names(self) -> BinaryWindow:
        return self._names


def _md5_checksum(
    stream: BinaryIO,
    start: int,
    size: Optional[int] = None,
    eigen: Optional[bytes] = None,
    expected: Optional[bytes] = None,
) -> Tuple[bytes, Optional[bool]]:
    hasher = hashlib.md5(eigen)
    for chunk in read_chunks(stream, start, size):
        hasher.update(chunk)
    hash = hasher.digest()
    match = None
    if expected is not None:
        match = hash == expected
    return hash, match


class SgaFileV2(SgaFile):
    _META_BLOCK = (SgaFile._MAGIC_VERSION_SIZE, SgaMetaBlockV2._SIZE)

    def __init__(self, parent: BinaryIO):
        super().__init__(parent)
        self._meta = SgaMetaBlockV2(BinaryWindow(parent, *self._META_BLOCK))
        self._header_window = BinaryWindow(
            parent, self._meta.header_pos, self._meta.header_size
        )
        _data_start = self._meta.data_pos
        _data_end = tell_end(parent)  # Terminal not specified in V2
        _data_size = _data_end - _data_start
        self._data_window = BinaryWindow(parent, _data_start, _data_size)
        self._toc = SgaTocV2(self._header_window)

    def verify(self):
        return self.verify_header() and self.verify_file()

    def verify_file(self, cached: bool = True):
        if not cached or not hasattr(self, "__verified_file"):
            _hash, self.__verified_file = _md5_checksum(
                self._parent,
                self._meta.header_pos,
                eigen=_FILE_MD5_EIGEN,
                expected=self._meta.file_md5,
            )
        return self.__verified_file

    def verify_header(self, cached: bool = True):
        if not cached or not hasattr(self, "__verified_header"):
            _hash, self.__verified_header = _md5_checksum(
                self._parent,
                self._meta.header_pos,
                self._meta.header_size,
                eigen=_HEADER_MD5_EIGEN,
                expected=self._meta.header_md5,
            )
        return self.__verified_header

    @property
    def meta(self) -> SgaMetaBlock:
        return self._meta

    @property
    def table_of_contents(self) -> SgaToc:
        return self._toc

    @property
    def data_block(self) -> BinaryWindow:
        return self._data_window


class EssenceFSV2(LazyEssenceFS):
    _essence = _ns_essence(version=local_version)
    _supports = _ns_supports(archive_verification=True)

    def __init__(self, data: Union[SgaFile, SgaFs, BinaryIO]):
        stream = sga = fakefs = None
        if isinstance(data, SgaFs):
            fakefs = data
        elif isinstance(data, SgaFile):
            sga = data
        elif isinstance(
            data, BinaryIO
        ):  # MUST COME LAST ~ SgaFile implements BinaryWrapper
            stream = data
        else:
            raise NotImplementedError(data)

        if stream is not None:
            sga = SgaFileV2(stream)
        if sga is not None:
            fakefs = LazySgaFs(sga)
        if fakefs is None:
            raise NotImplementedError(fakefs, sga, stream, data)

        super().__init__(fakefs._stream, fakefs)

    def getmeta(self, namespace: str = "standard"):
        return super().getmeta(namespace)

class EssenceFSSerializer(_FakeSerializer):
    """
    Serializer to read/write an SGA file to/from a stream from/to a SGA File System
    """

    version = local_version
    autoclose = False

    def read(self, stream: BinaryIO) -> EssenceFS:
        return EssenceFSV2(stream)

    def write(self, stream: BinaryIO, essence_fs: EssenceFS) -> int:
        raise NotImplementedError


essence_fs_serializer = EssenceFSSerializer()
registry.auto_register(essence_fs_serializer)

__all__ = [
    "essence_fs_serializer",
]

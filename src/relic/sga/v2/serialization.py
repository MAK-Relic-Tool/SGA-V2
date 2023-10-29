"""
Binary Serializers for Relic's SGA-V2

"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from enum import Enum
from typing import BinaryIO, Optional, Union, Literal, Tuple, Any

from relic.core.errors import RelicToolError
from relic.core.lazyio import BinaryWindow, ZLibFileReader, tell_end, BinaryProxySerializer
from relic.sga.core.definitions import StorageType
from relic.sga.core.hashtools import md5
from relic.sga.core.serialization import (
    SgaHeader,
    SgaTocHeader,
    SgaTocDrive,
    SgaTocFolder,
    SgaNameWindow,
    SgaTocInfoArea,
    SgaToc,
    SgaFile,
    SgaTocFile,
)


def _repr_name(t: Any):
    klass = t.__class__
    module = klass.__module__
    return ".".join([module, klass.__qualname__])


def _repr_obj(self, *args: str, name: str = None, **kwargs):
    klass_name = _repr_name(self)
    for arg in args:
        kwargs[arg] = getattr(self, arg)
    kwarg_line = ", ".join(f"{k}='{v}'" for k, v in kwargs.items())
    if len(kwarg_line) > 0:
        kwarg_line = f" ({kwarg_line})"  # space at start to avoid if below
    if name is None:
        return f"<{klass_name}{kwarg_line}>"
    else:
        return f"<{klass_name} '{name}'{kwarg_line}>"


class RelicUnixTimeSerializer:
    LE: Literal["little"] = "little"

    @classmethod
    def pack(cls, value: Union[float, int]) -> bytes:
        int_value = int(value)
        return int_value.to_bytes(4, cls.LE, signed=True)

    @classmethod
    def unpack(cls, buffer: bytes) -> int:
        return int.from_bytes(buffer, cls.LE, signed=False)


class RelicDateTimeSerializer:
    LE: Literal["little"] = "little"

    @classmethod
    def pack(cls, value: datetime) -> bytes:
        unix_value = cls.datetime2unix(value)
        return RelicUnixTimeSerializer.pack(unix_value)

    @classmethod
    def unpack(cls, buffer: bytes) -> datetime:
        value = RelicUnixTimeSerializer.unpack(buffer)
        return cls.unix2datetime(value)

    @classmethod
    def unix2datetime(cls, value: Union[int, float]):
        return datetime.fromtimestamp(value, timezone.utc)

    @classmethod
    def datetime2unix(cls, value: datetime) -> float:
        return value.replace(tzinfo=timezone.utc).timestamp()


def _next(offset, size):
    return offset + size


_FILE_MD5_EIGEN = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
_TOC_MD5_EIGEN = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"


class SgaHeaderV2(SgaHeader):
    _FILE_MD5 = (0, 16)
    _NAME = (_next(*_FILE_MD5), 128)
    _TOC_MD5 = (_next(*_NAME), 16)
    _TOC_SIZE = (_next(*_TOC_MD5), 4)
    _DATA_POS = (_next(*_TOC_SIZE), 4)
    _SIZE = _next(*_DATA_POS)
    _TOC_POS = 180
    _NAME_ENC = "utf-16-le"
    _NAME_PAD = "\0"
    _INT_FMT = {"byteorder":"little","signed":False}

    @property
    def file_md5(
        self,
    ) -> bytes:  # I marked this as a 'to do' but what did i need to 'to do'?
        return self._serializer.read_bytes(*self._FILE_MD5)

    @file_md5.setter
    def file_md5(self, value: bytes):
        self._serializer.write_bytes(value, *self._FILE_MD5)

    @property
    def name(self) -> str:
        return self._serializer.c_string.read(*self._NAME,encoding=self._NAME_ENC,padding=self._NAME_PAD)

    @name.setter
    def name(self, value: str):
        self._serializer.c_string.write(value,*self._NAME,encoding=self._NAME_ENC,padding=self._NAME_PAD)

    @property
    def toc_md5(
        self,
    ) -> bytes:  # I marked this as a 'to do' but what did i need to 'to do'?
        return self._serializer.read_bytes(*self._TOC_MD5)

    @toc_md5.setter
    def toc_md5(self, value: bytes):
        self._serializer.write_bytes(value, *self._TOC_MD5)

    @property
    def toc_pos(self) -> int:
        result = (
            self._TOC_POS
        )  # self._SIZE + SgaFileV2._MAGIC_VERSION_SIZE  # 184 | 0xB8
        return result
        # pass

    @toc_pos.setter
    def toc_pos(self, value: bytes):
        raise RelicToolError(
            "Header Pos is fixed in SGA v2!"
        )  # TODO raise an explicit `not writable` error

    @property
    def toc_size(self) -> int:
        return self._serializer.uint32.read(*self._TOC_SIZE,**self._INT_FMT)

    @toc_size.setter
    def toc_size(self, value: int):
        self._serializer.uint32.write(value,*self._TOC_SIZE,**self._INT_FMT)

    @property
    def data_pos(self) -> int:
        return self._serializer.uint32.read(*self._DATA_POS,**self._INT_FMT)

    @data_pos.setter
    def data_pos(self, value: int):
        self._serializer.uint32.write(value,*self._DATA_POS,**self._INT_FMT)

    @property
    def data_size(self) -> None:
        return None

    @data_size.setter
    def data_size(self, value: None):
        raise RelicToolError(
            "Data Size is not specified in SGA v2!"
        )  # TODO raise an explicit `not writable` error

    def __repr__(self):
        return _repr_obj(
            self,
            "file_md5",
            "header_md5",
            "toc_pos",
            "toc_size",
            "data_pos",
            "data_size",
            name=self.name,
        )


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
    _ALIAS = (0, 64)
    _NAME = (_next(*_ALIAS), 64)
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


class _SgaTocFileV2(SgaTocFile, BinaryProxySerializer):
    _NAME_OFFSET: Tuple[int, int] = None
    _FLAGS: Tuple[int, int] = None
    _DATA_OFFSET: Tuple[int, int] = None
    _COMP_SIZE: Tuple[int, int] = None
    _DECOMP_SIZE: Tuple[int, int] = None
    _SIZE: int = None
    _STORAGE_TYPE_MASK: int = 0xF0  # 00, 10, 20
    _STORAGE_TYPE_SHIFT: int = 4
    _INT_FORMAT = {"byteorder":"little","signed":False}

    def __init__(self, parent: BinaryIO):
        super().__init__(parent)

    @property
    def name_offset(self):  # name_rel_pos
        return self._serializer.int.read(*self._NAME_OFFSET,**self._INT_FORMAT)

    @name_offset.setter
    def name_offset(self, value: int):
        self._serializer.int.write(value,*self._NAME_OFFSET,**self._INT_FORMAT)

    @property
    def data_offset(self):  # data_rel_pos
        return self._serializer.int.read(*self._DATA_OFFSET,**self._INT_FORMAT)

    @data_offset.setter
    def data_offset(self, value: int):
        self._serializer.int.write(value,*self._DATA_OFFSET,**self._INT_FORMAT)

    @property
    def compressed_size(self):  # length_in_archive
        return self._serializer.int.read(*self._COMP_SIZE,**self._INT_FORMAT)

    @compressed_size.setter
    def compressed_size(self, value: int):
        self._serializer.int.write(value,*self._COMP_SIZE,**self._INT_FORMAT)

    @property
    def decompressed_size(self):  # length_on_disk
        return self._serializer.int.read(*self._DECOMP_SIZE,**self._INT_FORMAT)

    @decompressed_size.setter
    def decompressed_size(self, value: int):
        self._serializer.int.write(value,*self._DECOMP_SIZE,**self._INT_FORMAT)

    @property
    def storage_type(self) -> StorageType:
        """
        The Storage Type that the
        """
        value = self._serializer.int.read(*self._FLAGS,**self._INT_FORMAT)
        value &= self._STORAGE_TYPE_MASK
        value >>= self._STORAGE_TYPE_SHIFT
        return StorageType(value)

    @storage_type.setter
    def storage_type(self, value: StorageType):
        # assuming this IS IN FACT, a flag value, we need to read it to edit it
        flag = value << self._STORAGE_TYPE_SHIFT
        buffer_value = self._serializer.int.read(*self._FLAGS,**self._INT_FORMAT)
        buffer_value &= ~self._STORAGE_TYPE_MASK  # clear storage flag
        buffer_value |= flag  # apply storage flag
        self._serializer.int.write(buffer_value,*self._FLAGS,**self._INT_FORMAT)


class SgaTocFileV2Dow(_SgaTocFileV2):
    _NAME_OFFSET = (0, 4)
    _FLAGS = (_next(*_NAME_OFFSET), 4)
    _DATA_OFFSET = (_next(*_FLAGS), 4)
    _COMP_SIZE = (_next(*_DATA_OFFSET), 4)
    _DECOMP_SIZE = (_next(*_COMP_SIZE), 4)
    _SIZE = _next(*_DECOMP_SIZE)


class SgaTocFileDataHeaderV2Dow(BinaryProxySerializer):
    _NAME_OFFSET = (0, 256)
    _MODIFIED_OFFSET = (_next(*_NAME_OFFSET), 4)
    _CRC_OFFSET = (_next(*_MODIFIED_OFFSET), 4)
    _SIZE = _next(*_CRC_OFFSET)
    _NAME_ENC = "ascii"
    _NAME_PADDING = "\0"
    _INT_FORMAT = {"byteorder":"little","signed":False}

    @property
    def name(self) -> str:
        return self._serializer.c_string.read(*self._NAME_OFFSET,encoding=self._NAME_ENC,padding=self._NAME_PADDING)

    @name.setter
    def name(self, value: str):
        self._serializer.c_string.write(value,*self._NAME_OFFSET,encoding=self._NAME_ENC,padding=self._NAME_PADDING)

    @property
    def modified(self) -> int:
        """
        The time (from the unix epoch) when this file was modified.
        Measured to the second, fractions of a second are truncated.
        """
        buffer = self._serializer.read_bytes(*self._MODIFIED_OFFSET)
        return RelicUnixTimeSerializer.unpack(buffer)

    @modified.setter
    def modified(self, value: Union[float, int]):
        buffer = RelicUnixTimeSerializer.pack(value)
        _ = self._serializer.write_bytes(buffer, *self._MODIFIED_OFFSET)

    @property
    def crc32(self) -> int:
        return self._serializer.int.read(*self._CRC_OFFSET,**self._INT_FORMAT)

    @crc32.setter
    def crc32(self, value: int):
        self._serializer.int.write(value,*self._CRC_OFFSET,**self._INT_FORMAT)


class SgaTocFileDataV2Dow:
    def __init__(
        self,
        toc_file: SgaTocFile,
        name_window: SgaNameWindow,
        data_window: BinaryWindow,
    ):
        self._toc_file = toc_file
        self._name_window = name_window
        self._data_window = data_window

        size = SgaTocFileDataHeaderV2Dow._SIZE
        offset = self._toc_file.data_offset - size
        _data_header_window = BinaryWindow(self._data_window, offset, size)
        self._data_header = SgaTocFileDataHeaderV2Dow(_data_header_window)

    @property
    def name(self):
        return self._name_window.get_name(self._toc_file.name_offset)

    @property
    def header(self) -> SgaTocFileDataHeaderV2Dow:
        return self._data_header

    def data(self, decompress: bool = True) -> BinaryIO:
        offset = self._toc_file.data_offset
        size = self._toc_file.compressed_size
        window = BinaryWindow(self._data_window, offset, size)
        if decompress and self._toc_file.storage_type != StorageType.STORE:
            return ZLibFileReader(window)
        return window


class SgaTocFileV2ImpCreatures(_SgaTocFileV2):
    _NAME_OFFSET = (0, 4)
    _FLAGS = (_next(*_NAME_OFFSET), 1)
    _DATA_OFFSET = (_next(*_FLAGS), 4)
    _COMP_SIZE = (_next(*_DATA_OFFSET), 4)
    _DECOMP_SIZE = (_next(*_COMP_SIZE), 4)
    _SIZE = _next(*_DECOMP_SIZE)


class SgaV2GameFormat(Enum):
    DawnOfWar = "Dawn Of War"
    ImpossibleCreatures = "Impossible Creatures"


GAME_FORMAT_TOC_FILE = {
    SgaV2GameFormat.DawnOfWar: SgaTocFileV2Dow,
    SgaV2GameFormat.ImpossibleCreatures: SgaTocFileV2ImpCreatures,
}
GAME_FORMAT_TOC_FILE_DATA = {
    SgaV2GameFormat.DawnOfWar: SgaTocFileDataV2Dow,
    SgaV2GameFormat.ImpossibleCreatures: None,
}


class SgaTocV2(SgaToc):
    @classmethod
    def _determine_next_header_block_ptr(
        cls, header: SgaTocHeaderV2, toc_end:int,  index: int = -1,
    ) -> int:
        smallest = toc_end
        ptrs = [
            header.folder.offset,
            header.drive.offset,
            header.file.offset,
            header.name.offset,
        ]
        for ptr in ptrs:
            if index < ptr < smallest:
                smallest = ptr
        return smallest

    @classmethod
    def _determine_game(cls, header: SgaTocHeaderV2, toc_end:int):
        # Unfortunately DoW and IC (Steam) have a slightly different file layout
        # DoW is 20 and IC is 17
        # We can determine which via comparing the size of the full block
        file_block_start, file_count = header.file.info

        if file_count == 0:
            raise RelicToolError(
                f"Game format could not be determined; no files in file block."
            )

        file_block_end = cls._determine_next_header_block_ptr(header, toc_end, index=file_block_start)
        file_block_size = file_block_end - file_block_start
        file_def_size = file_block_size / file_count

        for game_format, format_class in GAME_FORMAT_TOC_FILE.items():
            if format_class._SIZE == file_def_size:
                return game_format
        EXPECTED = [
            f"'{format_class._SIZE}' ({game_format.value})"
            for (game_format, format_class) in GAME_FORMAT_TOC_FILE.items()
        ]  #
        raise RelicToolError(
            f"Game format could not be determined; expected '{EXPECTED}', received `{file_def_size}."
        )

    def __init__(self, parent: BinaryIO, game: Optional[SgaV2GameFormat] = None):
        super().__init__(parent)
        self._header = SgaTocHeaderV2(parent)
        self._drives = SgaTocInfoArea(
            parent, *self._header.drive.info, cls=SgaTocDriveV2
        )
        self._folders = SgaTocInfoArea(
            parent, *self._header.folder.info, cls=SgaTocFolderV2
        )
        if game is None:
            now = parent.tell()
            end = parent.seek(0,os.SEEK_END)
            parent.seek(now)
            game = self._determine_game(self._header, end)
        self._game_format = game

        self._files = SgaTocInfoArea(
            parent, *self._header.file.info, cls=GAME_FORMAT_TOC_FILE[self._game_format]
        )
        self._names = SgaNameWindow(parent, *self._header.name.info)

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
    def names(self) -> SgaNameWindow:
        return self._names

    @property
    def game_format(self) -> SgaV2GameFormat:
        return self._game_format


class SgaFileV2(SgaFile):
    _META_BLOCK = (SgaFile._MAGIC_VERSION_SIZE, SgaHeaderV2._SIZE)

    def __init__(self, parent: BinaryIO, game_format: Optional[SgaV2GameFormat] = None):
        super().__init__(parent)
        self._meta = SgaHeaderV2(BinaryWindow(parent, *self._META_BLOCK))
        self._header_window = BinaryWindow(
            parent, self._meta.toc_pos, self._meta.toc_size
        )
        _data_start = self._meta.data_pos
        _data_end = tell_end(parent)  # Terminal not specified in V2
        _data_size = _data_end - _data_start
        self._data_window = BinaryWindow(parent, _data_start, _data_size)
        self._toc = SgaTocV2(self._header_window, game=game_format)

    def __verify(
        self, cached: bool, error: bool, hasher: md5, expected: bytes, cache_name: str
    ) -> bool:
        if (
            self._serializer.stream.writable()
            or error  # we can't use the cache if we want to error
            or not cached
            or not hasattr(self, cache_name)
        ):
            args: Tuple[BinaryIO, bytes] = self._serializer.stream, expected
            if not error:
                result = hasher.check(*args)
            else:
                hasher.validate(*args)
                result = True
            setattr(self, cache_name, result)

        return getattr(self, cache_name)

    def verify_file(self, cached: bool = True, error: bool = False) -> bool:
        NAME = "__verified_file"
        hasher = md5(
            self._meta.toc_pos,
            eigen=_FILE_MD5_EIGEN,
        )
        return self.__verify(
            cached=cached,
            error=error,
            hasher=hasher,
            expected=self._meta.file_md5,
            cache_name=NAME,
        )

    def verify_header(self, cached: bool = True, error: bool = False) -> bool:
        NAME = "__verified_file"
        hasher = md5(
            self._meta.toc_pos,
            self._meta.toc_size,
            eigen=_TOC_MD5_EIGEN,
        )
        return self.__verify(
            cached=cached,
            error=error,
            hasher=hasher,
            expected=self._meta.toc_md5,
            cache_name=NAME,
        )

    @property
    def meta(self) -> SgaHeaderV2:
        return self._meta

    @property
    def table_of_contents(self) -> SgaTocV2:
        return self._toc

    @property
    def data_block(self) -> BinaryWindow:
        return self._data_window

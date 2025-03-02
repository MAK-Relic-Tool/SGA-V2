"""Binary Serializers for Relic's SGA-V2."""

from __future__ import annotations

import logging
import os
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import (
    BinaryIO,
    Optional,
    Union,
    Literal,
    Tuple,
    Any,
    Dict,
    TypeVar,
    Protocol,
    Generic,
)

from relic.core.errors import RelicToolError
from relic.core.lazyio import (
    BinaryWindow,
    ZLibFileReader,
    tell_end,
    BinaryProxySerializer,
    ByteConverter,
    CStringConverter,
    IntConverter,
)
from relic.core.lazyio import (
    BinaryProperty,
    ConstProperty,
    #    get_BinaryProxySerializer LogProperty,
)
from relic.sga.core.definitions import StorageType
from relic.sga.core.hashtools import md5, Hasher
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

logger = logging.getLogger(__name__)


def _repr_name(t: Any) -> str:
    klass = t.__class__
    module = klass.__module__
    return ".".join([module, klass.__qualname__])


def _repr_obj(self: Any, *args: str, name: Optional[str] = None, **kwargs: Any) -> str:
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
    def unix2datetime(cls, value: Union[int, float]) -> datetime:
        return datetime.fromtimestamp(value, timezone.utc)

    @classmethod
    def datetime2unix(cls, value: datetime) -> float:
        return value.replace(tzinfo=timezone.utc).timestamp()


def _next(offset: int, size: int) -> int:
    return offset + size


_FILE_MD5_EIGEN = b"E01519D6-2DB7-4640-AF54-0A23319C56C3"
_TOC_MD5_EIGEN = b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"

_T = TypeVar("_T")


class LogProperty(Generic[_T]):
    def __init__(
        self,
        property: BinaryProperty[_T] | ConstProperty[_T],
        name: str,
        logger: logging.Logger,
    ):
        self._property = property

    def __get__(self, instance: Any, owner: Any) -> _T:
        return self._property.__get__(instance, owner)

    def __set__(self, instance: Any, value: _T) -> None:
        return self._property.__set__(instance, value)

    @property
    def __doc__(self) -> Optional[str]:
        return self._property.__doc__

    @__doc__.setter
    def __doc__(self, value: Optional[str]) -> None:
        self._property.__doc__ = value


class SgaHeaderV2(SgaHeader):
    class Meta:
        file_md5_ptr = (0, 16)
        name_ptr = (16, 128)
        toc_md5_ptr = (144, 16)
        toc_size_ptr = (160, 4)
        data_pos_ptr = (164, 4)
        size = 168

        toc_pos = 180

        name_converter = CStringConverter("utf-16-le", "\0", name_ptr[1])
        uint32le_converter = IntConverter(4, "little", signed=False)

    file_md5: bytes = LogProperty(  # type: ignore
        BinaryProperty(*Meta.file_md5_ptr, converter=ByteConverter), "file_md5", logger
    )

    name: str = LogProperty(  # type: ignore
        BinaryProperty(*Meta.name_ptr, converter=Meta.name_converter), "name", logger
    )

    toc_md5: bytes = LogProperty(
        BinaryProperty(*Meta.toc_md5_ptr, converter=ByteConverter), "toc_md5", logger
    )  # type: ignore

    # Todo raise an explicit not writable error
    toc_pos: int = LogProperty(  # type: ignore
        ConstProperty(Meta.toc_pos, RelicToolError("Header Pos is fixed in SGA v2!")),
        "toc_pos",
        logger,
    )
    toc_size: int = LogProperty(  # type: ignore
        BinaryProperty(*Meta.toc_size_ptr, converter=Meta.uint32le_converter),
        "toc_size",
        logger,
    )
    data_pos: int = LogProperty(  # type: ignore
        BinaryProperty(*Meta.data_pos_ptr, converter=Meta.uint32le_converter),
        "data_pos",
        logger,
    )

    data_size: None = LogProperty(  # type: ignore
        ConstProperty(None, RelicToolError("Data Size is not specified in SGA v2!")),
        "data_size",
        logger,
    )

    def __repr__(self) -> str:
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
    _NAME_OFFSET: Tuple[int, int] = None  # type: ignore
    _FLAGS: Tuple[int, int] = None  # type: ignore
    _DATA_OFFSET: Tuple[int, int] = None  # type: ignore
    _COMP_SIZE: Tuple[int, int] = None  # type: ignore
    _DECOMP_SIZE: Tuple[int, int] = None  # type: ignore
    _SIZE: int = None  # type: ignore
    _STORAGE_TYPE_MASK: int = 0xF0  # 00, 10, 20
    _STORAGE_TYPE_SHIFT: int = 4
    _INT_FORMAT = {"byteorder": "little", "signed": False}

    def __init__(self, parent: BinaryIO):
        super().__init__(parent)

    @property
    def name_offset(self) -> int:  # name_rel_pos
        return self._serializer.int.read(*self._NAME_OFFSET, **self._INT_FORMAT)  # type: ignore

    @name_offset.setter
    def name_offset(self, value: int) -> None:
        self._serializer.int.write(value, *self._NAME_OFFSET, **self._INT_FORMAT)  # type: ignore

    @property
    def data_offset(self) -> int:  # data_rel_pos
        return self._serializer.int.read(*self._DATA_OFFSET, **self._INT_FORMAT)  # type: ignore

    @data_offset.setter
    def data_offset(self, value: int) -> None:
        self._serializer.int.write(value, *self._DATA_OFFSET, **self._INT_FORMAT)  # type: ignore

    @property
    def compressed_size(self) -> int:  # length_in_archive
        return self._serializer.int.read(*self._COMP_SIZE, **self._INT_FORMAT)  # type: ignore

    @compressed_size.setter
    def compressed_size(self, value: int) -> None:
        self._serializer.int.write(value, *self._COMP_SIZE, **self._INT_FORMAT)  # type: ignore

    @property
    def decompressed_size(self) -> int:  # length_on_disk
        return self._serializer.int.read(*self._DECOMP_SIZE, **self._INT_FORMAT)  # type: ignore

    @decompressed_size.setter
    def decompressed_size(self, value: int) -> None:
        self._serializer.int.write(value, *self._DECOMP_SIZE, **self._INT_FORMAT)  # type: ignore

    @property
    def storage_type(self) -> StorageType:
        """The Storage Type that determines whether the file is stored as-is, or
        compressed."""
        value = self._serializer.int.read(*self._FLAGS, **self._INT_FORMAT)  # type: ignore
        value &= self._STORAGE_TYPE_MASK
        value >>= self._STORAGE_TYPE_SHIFT
        return StorageType(value)

    @storage_type.setter
    def storage_type(self, value: StorageType) -> None:
        # assuming this IS IN FACT, a flag value, we need to read it to edit it
        flag = value << self._STORAGE_TYPE_SHIFT
        buffer_value = self._serializer.int.read(*self._FLAGS, **self._INT_FORMAT)  # type: ignore
        buffer_value &= ~self._STORAGE_TYPE_MASK  # clear storage flag
        buffer_value |= flag  # apply storage flag
        self._serializer.int.write(buffer_value, *self._FLAGS, **self._INT_FORMAT)  # type: ignore


class SgaTocFileV2Dow(_SgaTocFileV2):
    _NAME_OFFSET = (0, 4)
    _FLAGS = (_next(*_NAME_OFFSET), 4)
    _DATA_OFFSET = (_next(*_FLAGS), 4)
    _COMP_SIZE = (_next(*_DATA_OFFSET), 4)
    _DECOMP_SIZE = (_next(*_COMP_SIZE), 4)
    _SIZE = _next(*_DECOMP_SIZE)


# UGH; these names :yikes:
class SgaTocFileDataHeaderV2DowProtocol(Protocol):
    name: str
    crc32: int
    modified: int


@dataclass
class MemSgaTocFileDataHeaderV2Dow(SgaTocFileDataHeaderV2DowProtocol):
    name: str
    crc32: int
    modified: int

    @classmethod
    def create(
        cls, name: str, data: bytes, time: Optional[int] = None
    ) -> MemSgaTocFileDataHeaderV2Dow:
        from relic.sga.core import hashtools

        crc32 = hashtools.crc32.hash(data)

        if time is None:
            now = datetime.now()
            time = int(RelicDateTimeSerializer.datetime2unix(now))
        return MemSgaTocFileDataHeaderV2Dow(name, crc32, time)


class LazySgaTocFileDataHeaderV2Dow(
    BinaryProxySerializer, SgaTocFileDataHeaderV2DowProtocol
):
    class Meta:
        name_ptr = (0, 256)
        modified_ptr = (256, 4)
        crc_ptr = (260, 4)
        SIZE = 264

        name_cstring_converter = CStringConverter(
            encoding="ascii", padding="\0", size=name_ptr[1]
        )
        uint32le_converter = IntConverter(length=4, byteorder="little", signed=False)

    name: str = LogProperty(  # type: ignore
        BinaryProperty(*Meta.name_ptr, converter=Meta.name_cstring_converter),
        "name",
        logger,
    )
    crc32: int = LogProperty(  # type: ignore
        BinaryProperty(*Meta.crc_ptr, converter=Meta.uint32le_converter),
        "crc32",
        logger,
    )

    @property
    def modified(self) -> int:
        """The time (from the unix epoch) when this file was modified.

        Measured to the second, fractions of a second are truncated.
        """

        buffer = self._serializer.read_bytes(*self.Meta.modified_ptr)
        return RelicUnixTimeSerializer.unpack(buffer)

    @modified.setter
    def modified(self, value: Union[float, int]) -> None:
        buffer = RelicUnixTimeSerializer.pack(value)
        _ = self._serializer.write_bytes(buffer, *self.Meta.modified_ptr)

    def check_header_valid(self) -> bool:
        def _warn(name: str) -> None:
            logger.warning(
                f"Failed to parse File Data Header `{name}`, the header may be missing or invalid."
            )

        try:
            try:
                _name = self.name
            except (RelicToolError, UnicodeDecodeError) as _1:
                _warn("name")
                return False
            try:
                _crc32 = self.crc32
            except RelicToolError as _2:
                _warn("crc32")
                return False
            try:
                _modified = self.modified
            except RelicToolError as _3:
                _warn("modified")
                return False

        except Exception as e:
            logger.critical(
                """Encountered an unexpected error while performing the File Data Header's validation check.
    Please submit a bug report if one has not already been submitted.""",
                exc_info=e,
            )
        return True


class SgaTocFileDataV2:
    def __init__(
        self,
        toc_file: SgaTocFile,
        name_window: SgaNameWindow,
        data_window: BinaryWindow,
        has_data_header: Optional[bool] = False,
        has_safe_data_header: Optional[bool] = False,
    ):
        self._toc_file = toc_file
        self._name_window = name_window
        self._data_window = data_window

        size = LazySgaTocFileDataHeaderV2Dow.Meta.SIZE
        offset = self._toc_file.data_offset - size
        _data_header_window = BinaryWindow(self._data_window, offset, size)
        _lazy_data_header = LazySgaTocFileDataHeaderV2Dow(_data_header_window)

        self._data_header: SgaTocFileDataHeaderV2DowProtocol
        # We can safely use our properties here EXCEPT FOR DATA_HEADER PROPS
        if has_safe_data_header or (
            has_data_header and _lazy_data_header.check_header_valid()
        ):
            logger.debug(
                f"File `{self.name}` {'has' if has_safe_data_header else 'may have'} a Data Header"
            )
            self._data_header = _lazy_data_header
        else:
            logger.debug(f"File `{self.name}` is missing its Data Header")
            _name = self.name
            _data = self.data(True).read(-1)
            self._data_header = MemSgaTocFileDataHeaderV2Dow.create(_name, _data)

    @property
    def name(self) -> str:
        return self._name_window.get_name(self._toc_file.name_offset)

    @property
    def header(self) -> SgaTocFileDataHeaderV2DowProtocol:
        return self._data_header

    def data(self, decompress: bool = True) -> BinaryIO:
        logger.debug(
            f"Reading File Data from the Data Window (decompress={decompress})"
        )
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
    Unknown = "Unknown"


GAME_FORMAT_TOC_FILE = {
    SgaV2GameFormat.DawnOfWar: SgaTocFileV2Dow,
    SgaV2GameFormat.ImpossibleCreatures: SgaTocFileV2ImpCreatures,
}
GAME_FORMAT_TOC_FILE_DATA = {
    SgaV2GameFormat.DawnOfWar: SgaTocFileDataV2,
    SgaV2GameFormat.ImpossibleCreatures: None,
}


class SgaTocV2(SgaToc):
    @classmethod
    def _determine_next_header_block_ptr(
        cls,
        header: SgaTocHeaderV2,
        toc_end: int,
        index: int = -1,
    ) -> int:
        # SGA V2 has a static layout, but we should be lenient on our input
        """Determines the next table offset from the given index, or the toc end if
        there are no more tables."""

        smallest = toc_end
        ptrs = [
            header.drive.offset,
            header.folder.offset,
            header.file.offset,
            header.name.offset,
        ]
        for ptr in ptrs:
            if index < ptr < smallest:
                smallest = ptr
        return smallest

    @classmethod
    def _determine_game(cls, header: SgaTocHeaderV2, toc_end: int) -> SgaV2GameFormat:
        """
        Attempts to determine which V2 Specification the file table is using
        Dawn Of War (DoW) uses a 20 byte block
        Impossible Creatures: Steam Edition (IC) uses a 17 byte block
        If the file block is empty; Unknown is used to specify the format could not be determined.
        """
        # Unfortunately DoW and IC (Steam) have a slightly different file layout
        # DoW is 20 and IC is 17
        # We can determine which via comparing the size of the full block
        # IFF the file_count is 0, we can't determine the game, but we don't error since it is valid to have 0 files
        file_block_start, file_count = header.file.info

        if file_count == 0:
            logging.debug("Could not determine V2 type (DOW / IC); file count is 0!")
            return SgaV2GameFormat.Unknown

        file_block_end = cls._determine_next_header_block_ptr(
            header, toc_end, index=file_block_start
        )
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
            end = parent.seek(0, os.SEEK_END)
            parent.seek(now)
            game = self._determine_game(self._header, end)
        self._game_format = game

        toc_class = GAME_FORMAT_TOC_FILE.get(self._game_format)
        toc_offset, toc_count = self._header.file.info
        self._files = SgaTocInfoArea(parent, toc_offset, toc_count, cls=toc_class)  # type: ignore
        self._names = SgaNameWindow(parent, *self._header.name.info)

    @property
    def header(self) -> SgaTocHeader:
        return self._header

    @property
    def root_folders(self) -> SgaTocInfoArea[SgaTocDrive]:  # type: ignore
        return self._drives  # type: ignore

    @property
    def folders(self) -> SgaTocInfoArea[SgaTocFolder]:  # type: ignore
        return self._folders  # type: ignore

    @property
    def files(self) -> SgaTocInfoArea[SgaTocFile]:  # type: ignore
        return self._files  # type: ignore

    @property
    def names(self) -> SgaNameWindow:
        return self._names

    @property
    def game_format(self) -> SgaV2GameFormat:
        return self._game_format


class SgaFileV2(SgaFile):
    _META_BLOCK = (SgaFile._MAGIC_VERSION_SIZE, SgaHeaderV2.Meta.size)

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

        _expected_data_size = self.__determine_expected_data_window_size()
        self._has_file_data_headers = _expected_data_size <= _data_size
        self._has_safe_file_data_headers = _expected_data_size == _data_size
        logger.debug(
            f"File `{self._meta.name}` has {'' if self._has_file_data_headers else 'No '} {'Exact ' if self._has_safe_file_data_headers else ''}Headers"
        )

    def __determine_expected_data_window_size(self) -> int:
        total_header_size = (
            len(self._toc.files) * LazySgaTocFileDataHeaderV2Dow.Meta.SIZE
        )
        total_data_size = 0
        for __toc_file in self._toc.files:
            total_data_size += __toc_file.compressed_size
        return total_header_size + total_data_size

    def __verify(
        self,
        cached: bool,
        error: bool,
        hasher: Hasher[bytes],
        hash_kwargs: Dict[str, Any],
        expected: bytes,
        cache_name: str,
    ) -> bool:
        if (
            self._serializer.stream.writable()
            or error  # we can't use the cache if we want to error
            or not cached
            or not hasattr(self, cache_name)
        ):
            kwargs = hash_kwargs
            kwargs["stream"] = self._serializer.stream
            kwargs["expected"] = expected
            if not error:
                result = hasher.check(**kwargs)
            else:
                hasher.validate(**kwargs)
                result = True
            setattr(self, cache_name, result)

        return getattr(self, cache_name)  # type: ignore

    def verify_file(self, cached: bool = True, error: bool = False) -> bool:
        logger.debug(f"Verifying `{self._meta.name}` Header MD5")
        NAME = "__verified_file"
        return self.__verify(
            cached=cached,
            error=error,
            hasher=md5,
            hash_kwargs={"start": self._meta.toc_pos, "eigen": _FILE_MD5_EIGEN},
            expected=self._meta.file_md5,
            cache_name=NAME,
        )

    def verify_header(self, cached: bool = True, error: bool = False) -> bool:
        logger.debug(f"Verifying `{self._meta.name}` Header MD5")
        NAME = "__verified_header"
        return self.__verify(
            cached=cached,
            error=error,
            hasher=md5,
            hash_kwargs={
                "start": self._meta.toc_pos,
                "size": self._meta.toc_size,
                "eigen": _TOC_MD5_EIGEN,
            },
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

    @property
    def has_file_data_header(self) -> bool:
        return self._has_file_data_headers

    @property
    def has_safe_file_data_header(self) -> bool:
        return self._has_safe_file_data_headers

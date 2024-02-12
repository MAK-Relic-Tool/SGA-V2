import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Union, Optional

import pytest
from relic.core.errors import RelicToolError
from relic.sga.core import StorageType
from relic.sga.core.serialization import SgaTocHeader

from relic.sga.v2.serialization import (
    RelicUnixTimeSerializer,
    RelicDateTimeSerializer,
    _FILE_MD5_EIGEN,
    _TOC_MD5_EIGEN,
    SgaHeaderV2,
    SgaTocHeaderV2,
    SgaTocDriveV2,
    SgaTocFolderV2,
    SgaTocFileV2Dow,
    SgaTocFileV2ImpCreatures,
    _SgaTocFileV2,
)

_UINT32_M = 4294967295
_UINT16_M = 65535


def _pad_bytes(b: bytes, size: int, pad: bytes = b"\0"):
    return b + (size - len(b)) * pad


# RELIC UNIX TIME = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
_TestRelicUnixTimeSerializerData = [
    (float(0x54165D23) + 0.000001, b"#]\x16T"),
    (float(0x54165D23) + 0.999999, b"#]\x16T"),
    (0x54165D23, b"#]\x16T"),
]


@pytest.mark.parametrize(
    [
        "unpacked",
        "packed",
    ],
    _TestRelicUnixTimeSerializerData,
)
class TestRelicUnixTimeSerializer:
    def test_pack(self, unpacked: Union[float, int], packed: bytes):
        expected = packed
        packed = RelicUnixTimeSerializer.pack(unpacked)
        assert packed == expected

    def test_unpack(self, unpacked: Union[float, int], packed: bytes):
        expected = int(unpacked)
        unpacked = RelicUnixTimeSerializer.unpack(packed)
        assert unpacked == expected


_TestRelicDateTimeSerializer = [
    (
        datetime(2014, 9, 15, 3, 29, 39, 1, tzinfo=timezone.utc),
        b"#]\x16T",
        datetime(2014, 9, 15, 3, 29, 39, tzinfo=timezone.utc),
    ),
    (
        datetime(2014, 9, 15, 3, 29, 39, 999999, tzinfo=timezone.utc),
        b"#]\x16T",
        datetime(2014, 9, 15, 3, 29, 39, tzinfo=timezone.utc),
    ),
    (
        datetime(2014, 9, 15, 3, 29, 39, tzinfo=timezone.utc),
        b"#]\x16T",
        datetime(2014, 9, 15, 3, 29, 39, tzinfo=timezone.utc),
    ),
]


# RELIC DATE TIME = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
@pytest.mark.parametrize(["input", "packed", "unpacked"], _TestRelicDateTimeSerializer)
class TestRelicDateTimeSerializer:
    def test_pack(self, input: datetime, packed: bytes, unpacked: datetime):
        expected = packed
        packed = RelicDateTimeSerializer.pack(input)
        assert packed == expected

    def test_unpack(self, input: datetime, packed: bytes, unpacked: datetime):
        expected = unpacked
        unpacked = RelicDateTimeSerializer.unpack(packed)
        assert unpacked == expected


_TestRelicDateTimeSerializer_Unix2Datetime = [
    (
        float(0x54165D23) + 0.000001,
        datetime(2014, 9, 15, 3, 29, 39, 1, tzinfo=timezone.utc),
    ),
    (
        float(0x54165D23) + 0.999999,
        datetime(2014, 9, 15, 3, 29, 39, 999999, tzinfo=timezone.utc),
    ),
    (0x54165D23, datetime(2014, 9, 15, 3, 29, 39, tzinfo=timezone.utc)),
]


@pytest.mark.parametrize(
    ["value", "output"], _TestRelicDateTimeSerializer_Unix2Datetime
)
def test_relic_datetime_serializer_unix2datetime(value, output):
    result = RelicDateTimeSerializer.unix2datetime(value)
    assert result == output


# MD5 EIGEN = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Useful for ensuring the Eigen values aren't accidentally changed
_TEST_MD5_EIGEN = [
    ("_FILE_MD5_EIGEN", _FILE_MD5_EIGEN, b"E01519D6-2DB7-4640-AF54-0A23319C56C3"),
    ("_TOC_MD5_EIGEN", _TOC_MD5_EIGEN, b"DFC9AF62-FC1B-4180-BC27-11CCE87D3EFF"),
]


@pytest.mark.parametrize(["name", "value", "expected"], _TEST_MD5_EIGEN)
def test_md5_eigen(name: str, value: bytes, expected: bytes):
    assert value == expected


# SGA HEADER = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
def build_sga_header_buffer(
    file_md5: bytes, name: str, header_md5: bytes, header_size: int, data_pos: int
) -> bytes:
    MD5_SIZE = 16
    STR_SIZE = 64
    if len(file_md5) != MD5_SIZE:
        raise ValueError(file_md5, f"({len(file_md5)}), is not {MD5_SIZE} byes!")
    if len(name) > STR_SIZE:
        raise ValueError(name, "is too big!")
    if len(header_md5) != MD5_SIZE:
        raise ValueError(header_md5, f"({len(header_md5)}), is not {MD5_SIZE} byes!")

    parts = [
        file_md5,  # 16
        _pad_bytes(name.encode("utf-16-le"), STR_SIZE * 2),  # 128
        header_md5,  # 16
        header_size.to_bytes(4, "little", signed=False),  # 4
        data_pos.to_bytes(4, "little", signed=False),  # 4
    ]
    return b"".join(parts)


def build_sga_header_empty_buffer():
    MD5_SIZE = 16
    STR_SIZE = 64
    INT_SIZE = 4
    return b"\0" * (MD5_SIZE + STR_SIZE * 2 + MD5_SIZE + INT_SIZE + INT_SIZE)


@dataclass
class SgaHeaderData:
    file_md5: bytes
    name: str
    header_md5: bytes
    header_size: int
    data_pos: int

    header_pos: int = 180
    data_size: None = None

    def build_buffer(self) -> bytes:
        return build_sga_header_buffer(
            self.file_md5, self.name, self.header_md5, self.header_size, self.data_pos
        )


__TEST_SGA_HEADER_DATA = [
    SgaHeaderData(
        b"FILE_MD5\0\0\0\0\0\0\0\0",
        "John Travolta",
        b"HEADER_MD5\0\0\0\0\0\0",
        867,
        5309,
    ),
    SgaHeaderData(_FILE_MD5_EIGEN[0:16], "Joe Pecci", _TOC_MD5_EIGEN[0:16], 0, 12345),
    SgaHeaderData(_FILE_MD5_EIGEN[16:32], "Adam West", _TOC_MD5_EIGEN[16:32], 12345, 0),
]
_TEST_SGA_HEADER_DATA = [(data, data.build_buffer()) for data in __TEST_SGA_HEADER_DATA]


@pytest.mark.parametrize(["data", "buffer"], _TEST_SGA_HEADER_DATA)
class TestSgaHeaderV2:
    def test_read_file_md5(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.file_md5 == data.file_md5

    def test_write_file_md5(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            writer.file_md5 = data.file_md5
            assert writer.file_md5 == data.file_md5

    def test_read_name(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.name == data.name

    def test_write_name(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            writer.name = data.name
            assert writer.name == data.name

    def test_read_header_md5(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.toc_md5 == data.header_md5

    def test_write_header_md5(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            writer.toc_md5 = data.header_md5
            assert writer.toc_md5 == data.header_md5

    def test_read_header_size(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.toc_size == data.header_size

    def test_write_header_size(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            writer.toc_size = data.header_size
            assert writer.toc_size == data.header_size

    def test_read_data_pos(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.data_pos == data.data_pos

    def test_write_data_pos(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            writer.data_pos = data.data_pos
            assert writer.data_pos == data.data_pos

    def test_read_header_pos(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.toc_pos == data.header_pos

    def test_write_header_pos(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            try:
                writer.toc_pos = data.header_pos
                assert False, "Expected an error!"
            except RelicToolError as e:
                assert (
                    e.args[0] == "Header Pos is fixed in SGA v2!"
                )  # TODO, catch a specific error instead of checking msg

    def test_read_data_size(self, data: SgaHeaderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaHeaderV2(stream)
            assert interpreter.data_size == data.data_size

    def test_write_data_size(self, data: SgaHeaderData, buffer: bytes):
        empty = build_sga_header_empty_buffer()
        with BytesIO(empty) as writer_h:
            writer = SgaHeaderV2(writer_h)
            try:
                writer.data_size = data.data_size
                assert False, "Expected an error!"
            except RelicToolError as e:
                assert (
                    e.args[0] == "Data Size is not specified in SGA v2!"
                )  # TODO, catch a specific error instead of checking msg


# SGA TOC HEADER = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
@dataclass
class SgaTocHeaderPointerData:
    offset: int
    count: int

    @property
    def drive_info(self):
        return self.offset, self.count


def build_sga_toc_header_buffer(
    drive: SgaTocHeaderPointerData,
    folder: SgaTocHeaderPointerData,
    file: SgaTocHeaderPointerData,
    name: SgaTocHeaderPointerData,
) -> bytes:
    POS_SIZE = 4
    COUNT_SIZE = 2
    parts = []
    for ptr in [drive, folder, file, name]:
        parts.extend(
            [
                ptr.offset.to_bytes(POS_SIZE, "little", signed=False),  # 4
                ptr.count.to_bytes(COUNT_SIZE, "little", signed=False),  # 2
            ]
        )
    return b"".join(parts)


def build_sga_toc_header_empty_buffer():
    POS_SIZE = 4
    COUNT_SIZE = 2
    PARTS = 4
    return b"\0" * (POS_SIZE + COUNT_SIZE) * PARTS


@dataclass
class SgaTocHeaderData:
    drive: SgaTocHeaderPointerData
    folder: SgaTocHeaderPointerData
    file: SgaTocHeaderPointerData
    name: SgaTocHeaderPointerData

    def build_buffer(self) -> bytes:
        return build_sga_toc_header_buffer(
            self.drive, self.folder, self.file, self.name
        )


__TEST_SGA_TOC_HEADER_DATA = [
    SgaTocHeaderData(
        SgaTocHeaderPointerData(0, 1),
        SgaTocHeaderPointerData(2, 3),
        SgaTocHeaderPointerData(3, 4),
        SgaTocHeaderPointerData(5, 6),
    ),
    SgaTocHeaderData(
        SgaTocHeaderPointerData(10, 1),
        SgaTocHeaderPointerData(20, 3),
        SgaTocHeaderPointerData(30, 4),
        SgaTocHeaderPointerData(50, 6),
    ),
    SgaTocHeaderData(
        SgaTocHeaderPointerData(_UINT32_M, _UINT16_M),
        SgaTocHeaderPointerData(_UINT32_M, _UINT16_M),
        SgaTocHeaderPointerData(_UINT32_M, _UINT16_M),
        SgaTocHeaderPointerData(_UINT32_M, _UINT16_M),
    ),
]
_TEST_SGA_TOC_HEADER_DATA = [
    (data, data.build_buffer()) for data in __TEST_SGA_TOC_HEADER_DATA
]


@pytest.mark.parametrize(["data", "buffer"], _TEST_SGA_TOC_HEADER_DATA)
class TestSgaTocHeaderV2:
    @contextmanager
    def _get_interpreter(self, buffer: Optional[bytes] = None):
        if buffer is None:
            buffer = build_sga_toc_header_empty_buffer()
        with BytesIO(buffer) as stream:
            yield SgaTocHeaderV2(stream)

    def _test_count_write(
        self, pointer: SgaTocHeader.TablePointer, data: SgaTocHeaderPointerData
    ):
        pointer.count = data.count
        assert pointer.count == data.count

    def _test_count_read(
        self, pointer: SgaTocHeader.TablePointer, data: SgaTocHeaderPointerData
    ):
        assert pointer.count == data.count

    def _test_offset_write(
        self, pointer: SgaTocHeader.TablePointer, data: SgaTocHeaderPointerData
    ):
        pointer.offset = data.offset
        assert pointer.offset == data.offset

    def _test_offset_read(
        self, pointer: SgaTocHeader.TablePointer, data: SgaTocHeaderPointerData
    ):
        assert pointer.offset == data.offset

    # DRIVE
    def test_read_drive_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_offset_read(interpreter.drive, data.drive)

    def test_write_drive_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_offset_write(interpreter.drive, data.drive)

    def test_read_drive_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_count_read(interpreter.drive, data.drive)

    def test_write_drive_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_count_write(interpreter.drive, data.drive)

    # FOLDER
    def test_read_folder_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_offset_read(interpreter.folder, data.folder)

    def test_write_folder_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_offset_write(interpreter.folder, data.folder)

    def test_read_folder_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_count_read(interpreter.folder, data.folder)

    def test_write_folder_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_count_write(interpreter.folder, data.folder)

    # FILE
    def test_read_file_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_offset_read(interpreter.file, data.file)

    def test_write_file_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_offset_write(interpreter.file, data.file)

    def test_read_file_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_count_read(interpreter.file, data.file)

    def test_write_file_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_count_write(interpreter.file, data.file)

    # NAME
    def test_read_name_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_offset_read(interpreter.name, data.name)

    def test_write_name_offset(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_offset_write(interpreter.name, data.name)

    def test_read_name_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter(buffer) as interpreter:
            self._test_count_read(interpreter.name, data.name)

    def test_write_name_count(self, data: SgaTocHeaderData, buffer: bytes):
        with self._get_interpreter() as interpreter:
            self._test_count_write(interpreter.name, data.name)


# SGA TOC DRIVE = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
def build_sga_toc_drive_buffer(
    alias: str,
    name: str,
    first_folder: int,
    last_folder: int,
    first_file: int,
    last_file: int,
    root_folder,
) -> bytes:
    STR_SIZE = 64
    STR_ENC = "ascii"
    LE = "little"
    SIGNED = False
    NUM_SIZE = 2

    if len(alias) > STR_SIZE:
        raise ValueError(name, "is too big!")
    if len(name) > STR_SIZE:
        raise ValueError(name, "is too big!")

    def _enc_num(v: int) -> bytes:
        return v.to_bytes(NUM_SIZE, LE, signed=SIGNED)

    parts = [
        _pad_bytes(alias.encode(STR_ENC), STR_SIZE),  # 64
        _pad_bytes(name.encode(STR_ENC), STR_SIZE),  # 64
        _enc_num(first_folder),  # 2
        _enc_num(last_folder),  # 2
        _enc_num(first_file),  # 2
        _enc_num(last_file),  # 2
        _enc_num(root_folder),  # 2
    ]
    return b"".join(parts)


def build_sga_toc_drive_empty_buffer():
    STR_SIZE = 64
    INT_SIZE = 2
    INT_COUNT = 5
    STR_COUNT = 2
    return b"\0" * (STR_SIZE * STR_COUNT + INT_COUNT * INT_SIZE)


@dataclass
class SgaTocDriveData:
    alias: str
    name: str
    first_folder: int
    last_folder: int
    first_file: int
    last_file: int
    root_folder: int

    def build_buffer(self) -> bytes:
        return build_sga_toc_drive_buffer(
            self.alias,
            self.name,
            self.first_folder,
            self.last_folder,
            self.first_file,
            self.last_file,
            self.root_folder,
        )


__TEST_SGA_TOC_DRIVE_DATA = [
    SgaTocDriveData("data", "Alpha", 0, 1, 2, 3, 0),
    SgaTocDriveData(
        "attrib", "Bravo", _UINT16_M, _UINT16_M, _UINT16_M, _UINT16_M, _UINT16_M
    ),
    SgaTocDriveData("test", "Charlie", 0, 0, 0, 0, 0),
    SgaTocDriveData("", "", 8, 6, 7, 5, 3),
    SgaTocDriveData("", "Echo", 0, 9, 8, 6, 7),
    SgaTocDriveData("foxtrot", "", 5, 3, 0, 9, _UINT16_M),
]
_TEST_SGA_TOC_DRIVE_DATA = [
    (data, data.build_buffer()) for data in __TEST_SGA_TOC_DRIVE_DATA
]


@pytest.mark.parametrize(["data", "buffer"], _TEST_SGA_TOC_DRIVE_DATA)
class TestSgaTocDriveV2:
    def _get_empty(self):
        return build_sga_toc_drive_empty_buffer()

    def test_read_alias(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.alias == data.alias

    def test_write_alias(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.alias = data.alias
            assert writer.alias == data.alias

    def test_read_name(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.name == data.name

    def test_write_name(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.name = data.name
            assert writer.name == data.name

    def test_read_first_folder(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.first_folder == data.first_folder

    def test_write_first_folder(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.first_folder = data.first_folder
            assert writer.first_folder == data.first_folder

    def test_read_last_folder(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.last_folder == data.last_folder

    def test_write_last_folder(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.last_folder = data.last_folder
            assert writer.last_folder == data.last_folder

    def test_read_root_folder(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.root_folder == data.root_folder

    def test_write_root_folder(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.root_folder = data.root_folder
            assert writer.root_folder == data.root_folder

    def test_read_first_file(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.first_file == data.first_file

    def test_write_first_file(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.first_file = data.first_file
            assert writer.first_file == data.first_file

    def test_read_last_file(self, data: SgaTocDriveData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocDriveV2(stream)
            assert interpreter.last_file == data.last_file

    def test_write_last_file(self, data: SgaTocDriveData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocDriveV2(writer_h)
            writer.last_file = data.last_file
            assert writer.last_file == data.last_file


# SGA TOC FOLDER = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
def build_sga_toc_folder_buffer(
    name_offset: int,
    first_folder: int,
    last_folder: int,
    first_file: int,
    last_file: int,
) -> bytes:
    LE = "little"
    SIGNED = False
    NUM_SIZE = 2
    NAME_NUM_SIZE = 4

    def _enc_num(v: int, size: int = NUM_SIZE) -> bytes:
        return v.to_bytes(size, LE, signed=SIGNED)

    parts = [
        _enc_num(name_offset, NAME_NUM_SIZE),  # 4
        _enc_num(first_folder),  # 2
        _enc_num(last_folder),  # 2
        _enc_num(first_file),  # 2
        _enc_num(last_file),  # 2
    ]
    return b"".join(parts)


def build_sga_toc_folder_empty_buffer():
    NAME_OFFSET_SIZE = 4
    INT_SIZE = 2
    INT_COUNT = 4
    return b"\0" * (NAME_OFFSET_SIZE + INT_COUNT * INT_SIZE)


@dataclass
class SgaTocFolderData:
    name_offset: int
    first_folder: int
    last_folder: int
    first_file: int
    last_file: int

    def build_buffer(self) -> bytes:
        return build_sga_toc_folder_buffer(
            self.name_offset,
            self.first_folder,
            self.last_folder,
            self.first_file,
            self.last_file,
        )


__TEST_SGA_TOC_FOLDER_DATA = [
    SgaTocFolderData(0, 1, 2, 3, 0),
    SgaTocFolderData(_UINT16_M, _UINT16_M, _UINT16_M, _UINT16_M, _UINT16_M),
    SgaTocFolderData(0, 0, 0, 0, 0),
    SgaTocFolderData(8, 6, 7, 5, 3),
    SgaTocFolderData(0, 9, 8, 6, 7),
    SgaTocFolderData(_UINT32_M, 5, 3, 0, 9),
]
_TEST_SGA_TOC_FOLDER_DATA = [
    (data, data.build_buffer()) for data in __TEST_SGA_TOC_FOLDER_DATA
]


@pytest.mark.parametrize(["data", "buffer"], _TEST_SGA_TOC_FOLDER_DATA)
class TestSgaTocFolderV2:
    def _get_empty(self):
        return build_sga_toc_folder_empty_buffer()

    def test_read_name_offset(self, data: SgaTocFolderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocFolderV2(stream)
            assert interpreter.name_offset == data.name_offset

    def test_write_name_offset(self, data: SgaTocFolderData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocFolderV2(writer_h)
            writer.alias = data.name_offset
            assert writer.alias == data.name_offset

    def test_read_first_folder(self, data: SgaTocFolderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocFolderV2(stream)
            assert interpreter.first_folder == data.first_folder

    def test_write_first_folder(self, data: SgaTocFolderData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocFolderV2(writer_h)
            writer.first_folder = data.first_folder
            assert writer.first_folder == data.first_folder

    def test_read_last_folder(self, data: SgaTocFolderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocFolderV2(stream)
            assert interpreter.last_folder == data.last_folder

    def test_write_last_folder(self, data: SgaTocFolderData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocFolderV2(writer_h)
            writer.last_folder = data.last_folder
            assert writer.last_folder == data.last_folder

    def test_read_first_file(self, data: SgaTocFolderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocFolderV2(stream)
            assert interpreter.first_file == data.first_file

    def test_write_first_file(self, data: SgaTocFolderData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocFolderV2(writer_h)
            writer.first_file = data.first_file
            assert writer.first_file == data.first_file

    def test_read_last_file(self, data: SgaTocFolderData, buffer: bytes):
        with BytesIO(buffer) as stream:
            interpreter = SgaTocFolderV2(stream)
            assert interpreter.last_file == data.last_file

    def test_write_last_file(self, data: SgaTocFolderData, buffer: bytes):
        empty = self._get_empty()
        with BytesIO(empty) as writer_h:
            writer = SgaTocFolderV2(writer_h)
            writer.last_file = data.last_file
            assert writer.last_file == data.last_file


# SGA TOC FILE = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
def build_sga_toc_file_buffer(
    name_offset: int,
    storage_type: StorageType,
    data_offset: int,
    comp_size: int,
    decomp_size: int,
    *,
    dow_format: bool = True,
) -> bytes:
    _STORAGE_TYPE = {  # Too lazy to do flag shifting properly # TODO do this properly; it just bit me, and it will bite me again later
        StorageType.STORE: 0,
        StorageType.BUFFER_COMPRESS: 32,
        StorageType.STREAM_COMPRESS: 16,
    }
    storage_type_val = _STORAGE_TYPE[storage_type]

    NUM_SIZE = 4
    DOW_ST_SIZE = 4
    IC_ST_SIZE = 1
    LE = "little"
    SIGNED = False

    def _enc_num(v: int, size: int = NUM_SIZE) -> bytes:
        return v.to_bytes(size, LE, signed=SIGNED)

    parts = [
        _enc_num(name_offset),
        _enc_num(storage_type_val, size=IC_ST_SIZE if not dow_format else DOW_ST_SIZE),
        _enc_num(data_offset),
        _enc_num(comp_size),
        _enc_num(decomp_size),
    ]
    return b"".join(parts)


def build_sga_toc_file_empty_buffer(dow_format: bool = True):
    DOW_SIZE = 20
    IC_SIZE = 17
    return b"\0" * (DOW_SIZE if dow_format else IC_SIZE)


@dataclass
class SgaTocFileData:
    name_offset: int
    storage_type: StorageType
    data_offset: int
    compressed_size: int
    decompressed_size: int

    is_dow: bool = True

    def build_buffer(self) -> bytes:
        return build_sga_toc_file_buffer(
            self.name_offset,
            self.storage_type,
            self.data_offset,
            self.compressed_size,
            self.decompressed_size,
            dow_format=self.is_dow,
        )


__TEST_SGA_TOC_FILE_DATA = [
    SgaTocFileData(0, StorageType.STORE, 1, 2, 3, is_dow=True),
    SgaTocFileData(4, StorageType.BUFFER_COMPRESS, 5, 6, 7, is_dow=True),
    SgaTocFileData(
        _UINT32_M,
        StorageType.STREAM_COMPRESS,
        _UINT32_M,
        _UINT32_M,
        _UINT32_M,
        is_dow=True,
    ),
    SgaTocFileData(10, StorageType.STORE, 11, 12, 13, is_dow=False),
    SgaTocFileData(
        _UINT32_M,
        StorageType.BUFFER_COMPRESS,
        _UINT32_M,
        _UINT32_M,
        _UINT32_M,
        is_dow=False,
    ),
    SgaTocFileData(25, StorageType.STREAM_COMPRESS, 83, 63, 27, is_dow=False),
]
_TEST_SGA_TOC_FILE_DATA = [
    (data, data.build_buffer()) for data in __TEST_SGA_TOC_FILE_DATA
]


@pytest.mark.parametrize(["data", "buffer"], _TEST_SGA_TOC_FILE_DATA)
class TestSgaTocFileV2:
    @contextmanager
    def _get_interpreter(
        self, buffer: Optional[bytes] = None, dow_format: bool = True
    ) -> _SgaTocFileV2:
        if buffer is None:
            buffer = build_sga_toc_file_empty_buffer(dow_format)
        with BytesIO(buffer) as stream:
            yield (
                SgaTocFileV2Dow(stream)
                if dow_format
                else SgaTocFileV2ImpCreatures(stream)
            )

    def test_read_name_offset(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(buffer, dow_format=data.is_dow) as interpreter:
            assert interpreter.name_offset == data.name_offset

    def test_write_name_offset(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(dow_format=data.is_dow) as interpreter:
            interpreter.name_offset = data.name_offset
            assert interpreter.name_offset == data.name_offset

    def test_read_storage_type(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(buffer, dow_format=data.is_dow) as interpreter:
            assert interpreter.storage_type == data.storage_type

    def test_write_storage_type(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(dow_format=data.is_dow) as interpreter:
            interpreter.storage_type = data.storage_type
            assert interpreter.storage_type == data.storage_type

    def test_read_data_offset(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(buffer, dow_format=data.is_dow) as interpreter:
            assert interpreter.data_offset == data.data_offset

    def test_write_data_offset(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(dow_format=data.is_dow) as interpreter:
            interpreter.data_offset = data.data_offset
            assert interpreter.data_offset == data.data_offset

    def test_read_compressed_size(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(buffer, dow_format=data.is_dow) as interpreter:
            assert interpreter.compressed_size == data.compressed_size

    def test_write_compressed_size(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(dow_format=data.is_dow) as interpreter:
            interpreter.compressed_size = data.compressed_size
            assert interpreter.compressed_size == data.compressed_size

    def test_read_decompressed_size(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(buffer, dow_format=data.is_dow) as interpreter:
            assert interpreter.decompressed_size == data.decompressed_size

    def test_write_decompressed_size(self, data: SgaTocFileData, buffer: bytes):
        with self._get_interpreter(dow_format=data.is_dow) as interpreter:
            interpreter.decompressed_size = data.decompressed_size
            assert interpreter.decompressed_size == data.decompressed_size

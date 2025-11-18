import datetime
import io
import itertools
import logging
import zlib
from dataclasses import dataclass
from os import PathLike
from pathlib import PureWindowsPath
from typing import BinaryIO, Sequence, Any

from fsspec import AbstractFileSystem
from relic.sga.core import StorageType

from relic.sga.v2._util import _OmniHandle
from relic.sga.v2.native.models import FileEntryV2
from relic.sga.v2.native.parser import NativeParserV2

logger = logging.getLogger(__name__)


class _Node:
    def child(self, name: str) -> "_Directory|_File|None":
        raise NotImplementedError

    def mkdir(self, name: str, exists_ok: bool = False) -> "_Directory":
        raise NotImplementedError

    def info(self, details: bool = False) -> dict[str, Any]:
        raise NotImplementedError

    def rm(self, name: str):
        raise NotImplementedError

    def _close(self):
        raise NotImplementedError


@dataclass(slots=True)
class _Directory(_Node):
    name: str
    sub_folders: dict[str, "_Directory"]
    files: dict[str, "_File"]
    absolute_path: str

    def child(self, name: str) -> "_Directory|_File|None":
        return self.sub_folders.get(name, self.files.get(name))

    def mkdir(self, name: str, exists_ok: bool = False) -> "_Directory":
        node = self.child(name)
        if node is None:
            node = _Directory(
                name, {}, {}, str(PureWindowsPath(self.absolute_path) / name)
            )
            self.sub_folders[name] = node
            return node
        if not isinstance(node, _Directory):
            raise NotImplementedError
        if not exists_ok:
            raise NotImplementedError
        return node

    def info(self, details: bool = False) -> dict[str, Any] | str:
        if details:
            return {"name": self.absolute_path, "size": 0, "type": "directory"}
        else:
            return self.absolute_path

    def rm(self, name: str):
        if name in self.sub_folders:
            folder = self.sub_folders[name]
            folder._close()
            del self.sub_folders[name]

    def _close(self):
        for folder in self.sub_folders.values():
            folder._close()
        for file in self.files.values():
            file._close()


@dataclass(slots=True)
class _File(_Node):
    name: str = None
    modified: datetime.datetime = None
    crc32: int | None = None
    absolute_path: str = None
    _lazy: FileEntryV2 = None
    _mem: bytes = (
        None  # created when written OR after decompressing a compressed storage type; ALWAYS DECOMPRESSED
    )
    storage_type: StorageType = StorageType.STORE  # THE DESIRED STORAGE TYPE

    def info(self, details: bool = False) -> dict[str, Any] | str:
        if details:
            return {
                "name": self.absolute_path,
                "size": (
                    len(self._mem)
                    if self._mem is not None
                    else self._lazy.decompressed_size
                ),
                "type": "file",
                "crc32": self.crc32,
                "modified": self.modified,
                "storage_type": self.storage_type,
            }
        else:
            return self.absolute_path

    def _unlazy(self, data: bytes):
        self._mem = data
        if self._lazy is None:
            return
        self.modified = self._lazy.metadata.modified
        self.crc32 = self._lazy.metadata.crc32
        self.storage_type = self._lazy.storage_type
        self._lazy = None

    def _update_mem(self, data: bytes):
        self.touch()
        self._mem = data
        self.crc32 = None  # invalidate crc32

    def touch(self):
        self.modified = datetime.datetime.now()

    def _close(self):
        pass  # we dont store file handles here *_*


def _read_omni(handle: _OmniHandle, entry: FileEntryV2):
    raw = handle[entry.data_offset : entry.data_offset + entry.compressed_size]
    if entry.storage_type != StorageType.STORE:
        return zlib.decompress(raw)
    return raw


class _WriteFile:
    def __init__(self, fs_file: _File, handle: _OmniHandle):
        self._fs_file = fs_file
        if self._fs_file._lazy is not None:
            with handle as reader:
                raw = _read_omni(reader, self._fs_file._lazy)
                self._fs_file._unlazy(raw)
        else:
            raw = self._fs_file._mem

        self._backing = io.BytesIO(raw)
        self._dirty = False

    def write(self, data: bytes):
        self._dirty = True
        self._backing.write(data)

    def read(self, size: int | None = None) -> bytes:
        return self._backing.read(size)

    def flush(self):
        if self._dirty:
            self._fs_file._update_mem(self._backing.getvalue())
        self._dirty = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self._backing.close()


class _ReadOnlyFile:
    def __init__(self, fs_file: _File, handle: _OmniHandle):
        self._fs_file = fs_file
        self._handle = handle
        self._loc = 0
        lazy = self._fs_file._lazy

        if lazy is not None:  #
            if lazy.storage_type != StorageType.STORE:
                with (
                    self._handle as reader
                ):  # handle will close with context IFF it should
                    mem = self._fs_file._mem = _read_omni(reader, lazy)
                    self._fs_file._lazy = None
                self._handle = io.BytesIO(mem)
            self._fs_file._lazy = None
        elif self._fs_file._mem is not None:
            self._handle.close()
            self._handle = _OmniHandle(self._fs_file._mem)
        else:
            raise NotImplementedError

    def __enter__(self):
        self._handle.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._handle.close()

    def read(self, size: int | None = None):
        if size is None:
            return self._handle[:]
        result = self._handle[self._loc : self._loc + size]
        self._loc += len(result)
        return result


# TODO; according to a line *somewhere* in the fsspec
# paths expect all seperators to be forward slash?
#   BIG if true
#   better than having to know what seperator to use for pyfilesystem


# AbstractArchiveFileSystem seems to be super broken
class SgaV2(AbstractFileSystem):
    protocol = "sga-v2"
    cachable = False

    def __init__(
        self,
        handle: str | PathLike | int | BinaryIO | bytearray | bytes,
        parse: bool = True,
    ):
        super().__init__()
        self._handle = _OmniHandle(handle)
        self._root: _Directory = _Directory("", {}, {}, "")
        self._should_parse = parse
        self._parse()

    def _parse(self):
        if self._should_parse:
            with NativeParserV2(self._handle.safe_handle()) as parser:
                for file in parser.parse():
                    path = file.full_path(include_drive=True)
                    self._mkfile(path, True, file)
                self._meta = parser.get_metadata()

        self._should_parse = False

    @staticmethod
    def _parts(path: str):
        return PureWindowsPath(path.lstrip("\\").lstrip("//")).parts

    @staticmethod
    def _parents(*parts: str):
        return [str(parent) for parent in PureWindowsPath(*parts).parents]

    def _resolve_node(
        self, steps: Sequence[str], resolve_parent: bool = True
    ) -> tuple[_Directory, str]:
        cur_dir = self._root
        child = None
        if len(steps) == 0:
            return cur_dir, ""

        if resolve_parent:
            child = steps[-1]
            steps = steps[:-1]
        for step in steps:
            if cur_dir is None:
                raise FileNotFoundError
            cur_dir = cur_dir.child(step)
        return cur_dir, child

    def _mkdir(self, parts: Sequence[str], create_parents: bool = False) -> _Directory:
        cur_dir = self._root
        if create_parents:
            for step in parts:
                cur_dir = cur_dir.mkdir(step, exists_ok=True)
            return cur_dir
        else:
            cur_dir, new_dir = self._resolve_node(parts)
            if cur_dir is None or isinstance(cur_dir, _File):
                raise NotImplementedError
            return cur_dir.mkdir(new_dir, exists_ok=True)

    def mkdir(self, path: str, create_parents: bool = False, **kwargs):
        parts = self._parts(path)
        self._mkdir(
            parts, create_parents
        )  # we delegate to allow changing return type in _mkdir

    def _mkfile(
        self, path: str, create_parents: bool = False, entry: FileEntryV2 | None = None
    ) -> _File:
        steps = self._parts(path)
        parent_steps, child_step = steps[:-1], steps[-1]
        if create_parents:
            parent_dir = self._mkdir(parent_steps, True)
        else:
            parent_dir, _ = self._resolve_node(parent_steps, resolve_parent=False)
        created = _File(
            child_step, None, None, path, entry, b"" if entry is not None else None
        )
        if child_step in parent_dir.files:
            raise NotImplementedError
        parent_dir.files[child_step] = created
        return created

    def touch(self, path: str, **kwargs):
        steps = self._parts(path)
        cur_dir, file_name = self._resolve_node(steps)
        child = cur_dir.child(file_name)
        if child is None:
            cur_dir.files[file_name] = _File(
                file_name, None, None, path, None, b""
            )  # TODO
        elif not isinstance(child, _File):
            raise NotImplementedError
        else:
            child.modified = (
                datetime.datetime.now()
            )  # todo; ensure it matches our logic for unix time

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        is_binary = "b" in mode and not "t" in mode
        is_writing = any(k in mode for k in "wax+")

        """Return raw bytes-mode file-like from the file-system"""
        if is_writing:
            parent, child_name = self._resolve_node(
                self._parts(path), resolve_parent=True
            )
            child = parent.child(child_name)
            if child is None:
                child = parent.files[child_name] = _File(
                    child_name, datetime.datetime.now(), None, path, None, b"", StorageType.STORE
                )
            return _WriteFile(child, self._handle.safe_handle())
        else:
            child, _ = self._resolve_node(self._parts(path), resolve_parent=False)
            if child is None:
                raise FileNotFoundError(path)
            return _ReadOnlyFile(child, self._handle.safe_handle())

    def cp_file(self, path1, path2, **kwargs):

        src_dir, src_filename = self._resolve_node(self._parts(path1))
        dst_dir, dst_filename = self._resolve_node(self._parts(path2))

        if src_dir is dst_dir and src_filename == dst_filename:
            return  # same file

        src_file = src_dir.child(src_filename)
        dst_file = dst_dir.child(dst_filename)

        if src_file is None or not isinstance(src_file, _File):
            raise NotImplementedError

        if dst_file is not None:
            if isinstance(dst_file, _Directory):
                raise NotImplementedError
            elif isinstance(dst_file, _File):
                # TODO, if writing, error
                pass
        else:
            dst_dir.files[dst_filename] = _File(
                dst_filename,
                src_file.modified,
                src_file.crc32,
                path2,
                src_file._lazy,
                src_file._mem,
            )

    def ls(self, path, detail=True, **kwargs):
        node, _ = self._resolve_node(self._parts(path), resolve_parent=False)
        return [
            f.info(details=detail)
            for f in itertools.chain[_Node](node.sub_folders.values(), node.files.values())
        ]

    def info(self, path, **kwargs):
        node, _ = self._resolve_node(self._parts(path), resolve_parent=False)
        if node is None:
            raise FileNotFoundError(path)
        return node.info(details=True)

    def _rm(self, path):
        try:
            parent, child = self._resolve_node(self._parts(path), resolve_parent=True)
        except FileNotFoundError:
            return
        if parent is None:
            # silently fail?
            return
        parent.rm(child)


if __name__ == "__main__":
    fs = SgaV2(
    )
    print(list(fs.walk("/")))
    fs.mkdir("/remote/output", create_parents=True)
    fs.touch("/remote/output/success")  # creates empty file
    _exists = fs.exists("/remote/output/success")
    assert _exists
    assert fs.isfile("/remote/output/success")
    r = fs.cat("/remote/output/success")
    assert r == b"", r  # get content as bytestring
    fs.copy("/remote/output/success", "/remote/output/copy")
    fs.get("/remote/output/success", "./local/output/get")
    fs.put("./local/output/get", "/remote/output/put")
    r = fs.ls("/remote/output", detail=False)
    assert r == [
        "/remote/output/success",
        "/remote/output/copy",
        "/remote/output/put",
    ], r
    print(list(fs.walk("/")))
    fs.rm("/", recursive=True)
    r = fs.ls("/", detail=False)
    assert r == [], r
    print(list(fs.walk("/")))

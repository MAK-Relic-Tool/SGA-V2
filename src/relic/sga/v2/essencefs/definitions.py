from __future__ import annotations

import os
import time
import zlib
from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import PureWindowsPath
from threading import RLock
from typing import (
    BinaryIO,
    List,
    Iterable,
    Optional,
    Mapping,
    Union,
    Dict,
    Any,
    Tuple,
    Collection,
)

import fs.errors
from fs import ResourceType, open_fs
from fs.base import FS
from fs.info import Info
from fs.mode import Mode
from fs.subfs import SubFS
from relic.core.errors import RelicToolError
from relic.core.lazyio import BinaryWindow, read_chunks, chunk_copy, BinaryWrapper
from relic.sga.core.definitions import MAGIC_WORD, StorageType
from relic.sga.core.essencefs import EssenceFS
from relic.sga.core.hashtools import crc32, md5
from relic.sga.core.serialization import (
    SgaNameWindow,
    SgaTocFolder,
    SgaTocDrive,
    VersionSerializer,
)

from relic.sga.v2.arciv.definitions import (
    Arciv,
    TocFolderItem,
    TocHeader,
    TocStorage,
    TocFileItem,
    TocItem,
)
from relic.sga.v2.definitions import version
from relic.sga.v2.serialization import (
    SgaTocFileDataV2Dow,
    SgaTocFileV2Dow,
    RelicDateTimeSerializer,
    SgaFileV2,
    SgaV2GameFormat,
    SgaHeaderV2,
    SgaTocHeaderV2,
    SgaTocFileDataHeaderV2Dow,
    SgaTocDriveV2,
    SgaTocFolderV2,
    SgaTocFileV2ImpCreatures,
    _SgaTocFileV2,
    _FILE_MD5_EIGEN,
    _TOC_MD5_EIGEN,
)

NS_BASIC = "basic"
NS_DETAILS = "details"
NS_ESSENCE = "essence"


def build_ns_basic(name: str, is_dir: bool):
    return {"name": name, "is_dir": is_dir}


def build_ns_details(
    type: ResourceType,
    size: int,
    *,
    accessed: Optional[Union[float, int]] = None,
    created: Optional[Union[float, int]] = None,
    metadata_changed: Optional[Union[float, int]] = None,
    modified: Optional[Union[float, int]] = None,
):
    return {
        "type": int(type),
        "size": size,
        "accessed": accessed,
        "created": created,
        "metadata_changed": metadata_changed,
        "modified": modified,
    }


class SgaPathResolver:
    SEP = "\\"
    INV_SEP = "/"
    ROOT = SEP

    # TODO, move pathing logic to this class
    #   SGA is picky about how to handle files,
    #   and using the base implementations in FS is liable to cause issues
    #   as evidenced by how validatepath doesn't work for makedirs
    #   because it only calls iterparts, which can also fail, I think with mismatched seperators?

    @classmethod
    def build(cls, *path: str, alias: Optional[str] = None):
        full_path = cls.join(*path)
        full_path = cls.fix_case(full_path)
        if alias:
            if len(full_path) == 0:
                full_path = cls.ROOT
            elif full_path[0] != cls.ROOT:
                full_path = cls.ROOT + full_path
            return f"{alias}:{full_path}"
        return full_path

    @classmethod
    def parse(cls, path: str) -> Tuple[Optional[str], str]:
        if ":" in path:
            alias, path = path.split(":", maxsplit=1)
        else:
            alias = None
        return alias, path

    @classmethod
    def fix_seperator(cls, path: str):
        return path.replace(cls.INV_SEP, cls.SEP)

    @classmethod
    def fix_case(cls, path: str):
        return path.lower()

    @classmethod
    def split_parts(cls, path: str, include_root: bool = True) -> List[str]:
        path = cls.fix_seperator(path)
        path = cls.fix_case(path)

        if path == cls.ROOT:  # Handle special case
            if include_root:
                return [cls.ROOT]
            return []

        if len(path) == 0:
            return []

        parts = path.split(cls.SEP)
        if parts[0] == "" and path[0] == cls.SEP:  # captured root
            if include_root:
                parts[0] = cls.ROOT
            else:
                parts = parts[1:]
        return parts

    @classmethod
    def join(cls, *parts: str, add_root: bool = False) -> str:
        parts = (cls.fix_seperator(part) for part in parts)
        result = ""
        for part in parts:
            if (len(part) > 0 and part[0] == cls.SEP) or len(result) == 0:
                result = part
            elif result[-1] != cls.SEP:
                result += cls.SEP + part
            else:
                result += part

        if add_root and (len(result) == 0 or result[0] != cls.ROOT):
            result = cls.ROOT + result
        return result

    @classmethod
    def split(cls, path) -> Tuple[str, str]:
        parts = cls.split_parts(path)
        if len(parts) > 0:
            return cls.join(*parts[:-1]), parts[-1]
        return "", path

    @classmethod
    def strip_root(cls, path) -> str:
        if len(path) > 0 and path[0] == cls.ROOT:
            return path[1:]
        else:
            return path

    @classmethod
    def basename(cls, path) -> str:
        return cls.split(path)[1]

    @classmethod
    def dirname(cls, path) -> str:
        return cls.split(path)[0]


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


class _SgaFsFileV2:
    @property
    def name(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError()

    def getinfo(self, namespaces: Optional[Collection[str]] = None) -> Info:
        raise NotImplementedError()

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        raise NotImplementedError()

    @contextmanager
    def openbin(self, mode: str) -> BinaryIO:
        raise NotImplementedError()

    def verify_crc32(self, error: bool) -> bool:
        raise NotImplementedError()

    def recalculate_crc32(self):
        raise NotImplementedError()

    @property
    def crc32(self) -> int:
        raise NotImplementedError()

    @property
    def storage_type(self) -> StorageType:
        raise NotImplementedError()

    @property
    def modified(self) -> datetime:
        raise NotImplementedError

    def __repr__(self):
        klass_name = _repr_name(self)
        file_name = self.name
        kwarg_keys = "crc32", "storage_type", "modified"
        kwargs = {k: getattr(self, k) for k in kwarg_keys}
        kwarg_line = ", ".join(f"{k}='{v}'" for k, v in kwargs.items())
        if len(kwarg_line) > 0:
            kwarg_line = f" ({kwarg_line})"  # space at start to avoid if below
        return f"<{klass_name} '{file_name}'{kwarg_line}>"


class SgaFsFileV2Lazy(_SgaFsFileV2):
    def __init__(self, info: SgaTocFileV2Dow, data: SgaTocFileDataV2Dow):
        # TODO
        #   we should probably accept a lock argument instead
        #   this will only protect this file from being read/written simultaneously
        #   reading/writing
        self._lock = RLock()

        # Disk (Lazy) Fields
        self._info = info
        self._data_info = data

    @property
    def name(self) -> str:
        with self._lock:
            return self._data_info.name

    def close(self):
        pass

    @property
    def crc32(self) -> int:
        return self._data_info.header.crc32

    @property
    def storage_type(self) -> StorageType:
        return self._info.storage_type

    def getinfo(self, namespaces: Optional[Collection[str]] = None) -> Info:
        if namespaces is None:
            namespaces = []

        info = {NS_BASIC: build_ns_basic(self.name, False)}

        with self._lock:
            if NS_DETAILS in namespaces:
                info[NS_DETAILS] = build_ns_details(
                    ResourceType.file,
                    self._info.decompressed_size,
                    modified=self.modified_unix,
                )
            if NS_ESSENCE in namespaces:
                info[NS_ESSENCE] = {
                    "crc32": self.crc32,
                    "storage_type": self.storage_type,
                }
            return Info(info)

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        raise RelicToolError(
            "Cannot write to a lazy file! Did the folder not convert this to a mem-file?"
        )

    @contextmanager
    def openbin(self, mode: str) -> BinaryIO:
        _mode = Mode(mode)
        if _mode.writing:
            raise RelicToolError(
                "Cannot write to a lazy file! Did the folder not convert this to a mem-file?"
            )

        with self._lock:
            yield self._data_info.data(decompress=True)

    def verify_crc32(self, error: bool) -> bool:
        hasher = crc32(start=0)
        # Locking should be handled by opening file, no need to lock here
        with self.openbin("r") as stream:
            expected = self._data_info.header.crc32
            if error:
                hasher.validate(stream, expected, name=f"File '{self.name}' CRC32")
                return True

            return hasher.check(stream, expected)

    def recalculate_crc32(self):
        raise RelicToolError(
            "Cannot write to a lazy file! Did the folder not convert this to a mem-file?"
        )

    @property
    def modified(self) -> datetime:
        return RelicDateTimeSerializer.unix2datetime(self._data_info.header.modified)

    @property
    def modified_unix(self) -> int:
        return self._data_info.header.modified


class SgaFsFileV2Mem(_SgaFsFileV2):
    def __init__(
        self,
        name: str,
        storage_type: Optional[StorageType] = None,
        data: Optional[Union[bytes, BinaryIO]] = None,
        modified: Optional[datetime] = None,
        crc: Optional[int] = None,
    ):
        self._lock = RLock()

        self._name: str = name
        self._modified: datetime = time.time() if modified is None else modified
        self._storage_type: Optional[StorageType] = (
            storage_type if storage_type is not None else StorageType.STORE
        )

        # Create In-Memory handle
        self._handle = BytesIO()
        self._exposed_handle = BinaryWrapper(self._handle, close_parent=False)
        if data is None:
            pass
        elif isinstance(data, bytes):
            self._handle.write(data)
        else:
            for chunk in read_chunks(data):
                self._handle.write(chunk)

        self._size: int = (
            self._handle.tell()
        )  # Take advantage of ptr being at end of stream

        # crc32 hasher will read from start of stream, no need to seek
        self._crc32: int = crc if crc is not None else crc32(start=0).hash(self._handle)
        self._handle.seek(0)  # Ensure handle points to start of stream, again

    def close(self):
        if self._handle is not None:
            self._handle.close()

    @property
    def name(self) -> str:
        return self._name

    @property
    def crc32(self) -> int:
        return self._crc32

    @property
    def storage_type(self) -> StorageType:
        return self._storage_type

    @property
    def modified(self) -> datetime:
        return self._modified

    @property
    def modified_unix(self) -> float:
        return RelicDateTimeSerializer.datetime2unix(self._modified)

    def getinfo(self, namespaces: Optional[Collection[str]] = None) -> Info:
        info = {NS_BASIC: build_ns_basic(self.name, False)}
        if NS_DETAILS in namespaces:
            info[NS_DETAILS] = build_ns_details(
                ResourceType.file, self._size, modified=self.modified_unix
            )
        if NS_ESSENCE in namespaces:
            info[NS_ESSENCE] = {"crc32": self.crc32, "storage_type": self.storage_type}
        return Info(info)

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        if NS_DETAILS in info:
            self._modified = info[NS_DETAILS]["modified"]

        if NS_ESSENCE in info:
            self._crc32 = info[NS_ESSENCE].get("crc32", self._crc32)
            self._storage_type = info[NS_ESSENCE].get(
                "storage_type", self._storage_type
            )

    @contextmanager
    def openbin(self, mode: str) -> BinaryIO:
        _mode = Mode(mode)
        # TODO, Wrapper for 'mode' protections

        with self._lock:
            yield self._exposed_handle
            self._handle.seek(0)  # reset handle
        if _mode.writing:  # mem-file will recalculate CRC at will
            self.recalculate_crc32()

    def verify_crc32(self, error: bool) -> bool:
        hasher = crc32()
        with self.openbin("r") as stream:
            expected = self._crc32
            if error:
                hasher.validate(stream, expected)
                return True

            return hasher.check(stream, expected)

    def recalculate_crc32(self):
        with self._lock:
            hasher = crc32(start=0)
            with self.openbin("r") as stream:
                self._crc32 = hasher.hash(stream)
            self._handle.seek(0)  # reset handle


class SgaFsFileV2(_SgaFsFileV2):
    def __init__(
        self,
        lazy: Optional[SgaFsFileV2Lazy] = None,
        mem: Optional[SgaFsFileV2Mem] = None,
    ):
        if lazy is not None and mem is not None:
            raise RelicToolError(
                "File trying to be created as both a lazy and in-memory file!"
            )
        if lazy is None and mem is None:
            raise RelicToolError(
                "File trying to be created without specifying lazy/in-memory!"
            )

        self._is_lazy: bool = lazy is not None
        self._backing: _SgaFsFileV2 = lazy or mem  # type: ignore # at least one will not be None

    def close(self):
        return self._backing.close()

    def _unlazy(self):
        if not self._is_lazy:
            return
        self._is_lazy = False
        with self._backing.openbin("r") as data_src:
            self._backing = SgaFsFileV2Mem(
                name=self.name,
                storage_type=self.storage_type,
                data=data_src,
                modified=self.modified,
                crc=self.crc32,
            )

    @property
    def storage_type(self) -> StorageType:
        return self._backing.storage_type

    @property
    def modified(self) -> datetime:
        return self._backing.modified

    @property
    def crc32(self) -> int:
        return self._backing.crc32

    @property
    def name(self) -> str:
        return self._backing.name

    def getinfo(self, namespaces: Optional[Collection[str]] = None) -> Info:
        if namespaces is None:
            namespaces = []
        return self._backing.getinfo(namespaces)

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        self._unlazy()
        self._backing.setinfo(info)

    def openbin(self, mode: str) -> BinaryIO:
        _mode = Mode(mode)
        if _mode.writing:
            self._unlazy()
        # child instances handle context management
        with self._backing.openbin(mode) as stream:
            return stream

    def verify_crc32(self, error: bool) -> bool:
        return self._backing.verify_crc32(error)

    def recalculate_crc32(self):
        self._unlazy()
        self._backing.recalculate_crc32()


class _SgaFsFolderV2:
    @property
    def name(self):
        raise NotImplementedError

    @property
    def basename(self) -> str:
        return SgaPathResolver.basename(self.name)

    def getinfo(self, namespace: Collection[str]) -> Info:
        raise NotImplementedError

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        raise NotImplementedError

    def add_file(self, file: _SgaFsFileV2):
        raise NotImplementedError

    def add_folder(self, folder: _SgaFsFolderV2):
        raise NotImplementedError

    @property
    def folders(self) -> List[_SgaFsFolderV2]:
        raise NotImplementedError

    @property
    def files(self) -> List[_SgaFsFileV2]:
        raise NotImplementedError

    def scandir(self) -> Iterable[str]:
        raise NotImplementedError

    def get_child(self, name: str) -> Optional[Union[_SgaFsFileV2, _SgaFsFolderV2]]:
        raise NotImplementedError

    def remove_child(self, name: str):
        raise NotImplementedError

    def remove_file(self, name: str):
        raise NotImplementedError

    def remove_folder(self, name: str):
        raise NotImplementedError

    def empty(self) -> bool:
        pass

    def __repr__(self):
        klass_name = _repr_name(self)
        folder_name = self.name

        folder_count = len(self.folders)
        file_count = len(self.files)
        kwargs = {"folders": folder_count, "files": file_count}
        kwarg_line = ", ".join(f"{k}='{v}'" for k, v in kwargs.items())
        if len(kwarg_line) > 0:
            kwarg_line = f" ({kwarg_line})"  # space at start to avoid if below
        return f"<{klass_name} '{folder_name}'{kwarg_line}>"


class SgaFsFolderV2Mem(_SgaFsFolderV2):
    def __init__(self, name: str):
        self._name = name
        self._children: Dict[str, Union[_SgaFsFolderV2, _SgaFsFileV2]] = {}
        self._folders: Dict[str, _SgaFsFolderV2] = {}
        self._files: Dict[str, _SgaFsFileV2] = {}

    @property
    def name(self):
        return self._name

    def getinfo(self, namespace: Collection[str]) -> Info:
        return Info({NS_BASIC: build_ns_basic(self._name, True)})

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        raise RelicToolError("SGA Folder's have no settable information!")

    def _add_child(self, name: str, resource: Any, alt_lookup: Dict[str, Any]):
        if name in self._children:
            if name in self._files:
                raise fs.errors.FileExists(name)
            elif name in self._folders:
                raise fs.errors.DirectoryExists(name)
            else:
                raise fs.errors.ResourceError(
                    f"Child '{name}' ({str(resource)}) already exists ({str(alt_lookup[name])})!"
                )
        self._children[name] = resource
        alt_lookup[name] = resource

    def add_file(self, file: _SgaFsFileV2):
        self._add_child(file.name, file, self._files)

    def add_folder(self, folder: _SgaFsFolderV2):
        self._add_child(folder.name, folder, self._folders)

    @property
    def folders(self) -> List[_SgaFsFolderV2]:
        return list(self._folders.values())

    @property
    def files(self) -> List[_SgaFsFileV2]:
        return list(self._files.values())

    def scandir(self) -> Iterable[str]:
        return list(self._children.keys())

    def get_child(self, name: str) -> Optional[Union[_SgaFsFileV2, _SgaFsFolderV2]]:
        return self._children.get(name)

    def remove_file(self, name: str):
        if name not in self._children:
            raise fs.errors.ResourceNotFound(name)
        if name not in self._files:
            raise fs.errors.FileExpected(name)
        self._files[name].close()  # close bytes
        del self._files[name]

    def remove_folder(self, name: str):
        if name not in self._children:
            raise fs.errors.ResourceNotFound(name)
        if name not in self._folders:
            raise fs.errors.DirectoryExpected(name)
        if not self._folders[name].empty():
            raise fs.errors.DirectoryNotEmpty(name)

        del self._folders[name]

    def remove_child(self, name: str):
        if name in self._folders:
            self.remove_folder(name)
        elif name in self._files:
            self.remove_file(name)
        else:
            raise fs.errors.ResourceNotFound(name)


class SgaFsFolderV2Lazy(_SgaFsFolderV2):
    def __init__(
        self,
        info: SgaTocFolder,
        name_window: SgaNameWindow,
        data_window: BinaryWindow,
        all_files: List[SgaFsFileV2],
        all_folders: List[SgaFsFolderV2],
    ):
        self._info = info
        self._name_window = name_window
        self._data_window = data_window
        self._all_files = all_files
        self._all_folders = all_folders
        self._files: Optional[Dict[str, _SgaFsFileV2]] = None
        self._folders: Optional[Dict[str, _SgaFsFolderV2]] = None

    def getinfo(self, namespace: Collection[str]) -> Info:
        return Info({NS_BASIC: build_ns_basic(self.name, True)})

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        pass

    def add_file(self, file: _SgaFsFileV2):
        raise RelicToolError(
            "Cannot add a file to a Lazy Folder! Was this not converted to a Mem-Folder?"
        )

    def add_folder(self, folder: _SgaFsFolderV2):
        raise RelicToolError(
            "Cannot add a folder to a Lazy Folder! Was this not converted to a Mem-Folder?"
        )

    def scandir(self) -> Iterable[str]:
        return [*self._files_lookup.keys(), *self._folder_lookup.keys()]

    def get_child(self, name: str) -> Optional[Union[_SgaFsFileV2, _SgaFsFolderV2]]:
        if name in self._files_lookup:
            return self._files_lookup[name]
        if name in self._folder_lookup:
            return self._folder_lookup[name]
        return None

    @property
    def name(self):
        full_path = self._name_window.get_name(self._info.name_offset)
        return SgaPathResolver.basename(full_path)

    @property
    def _files_lookup(self) -> Dict[str, _SgaFsFileV2]:
        if self._files is None:
            info = self._info
            sub_files = self._all_files[info.first_file : info.last_file]
            self._files = {f.name: f for f in sub_files}
        return self._files

    @property
    def _folder_lookup(self) -> Dict[str, _SgaFsFolderV2]:
        if self._folders is None:
            info = self._info
            sub_folders = self._all_folders[info.first_folder : info.last_folder]
            self._folders = {f.name: f for f in sub_folders}
        return self._folders

    @property
    def files(self) -> List[_SgaFsFileV2]:
        return list(self._files_lookup.values())

    @property
    def folders(self) -> List[_SgaFsFolderV2]:
        return list(self._folder_lookup.values())

    def remove_file(self, name: str):
        raise RelicToolError("Cannot remove a file from a Lazy folder!")

    def remove_folder(self, name: str):
        raise RelicToolError("Cannot remove a folder from a Lazy folder!")

    def remove_child(self, name: str):
        raise RelicToolError("Cannot remove a resource from a Lazy folder!")


class SgaFsFolderV2(_SgaFsFolderV2):
    def __init__(
        self,
        lazy: Optional[SgaFsFolderV2Lazy] = None,
        mem: Optional[SgaFsFolderV2Mem] = None,
    ):
        if lazy is not None and mem is not None:
            raise RelicToolError(
                "Folder trying to be created as both a lazy and in-memory folder!"
            )
        if lazy is None and mem is None:
            raise RelicToolError(
                "Folder trying to be created without specifying lazy/in-memory!"
            )

        self._is_lazy: bool = lazy is not None
        self._backing: _SgaFsFolderV2 = lazy or mem  # type: ignore # at least one will not be None

    def _unlazy(self):
        if not self._is_lazy:
            return
        self._is_lazy = False
        root = SgaFsFolderV2Mem(self._backing.name)
        # Migrate folder structure
        for folder in self._backing.folders:
            root.add_folder(folder)
        for file in self._backing.files:
            root.add_file(file)
        self._backing = root

    def _unlazy_children(self):
        for child in self._backing.files:
            if hasattr(child, "_unlazy"):
                child._unlazy()
        for child in self._backing.folders:
            if hasattr(child, "_unlazy"):
                child._unlazy()
            if hasattr(child, "_unlazy_children"):
                child._unlazy_children()

    def getinfo(self, namespaces: Optional[Collection[str]] = None) -> Info:
        return self._backing.getinfo(namespaces)

    def setinfo(self, info: Mapping[str, Mapping[str, object]]):
        self._unlazy()
        self.setinfo(info)

    @property
    def name(self):
        return self._backing.name

    def add_file(self, file: _SgaFsFileV2):
        self._unlazy()
        return self._backing.add_file(file)

    def add_folder(self, folder: _SgaFsFolderV2):
        self._unlazy()
        return self._backing.add_folder(folder)

    @property
    def folders(self) -> List[_SgaFsFolderV2]:
        return self._backing.folders

    @property
    def files(self) -> List[_SgaFsFileV2]:
        return self._backing.files

    def scandir(self) -> Iterable[str]:
        return self._backing.scandir()

    def get_child(self, part):
        return self._backing.get_child(part)

    def remove_file(self, name: str):
        return self._backing.remove_file(name)

    def remove_folder(self, name):
        return self._backing.remove_folder(name)

    def remove_child(self, name):
        return self._backing.remove_child(name)

    def __repr__(self):
        try:
            klass_name = _repr_name(self)
            folder_name = self.name

            folder_count = len(self.folders)
            file_count = len(self.files)
            kwargs = {
                "folders": folder_count,
                "files": file_count,
                "backing": _repr_name(self._backing),
            }
            kwarg_line = ", ".join(f"{k}='{v}'" for k, v in kwargs.items())
            if len(kwarg_line) > 0:
                kwarg_line = f" ({kwarg_line})"  # space at start to avoid if below
            return f"<{klass_name} '{folder_name}'{kwarg_line}>"
        except Exception as e:
            return f"<! Error getting repr for {self.__class__}, '{e}' !>"


class _SgaFsDriveV2:
    @property
    def name(self):
        raise NotImplementedError

    @property
    def alias(self):
        raise NotImplementedError

    @property
    def root(self) -> SgaFsFolderV2:
        raise NotImplementedError


class SgaFsDriveV2Lazy(_SgaFsDriveV2):
    def __init__(
        self,
        info: SgaTocDrive,
        all_folders: List[SgaFsFolderV2],
    ):
        self._info = info
        self._all_folders = all_folders
        self._root = None

    @property
    def name(self):
        return self._info.name

    @property
    def alias(self):
        return self._info.alias

    @property
    def root(self) -> SgaFsFolderV2:
        if self._root is None:
            self._root = self._all_folders[self._info.root_folder]
        return self._root


class SgaFsDriveV2Mem(_SgaFsDriveV2):
    def __init__(self, name: str, alias: str, root: Optional[SgaFsFolderV2] = None):
        self._name = name
        self._alias = alias
        self._root = root or SgaFsFolderV2(mem=SgaFsFolderV2Mem(""))

    @property
    def name(self):
        return self._name

    @property
    def alias(self):
        return self._alias

    @property
    def root(self) -> SgaFsFolderV2:
        return self._root


class SgaFsDriveV2(_SgaFsDriveV2):
    def __init__(
        self,
        lazy: Optional[SgaFsDriveV2Lazy] = None,
        mem: Optional[SgaFsDriveV2Mem] = None,
    ):
        if lazy is not None and mem is not None:
            raise RelicToolError(
                "Drive trying to be created as both a lazy and in-memory drive!"
            )
        if lazy is None and mem is None:
            raise RelicToolError(
                "Drive trying to be created without specifying lazy/in-memory!"
            )

        self._is_lazy: bool = lazy is not None
        self._backing: _SgaFsDriveV2 = lazy or mem  # type: ignore # at least one will not be None

    def _unlazy(self):
        if not self._is_lazy:
            return
        self._is_lazy = False
        root_folder = self._backing.root
        if hasattr(root_folder, "_unlazy"):
            root_folder._unlazy()
        self._backing = SgaFsDriveV2Mem(
            self._backing.name, self._backing.alias, root_folder
        )

    def _unlazy_children(self):
        if hasattr(self.root, "_unlazy_children"):
            self.root._unlazy_children()

    @property
    def name(self):
        return self._backing.name

    @property
    def alias(self):
        return self._backing.alias

    @property
    def root(self):
        return self._backing.root


class _V2TocDisassembler:
    @dataclass
    class TocInfo:
        drive_count: int
        folder_count: int
        file_count: int
        name_count: int

        drive_block: BinaryIO
        folder_block: BinaryIO
        file_block: BinaryIO
        name_block: BinaryIO
        data_block: BinaryIO

    def __init__(self, game_format: SgaV2GameFormat):
        self.data_block: BytesIO = BytesIO()
        self.name_block: BytesIO = BytesIO()
        self._game_format = game_format
        self.file_block: BytesIO = BytesIO()
        self.drive_block: BytesIO = BytesIO()
        self.folder_block: BytesIO = BytesIO()
        self._folder_count = 0
        self.name_table: Dict[str, int] = {}
        self._file_count = 0
        self._drive_count = 0

    def _write_name_to_table(self, table: Dict[str, int], name: str) -> int:
        name = SgaPathResolver.fix_seperator(name)
        _, name = SgaPathResolver.parse(name)
        name = SgaPathResolver.strip_root(name)
        index = table.get(name)

        if index is None:
            index = table[name] = self.name_block.tell()
            enc_name = name.encode("ascii") + b"\0"
            self.name_block.write(enc_name)

        return index

    def write_name(self, name: str = SgaPathResolver.ROOT) -> int:
        return self._write_name_to_table(self.name_table, name)

    def write_data(
        self,
        name: str,
        modified: Union[int, float, datetime],
        uncompressed: bytes,
        storage_type: StorageType,
        path: Optional[str] = None,
    ) -> Tuple[int, Tuple[int, int]]:
        handle = self.data_block

        window_start = handle.tell()
        window_size = SgaTocFileDataHeaderV2Dow._SIZE

        buffer = b"\0" * window_size
        handle.write(buffer)

        # Write Header
        _header_window = BinaryWindow(handle, window_start, window_size)
        data_header = SgaTocFileDataHeaderV2Dow(_header_window)
        data_header.name = name
        if isinstance(modified, datetime):
            modified = RelicDateTimeSerializer.datetime2unix(modified)
        data_header.modified = modified

        data_header.crc32 = crc32.hash(uncompressed)

        # Write Data
        data_ptr = window_start + window_size
        handle.seek(data_ptr)

        decomp_size = len(uncompressed)

        if storage_type == StorageType.STORE:
            handle.write(uncompressed)
            comp_size = decomp_size
        else:
            compressor = zlib.compressobj(level=9)
            for chunk in read_chunks(uncompressed):
                comp_chunk = compressor.compress(chunk)
                handle.write(comp_chunk)
            handle.write(compressor.flush())
            comp_size = handle.tell() - data_ptr

        result = data_ptr, (decomp_size, comp_size)

        return result

    def write_drive(
        self,
        alias: Optional[str] = None,
        name: Optional[str] = None,
        first_folder: Optional[int] = None,
        last_folder: Optional[int] = None,
        first_file: Optional[int] = None,
        last_file: Optional[int] = None,
        root_folder: Optional[int] = None,
        *,
        window_start: Optional[int] = None,
    ):
        handle = self.drive_block

        window_size = SgaTocDriveV2._SIZE
        if window_start is None:
            handle.seek(0, os.SEEK_END)
            window_start = handle.tell()
            buffer = b"\0" * window_size
            handle.write(buffer)
            self._drive_count += 1

        # Write Drive
        with BinaryWindow(handle, window_start, window_size) as window:
            toc_drive = SgaTocDriveV2(window)
            if name is not None:
                toc_drive.name = name
            if alias is not None:
                toc_drive.alias = alias
            if first_folder is not None:
                toc_drive.first_folder = first_folder
            if last_folder is not None:
                toc_drive.last_folder = last_folder
            if first_file is not None:
                toc_drive.first_file = first_file
            if last_file is not None:
                toc_drive.last_file = last_file
            if root_folder is not None:
                toc_drive.root_folder = root_folder

    def write_file(
        self,
        name_offset: Optional[int] = None,
        storage_type: Optional[StorageType] = None,
        data_offset: Optional[int] = None,
        compressed_size: Optional[int] = None,
        decompressed_size: Optional[int] = None,
        *,
        window_start: Optional[int] = None,
    ) -> int:
        _TOC_FILE_HANDLERS = {
            SgaV2GameFormat.DawnOfWar: SgaTocFileV2Dow,
            SgaV2GameFormat.ImpossibleCreatures: SgaTocFileV2ImpCreatures,
        }

        _TOC_FILE = _TOC_FILE_HANDLERS[self._game_format]
        handle = self.file_block

        window_size = _TOC_FILE._SIZE
        if window_start is None:
            handle.seek(0, os.SEEK_END)
            window_start = handle.tell()
            buffer = b"\0" * window_size
            handle.write(buffer)
            self._file_count += 1

        # Write Drive
        with BinaryWindow(handle, window_start, window_size) as window:
            toc_file: _SgaTocFileV2 = _TOC_FILE(window)
            if name_offset is not None:
                toc_file.name_offset = name_offset
            if storage_type is not None:
                toc_file.storage_type = storage_type
            if data_offset is not None:
                toc_file.data_offset = data_offset
            if compressed_size is not None:
                toc_file.compressed_size = compressed_size
            if decompressed_size is not None:
                toc_file.decompressed_size = decompressed_size

            return window_start

    def write_folder(
        self,
        name_offset: Optional[int] = None,
        first_folder: Optional[int] = None,
        last_folder: Optional[int] = None,
        first_file: Optional[int] = None,
        last_file: Optional[int] = None,
        *,
        window_start: Optional[int] = None,
    ) -> int:
        handle = self.folder_block

        window_size = SgaTocFolderV2._SIZE
        if window_start is None:
            handle.seek(0, os.SEEK_END)
            window_start = handle.tell()
            buffer = b"\0" * window_size
            handle.write(buffer)
            self._folder_count += 1

        # Write Folder
        with BinaryWindow(handle, window_start, window_size) as window:
            toc_folder = SgaTocFolderV2(window)
            if name_offset is not None:
                toc_folder.name_offset = name_offset
            if first_folder is not None:
                toc_folder.first_folder = first_folder
            if last_folder is not None:
                toc_folder.last_folder = last_folder
            if first_file is not None:
                toc_folder.first_file = first_file
            if last_file is not None:
                toc_folder.last_file = last_file

        return window_start

    def disassemble(self):
        raise NotImplementedError

    def _prep_read(self):
        self.drive_block.seek(0)
        self.folder_block.seek(0)
        self.file_block.seek(0)
        self.name_block.seek(0)
        self.data_block.seek(0)

    def close(self):
        self.drive_block.close()
        self.folder_block.close()
        self.file_block.close()
        self.name_block.close()
        self.data_block.close()

    @property
    def folder_count(self) -> int:
        return self._folder_count

    def get_info(self):
        self._prep_read()  # prep blocks for read
        return self.TocInfo(
            self.drive_count,
            self.folder_count,
            self.file_count,
            self.name_count,
            self.drive_block,
            self.folder_block,
            self.file_block,
            self.name_block,
            self.data_block,
        )

    def __enter__(self):
        self.disassemble()
        return self.get_info()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def file_count(self) -> int:
        return self._file_count

    @property
    def drive_count(self) -> int:
        return self._drive_count

    @property
    def name_count(self) -> int:
        return len(self.name_table)


class SgaFsV2TocDisassembler(_V2TocDisassembler):
    """
    Disassembles a SGA Fs into separate in-memory partial ToC blocks, which can be spliced together to form a coherent ToC block.
    """

    def __init__(self, sga: EssenceFSV2, game_format: Optional[SgaV2GameFormat] = None):
        super().__init__(game_format or sga._game_format)
        self.filesystem = sga

    def write_fs_tree_names(self, folder: _SgaFsFolderV2, path: str = None):
        # Writes file names in manner mostly consistent with default SGA archives (file names I believe are written in the order that the .arciv file specifies, because we intermediate with pyfilesystem, we can't 1-1 this)
        #   Additionally; this now doesn't write file names, because file names are ALWAYS at the end of the block
        #       We could write them after writing the file tree; but this wouldn't work with multi-drive sgas

        folders = sorted(
            [sub_folder for sub_folder in folder.folders], key=lambda x: x.name
        )
        # files = sorted([sub_file.name for sub_file in folder.files])

        name = folder.name
        parent_full_path = (
            SgaPathResolver.join(path, name) if path is not None else name
        )
        self.write_name(parent_full_path)

        for folder in folders:
            _alias, full_fold_path = SgaPathResolver.parse(
                SgaPathResolver.join(parent_full_path, folder.name)
            )
            self.write_name(full_fold_path)

        for folder in folders:
            self.write_fs_tree_names(folder, parent_full_path)

        # for file_path in files:
        #     self.write_name(file_path)

    def write_fs_sub_folders(
        self, folder: _SgaFsFolderV2
    ) -> List[Tuple[int, _SgaFsFolderV2]]:
        # Fills the folder buffer with temp folders
        results = []
        for sub_folder in folder.folders:
            sub_folder_wb = self.write_folder()
            pair = (sub_folder_wb, sub_folder)
            results.append(pair)
        return results

    def write_fs_file(
        self, file: _SgaFsFileV2, write_back: Optional[int] = None
    ) -> None:
        name = file.name
        modified = file.modified
        storage_type = file.storage_type

        name_offset = self.write_name(name)

        with file.openbin("r") as h:
            uncompressed_buffer = h.read()

        data_offset, (decomp_size, comp_size) = self.write_data(
            name, modified, uncompressed_buffer, storage_type
        )

        # We dont care abou the resulting wb
        __wb = self.write_file(
            name_offset,
            storage_type,
            data_offset,
            comp_size,
            decomp_size,
            window_start=write_back,
        )

        # return index

    def write_fs_folder(
        self,
        folder: _SgaFsFolderV2,
        path: Optional[str] = None,
        write_back: Optional[int] = None,
    ) -> None:
        name = folder.name
        full_path = SgaPathResolver.join(path, name) if path is not None else name
        # index = self.folder_count
        name_offset = self.write_name(full_path)
        if write_back is None:
            write_back = self.write_folder()

        folder_start = self.folder_count
        sub_folders = self.write_fs_sub_folders(folder)
        folder_end = self.folder_count

        for wb, sub_folder in sub_folders:
            self.write_fs_folder(sub_folder, path=full_path, write_back=wb)

        file_start = self.file_count
        for file in folder.files:
            self.write_fs_file(file)
        file_end = self.file_count

        if file_start == file_end:
            file_start = file_end = 0

        self.write_folder(
            name_offset=name_offset,
            first_folder=folder_start,
            last_folder=folder_end,
            first_file=file_start,
            last_file=file_end,
            window_start=write_back,
        )
        # return index

    def write_fs_drive(self, drive: _SgaFsDriveV2) -> None:
        name = drive.name
        alias = drive.alias

        self.write_fs_tree_names(drive.root)  # Writes file names

        folder_root = folder_start = self.folder_count
        file_start = self.file_count

        folder_root_wb = self.write_folder()

        self.write_fs_folder(
            drive.root,
            path=SgaPathResolver.build(alias=alias),
            write_back=folder_root_wb,
        )

        folder_end = self.folder_count
        file_end = self.file_count

        # index = self.drive_count
        self.write_drive(
            alias, name, folder_start, folder_end, file_start, file_end, folder_root
        )
        # return index

    def _disassemble_fs(self):
        for drive in self.filesystem.drives:
            self.write_fs_drive(drive)

    def disassemble(self):
        return self._disassemble_fs()


class ArcivV2TocDisassembler(_V2TocDisassembler):
    def __init__(
        self,
        filesystem: Optional[FS],
        arciv: Arciv,
        game_format: Optional[SgaV2GameFormat] = None,
        filesystem_root: str = None,
    ):
        super().__init__(game_format or SgaV2GameFormat.DawnOfWar)
        self.filesystem = filesystem
        self.arciv = arciv
        self._root = filesystem_root
        self.name_tables = {}
        self.name_table = None  # to force errors

    @property
    def name_count(self) -> int:
        return sum(len(v) for v in self.name_tables.values())

    def _get_or_make_name_table(self, drive: TocItem):
        key = f"{drive.TOCHeader.Name}-{drive.TOCHeader.Alias}"
        result = self.name_tables.get(key)
        if result is None:
            result = self.name_tables[key] = {}
        return result

    def write_name_in_drive(
        self, drive: TocItem, name: str = SgaPathResolver.ROOT
    ) -> int:
        name_table = self._get_or_make_name_table(drive)
        return self._write_name_to_table(name_table, name.lower())

    def write_name(self, name: str = SgaPathResolver.ROOT) -> int:
        raise RelicToolError(
            f"{ArcivV2TocDisassembler.__name__} should use '{self.write_name_in_drive.__name__}'!"
        )

    def _get_fspath(self, path: str, fs_info: Optional[Tuple[FS, str]]):
        SEPS = [
            ("\\", r"/"),
            (r"/", "\\"),
        ]  # pyfilesystem is such a whiny bitch when it comes to path seperators for osfs; ill eat these words if python doesn't actually handle the inverse seperator; but GDamn, its annoying
        if fs_info is not None:
            filesystem, root = fs_info
            path = path.replace(root, "", 1)
            invalid = filesystem.getmeta().get("invalid_path_chars", "")
            for sep, inv_sep in SEPS:
                if sep in invalid and sep in path:
                    path = path.replace(sep, inv_sep)

            return path
        else:
            return path

    def write_arciv_sub_folders(
        self, folder: TocFolderItem
    ) -> List[Tuple[int, TocFolderItem]]:
        # Fills the folder buffer with temp folders
        results = []
        for sub_folder in folder.Folders:
            sub_folder_wb = self.write_folder()
            pair = (sub_folder_wb, sub_folder)
            results.append(pair)
        return results

    def write_arciv_sub_files(
        self, folder: TocFolderItem
    ) -> list[Tuple[int, TocFileItem]]:
        # Fills the folder buffer with temp folders
        sorted_results = {}
        for file in sorted(folder.Files, key=lambda x: x.File.lower()):
            file_wb = self.write_file()
            sorted_results[id(file)] = file_wb

        results = []
        for file in folder.Files:
            result = sorted_results[id(file)], file
            results.append(result)

        return results

    def write_arciv_file_names(self, folder: TocFolderItem, drive: TocItem):
        for file in sorted(folder.Files, key=lambda x: x.File.lower()):
            self.write_name_in_drive(drive, file.File)

        for folder in folder.Folders:
            self.write_arciv_file_names(folder, drive)

    def write_arciv_folder_names(
        self, folder: TocFolderItem, drive: TocItem, path: str = None
    ):
        name = folder.FolderInfo.folder
        parent_full_path = (
            SgaPathResolver.join(path, name) if path is not None else name
        )
        self.write_name_in_drive(drive, parent_full_path)

        for sub_folder in sorted(
            folder.Folders, key=lambda x: x.FolderInfo.folder.lower()
        ):
            full_subfolder_path = SgaPathResolver.join(
                parent_full_path, sub_folder.FolderInfo.folder
            )
            self.write_name_in_drive(drive, full_subfolder_path)

        for sub_folder in folder.Folders:
            self.write_arciv_folder_names(sub_folder, drive, parent_full_path)

    def write_arciv_names(self):
        for toc_item in self.arciv.TOCList:
            self.write_arciv_folder_names(toc_item.RootFolder, toc_item)
        for toc_item in self.arciv.TOCList:
            self.write_arciv_file_names(toc_item.RootFolder, toc_item)

    def _get_fs_info(
        self, path: str, namespaces: List[str], fs_info: Optional[Tuple[FS, str]] = None
    ):
        if fs_info is not None:
            filesystem, _ = fs_info
            fs_path = self._get_fspath(path, fs_info)
            return filesystem.getinfo(fs_path, namespaces)
        IS_DIR = os.path.isdir(path)
        _INFO = {NS_BASIC: build_ns_basic(os.path.basename(path), is_dir=IS_DIR)}
        if NS_DETAILS in namespaces:
            stat = os.stat(path)
            if IS_DIR:
                rtype = ResourceType.directory
            elif os.path.isfile(path):
                rtype = ResourceType.file
            else:
                raise NotImplementedError(f"Can't determine rtype of '{path}'")

            _INFO[NS_DETAILS] = build_ns_details(
                rtype,
                stat.st_size,
                accessed=stat.st_atime,
                created=stat.st_ctime,
                modified=stat.st_mtime,
            )
        return Info(_INFO)

    def write_arciv_file(
        self,
        file: TocFileItem,
        drive: TocItem,
        write_back: Optional[int] = None,
        fs_info: Optional[Tuple[FS, str]] = None,
    ) -> None:
        name = file.File.lower()
        fs_path = self._get_fspath(file.Path, fs_info)

        filesystem = fs_info[0] if fs_info is not None else self.filesystem
        if filesystem is None:
            raise RelicToolError(
                "A path was taken that did not setup the source filesystem! Please file a bug report."
            )

        info = self._get_fs_info(fs_path, ["details"], fs_info)
        modified = info.modified
        size = info.size
        if file.Store is None:
            storage_type = SgaFsV2Assembler.resolve_storage_type(
                drive.TOCHeader.Storage, file.Path, size
            )
        else:
            storage_type = file.Store
        name_offset = self.write_name_in_drive(drive, name)

        with filesystem.openbin(fs_path, "r") as h:
            uncompressed_buffer = h.read()

        data_offset, (decomp_size, comp_size) = self.write_data(
            name, modified, uncompressed_buffer, storage_type
        )

        # We dont care about the resulting wb
        __wb = self.write_file(
            name_offset,
            storage_type,
            data_offset,
            comp_size,
            decomp_size,
            window_start=write_back,
        )

        # return index

    def write_arciv_folder(
        self,
        folder: TocFolderItem,
        drive: TocItem,
        path: Optional[str] = None,
        write_back: Optional[int] = None,
        fs_info: Optional[Tuple[FS, str]] = None,
        *,
        root_folder: bool = False,
    ) -> None:
        name = folder.FolderInfo.folder
        full_path = SgaPathResolver.join(path, name) if path is not None else name
        # index = self.folder_count
        name_offset = self.write_name_in_drive(drive, full_path)
        if write_back is None:
            write_back = self.write_folder()

        folder_start = self.folder_count
        sub_folders = self.write_arciv_sub_folders(folder)
        folder_end = self.folder_count

        file_start = self.file_count
        sub_files = self.write_arciv_sub_files(folder)
        file_end = self.file_count

        for wb, sub_folder in sub_folders:
            self.write_arciv_folder(
                sub_folder, drive=drive, path=full_path, write_back=wb, fs_info=fs_info
            )

        for wb, sub_file in sub_files:
            self.write_arciv_file(sub_file, drive=drive, write_back=wb, fs_info=fs_info)
        #
        # if file_start == file_end:
        #     file_start = file_end = 0

        self.write_folder(
            name_offset=name_offset,
            first_folder=folder_start,
            last_folder=folder_end,
            first_file=file_start,
            last_file=file_end,
            window_start=write_back,
        )
        # return index

    def write_arciv_drive(
        self, drive: TocItem, fs_info: Optional[Tuple[FS, str]] = None
    ) -> None:
        name = drive.TOCHeader.Name
        alias = drive.TOCHeader.Alias

        folder_root = folder_start = self.folder_count
        file_start = self.file_count

        folder_root_wb = self.write_folder()

        self.write_arciv_folder(
            drive.RootFolder,
            write_back=folder_root_wb,
            drive=drive,
            fs_info=fs_info,
            root_folder=True,
        )

        folder_end = self.folder_count
        file_end = self.file_count

        # index = self.drive_count
        self.write_drive(
            alias, name, folder_start, folder_end, file_start, file_end, folder_root
        )
        # return index

    def _disassemble_arciv(self):
        self.write_arciv_names()
        for drive in self.arciv.TOCList:
            if self.filesystem:
                self.write_arciv_drive(
                    drive=drive, fs_info=(self.filesystem, self._root)
                )
            else:
                with open_fs(drive.TOCHeader.RootPath) as filesystem:
                    self.write_arciv_drive(
                        drive=drive, fs_info=(filesystem, drive.TOCHeader.RootPath)
                    )

    def disassemble(self):
        return self._disassemble_arciv()


class _SgaV2Serializer:
    ARCHIVE_HEADER_POS = 12
    TOC_HEADER_POS = 180
    TOC_HEADER_SIZE = 24
    TOC_BLOCK_POS = TOC_HEADER_POS + TOC_HEADER_SIZE
    MD5_START = TOC_HEADER_POS

    def __init__(self, handle: BinaryIO, name: str, safe_mode: bool = False):
        self.out = handle
        self.working_handle = (
            BytesIO()
            if safe_mode or not (self.out.writable() and self.out.readable())
            else self.out
        )
        self.archive_name = name

    @abstractmethod
    @contextmanager
    def _disassemble_toc(self) -> _V2TocDisassembler.TocInfo:
        raise NotImplementedError

    def write(self):
        if self.working_handle.tell() != 0:
            raise RelicToolError(
                "Writing an SGA to the middle of a file! If this is intended behaviour; please write to a BinaryWindow or a BytesIO object"
            )

        self.write_magic_version(self.working_handle)  # write version

        if self.working_handle.tell() != self.ARCHIVE_HEADER_POS:
            raise RelicToolError(
                "The Serializer failed to write the the Magic Word and Version!"
            )

        meta_wb = self.write_meta_block(self.working_handle)  # write blank meta

        if self.working_handle.tell() != self.TOC_HEADER_POS:
            raise RelicToolError(
                "The Serializer failed to write the Archive Header (First Pass; writing blanks)!"
            )

        toc_wb = self.write_toc_header(self.working_handle)  # Write blank TOC header

        if self.working_handle.tell() != self.TOC_BLOCK_POS:
            raise RelicToolError(
                "The Serializer failed to write the ToC Header (First Pass; writing blanks)!"
            )

        with self._disassemble_toc() as info:  # INFO contains TOC and Data block, must be completed in this context
            drive_count, folder_count, file_count, name_count = (
                info.drive_count,
                info.folder_count,
                info.file_count,
                info.name_count,
            )
            (
                drive_offset,
                folder_offset,
                file_offset,
                name_offset,
            ), dynamic_toc_size = self.write_toc(
                self.working_handle,
                info.drive_block,
                info.folder_block,
                info.file_block,
                info.name_block,
            )
            toc_size = dynamic_toc_size + self.TOC_HEADER_SIZE

            data_offset = self.working_handle.tell()
            chunk_copy(info.data_block, self.working_handle)

        # Second pass, Fill TOC
        self.working_handle.seek(self.TOC_HEADER_POS)

        self.write_toc_header(
            self.working_handle,
            drive_offset,
            drive_count,
            folder_offset,
            folder_count,
            file_offset,
            file_count,
            name_offset,
            name_count,
            update=True,
            window_start=toc_wb,
        )

        # Third pass, Fill Metadata
        name = self.archive_name
        header_size = toc_size
        file_md5 = md5.hash(
            self.working_handle, start=self.MD5_START, eigen=_FILE_MD5_EIGEN
        )
        header_md5 = md5.hash(
            self.working_handle,
            start=self.MD5_START,
            size=toc_size,
            eigen=_TOC_MD5_EIGEN,
        )

        self.working_handle.seek(self.ARCHIVE_HEADER_POS)

        self.write_meta_block(
            self.working_handle,
            file_md5,
            name,
            header_md5,
            data_offset,
            header_size,
            window_start=meta_wb,
        )

        # Finalize stream: copy to output (unless we were able to write to the output directly)
        if self.out is self.working_handle:
            return
        chunk_copy(self.working_handle, self.out, src_start=0)

    @classmethod
    def write_magic_version(cls, handle: BinaryIO):
        MAGIC_WORD.write(handle)
        VersionSerializer.write(handle, version)

    @classmethod
    def write_meta_block(
        cls,
        handle: BinaryIO,
        file_md5: Optional[bytes] = None,
        name: Optional[str] = None,
        header_md5: Optional[bytes] = None,
        data_pos: Optional[int] = None,
        header_size: Optional[int] = None,
        *,
        window_start: Optional[int] = None,
    ) -> int:
        window_size = SgaHeaderV2._SIZE
        if window_start is None:
            window_start = handle.tell()
            buffer = b"\0" * window_size
            handle.write(buffer)

        with BinaryWindow(handle, window_start, window_size) as window:
            meta_block = SgaHeaderV2(window)
            if file_md5 is not None:
                meta_block.file_md5 = file_md5
            if name is not None:
                meta_block.name = name
            if header_md5 is not None:
                meta_block.toc_md5 = header_md5
            if data_pos is not None:
                meta_block.data_pos = data_pos
            if header_size is not None:
                meta_block.toc_size = header_size
        return window_start

    @classmethod
    def write_toc_header(
        cls,
        handle: BinaryIO,
        drive_pos: Optional[int] = None,
        drive_count: Optional[int] = None,
        folder_pos: Optional[int] = None,
        folder_count: Optional[int] = None,
        file_pos: Optional[int] = None,
        file_count: Optional[int] = None,
        name_pos: Optional[int] = None,
        name_count: Optional[int] = None,
        *,
        update: bool = False,
        window_start: Optional[int] = None,
    ) -> int:
        if window_start is None:
            window_start = handle.tell()
        window_size = SgaTocHeaderV2._SIZE

        if not update:
            buffer = b"\0" * window_size
            handle.write(buffer)

        with BinaryWindow(handle, window_start, window_size) as window:
            toc_header = SgaTocHeaderV2(window)
            areas = [
                toc_header.drive,
                toc_header.folder,
                toc_header.file,
                toc_header.name,
            ]
            values = [
                (drive_pos, drive_count),
                (folder_pos, folder_count),
                (file_pos, file_count),
                (name_pos, name_count),
            ]
            for area, (offset, count) in zip(areas, values):
                if offset is not None:
                    area.offset = offset
                if count is not None:
                    area.count = count

        return window_start

    @classmethod
    def write_toc(
        cls,
        handle: BinaryIO,
        drive_block: BinaryIO,
        folder_block: BinaryIO,
        file_block: BinaryIO,
        name_block: BinaryIO,
    ) -> Tuple[Tuple[int, int, int, int], int]:
        blocks = [drive_block, folder_block, file_block, name_block]
        positions = [-1] * len(blocks)

        toc_start = handle.tell()

        for i, block in enumerate(blocks):
            positions[i] = handle.tell() - cls.TOC_HEADER_POS
            chunk_copy(block, handle)

        toc_end = handle.tell()

        toc_size = toc_end - toc_start

        block_ptrs = (
            positions[0],
            positions[1],
            positions[2],
            positions[3],
        )  # to shutup mypy
        return block_ptrs, toc_size


class SgaFsV2Serializer(_SgaV2Serializer):
    def __init__(
        self,
        sga: EssenceFSV2,
        handle: BinaryIO,
        game_format: Optional[SgaV2GameFormat] = None,
        name: Optional[str] = None,
        safe_mode: bool = False,
    ):
        if name is None and hasattr(handle, "name"):  # Try to use file name
            name, _ = os.path.splitext(os.path.basename(handle.name))
        if name is None:  # Try to use archive name
            name = sga.getmeta(NS_ESSENCE).get("name")
        if name is None:
            raise RelicToolError("Archive Name not specified")

        super().__init__(handle, name, safe_mode)
        self.sga = sga
        self.game = game_format

    @contextmanager
    def _disassemble_toc(self) -> _V2TocDisassembler.TocInfo:
        with SgaFsV2TocDisassembler(self.sga, self.game) as info:
            yield info


class ArcivV2Serializer(_SgaV2Serializer):
    def __init__(
        self,
        arciv: Arciv,
        handle: BinaryIO,
        filesystem: Optional[FS] = None,
        game_format: Optional[SgaV2GameFormat] = None,
        name: Optional[str] = None,
        safe_mode: bool = False,
    ):
        name = name or arciv.ArchiveHeader.ArchiveName

        super().__init__(handle, name, safe_mode)
        self.arciv = arciv
        self.filesystem = filesystem
        self.game = game_format

    @contextmanager
    def _disassemble_toc(self) -> _V2TocDisassembler.TocInfo:
        try:
            if self.filesystem is not None:
                sys_path = self.filesystem.getsyspath("/")
            else:
                sys_path = None
        except:
            raise

        with ArcivV2TocDisassembler(
            self.filesystem,
            arciv=self.arciv,
            game_format=self.game,
            filesystem_root=sys_path,
        ) as info:
            yield info


class PackingScanner:
    class ArchiveHeader:
        name: str

    class TocItem:
        ...

    header: ArchiveHeader
    toc_list: List[TocItem]
    ...


class PackingSettings:
    ...


class SgaFsV2Assembler:
    DEFAULT_STORAGE_TYPE = StorageType.STREAM_COMPRESS  #

    @classmethod
    def resolve_storage_type(
        cls,
        resolvers: List[TocStorage],
        path: str,
        size: int,
        default_storage_type: StorageType = DEFAULT_STORAGE_TYPE,
    ):
        def _check_size(min_size: int, max_size: int, _size: int):
            min_check = min_size == -1 or (0 <= min_size <= _size)
            max_check = max_size == -1 or (max_size >= 0 and max_size >= _size)
            return min_check and max_check

        def _check_wildcard(wildcard: str, path: str):
            # TODO ~ this is a hack; it may work, but we should probably not depend on PathLib (it's platform-dependence has caused problems in the past)
            _p = PureWindowsPath(path)
            return _p.match(wildcard)

        for resolver in resolvers:
            if not _check_size(resolver.MinSize, resolver.MaxSize, size):
                continue

            if not _check_wildcard(resolver.Wildcard, path):
                continue

            if resolver.Storage is None:
                return default_storage_type
            else:
                return resolver.Storage

        return default_storage_type

    @classmethod
    def assemble_file_tree(
        cls, header: TocHeader, file: TocFileItem, path: str = None
    ) -> Iterable[Tuple[str, str, StorageType]]:
        # ALias is not included in the path
        name = file.File  # File is name; confusingly
        full_path = SgaPathResolver.join(path, name) if path is not None else name
        sys_path = file.Path
        size = os.stat(sys_path).st_size
        storage_type = (
            file.Store
            if file.Store is not None
            else cls.resolve_storage_type(header.Storage, full_path, size)
        )
        yield full_path, sys_path, storage_type

    @classmethod
    def assemble_folder_tree(
        cls, header: TocHeader, folder: TocFolderItem, path: str = None
    ) -> Iterable[Tuple[str, str, StorageType]]:
        # ALias is not included in the path
        name = folder.FolderInfo.folder  # folder is name; confusingly
        full_path = SgaPathResolver.join(path, name) if path is not None else name
        for file in folder.Files:
            yield from cls.assemble_file_tree(header, file, full_path)
        for sub_folder in folder.Folders:
            yield from cls.assemble_folder_tree(header, sub_folder, full_path)

    @classmethod
    def assemble(cls, manifest: Arciv) -> Tuple[EssenceFSV2, Iterable[str]]:
        sga = EssenceFSV2(
            game=SgaV2GameFormat.DawnOfWar, name=manifest.ArchiveHeader.ArchiveName
        )  # TODO does IC support modding?
        file_list = []
        for toc in manifest.TOCList:
            with sga.create_drive(toc.TOCHeader.Name, toc.TOCHeader.Alias) as drive:
                for file_path, sys_path, storage_type in cls.assemble_folder_tree(
                    toc.TOCHeader, toc.RootFolder
                ):
                    parent_folder, file_name = SgaPathResolver.split(file_path)
                    sys_file_info = os.stat(sys_path)
                    size = sys_file_info.st_size
                    modified = RelicDateTimeSerializer.unix2datetime(
                        sys_file_info.st_mtime
                    )

                    with drive.makedirs(parent_folder, recreate=True) as parent_folder:
                        with open(sys_path, "rb") as file_src:
                            with parent_folder.openbin(file_name, "w") as file_dst:
                                chunk_copy(file_src, file_dst)
                            info = {
                                NS_DETAILS: build_ns_details(
                                    ResourceType.file, size=size, modified=modified
                                ),
                                NS_ESSENCE: {"storage_type": storage_type},
                            }
                            parent_folder.setinfo(file_name, info)
                            full_file_path = SgaPathResolver.build(
                                file_path, alias=toc.TOCHeader.Alias
                            )
                            file_list.append(full_file_path)
        return sga, file_list


class SgaFsV2Packer:
    @classmethod
    def serialize_sga(
        cls,
        sga: EssenceFSV2,
        handle: BinaryIO,
        name: Optional[str] = None,
        safe_mode: bool = False,
    ) -> None:
        serializer = SgaFsV2Serializer(sga, handle, name=name, safe_mode=safe_mode)
        serializer.write()

    @classmethod
    def serialize_arciv(
        cls,
        arciv: Arciv,
        handle: BinaryIO,
        name: Optional[str] = None,
        safe_mode: bool = False,
    ) -> None:
        serializer = ArcivV2Serializer(
            arciv, handle=handle, name=name, safe_mode=safe_mode
        )
        serializer.write()

    @classmethod
    def assemble(cls, manifest) -> tuple[EssenceFSV2, Iterable[str]]:
        return SgaFsV2Assembler.assemble(manifest)

    @classmethod
    def pack(cls, manifest: Arciv, handle: BinaryIO, safe_mode: bool = False):
        cls.serialize_arciv(manifest, handle, safe_mode=safe_mode)
        # sga, file_list = cls.assemble(manifest)
        # cls.serialize(sga,handle, safe_mode=safe_mode,file_list=file_list)
        # sga.close()


class DriveExistsError(RelicToolError):
    ...


class EssenceSubFsV2(SubFS):
    def __init__(self, parent_fs, path):
        super().__init__(parent_fs, SgaPathResolver.ROOT)  # Give parent a dummy value
        self._alias, self._sub_dir = SgaPathResolver.parse(path)

    def delegate_path(self, path):  # type: (Text) -> Tuple[_F, Text]
        # _path = join(self._sub_dir, relpath(normpath(path)))
        aliased_path = SgaPathResolver.build(self._sub_dir, path, alias=self._alias)
        return self._wrap_fs, aliased_path


class EssenceFSV2(EssenceFS):
    subfs_class = EssenceSubFsV2

    def __init__(
        self,
        handle: Optional[BinaryIO] = None,
        parse_handle: bool = False,
        game: Optional[SgaV2GameFormat] = None,
        in_memory: bool = False,
        *,
        name: Optional[str] = None,
        verify_header=False,
        verify_file=False,
        editable: bool = True,
    ):
        """
        :param handle: The backing IO object to read/write to. If not present, the archive is automatically treated as an empty in-memory archive.
        :parse_handle: Parses the handle as an SGA file, if false, the archive is treated as an empty in-memory archive.
        :param in_memory: Loads the archive in-memory if the handle is parsed. Does nothing if parse_handle is False.
        :param game: Specifies the game format. Impossible Creatures and Dawn of War use slightly different versions of the V2 specification, this allows the archive to know which version to use if it's ambitious.
        :param verify_header: Validates the Header MD5 when parsing the file; raises a MD5 Hash Mismatch error on failure.
        :param verify_file:Validates the File MD5 when parsing the file; raises a MD5 Hash Mismatch error on failure.
        """
        super().__init__()

        self._stream = handle
        self._file_md5: Optional[bytes] = None
        self._header_md5: Optional[bytes] = None
        self._drives: Dict[str, SgaFsDriveV2] = {}
        self._lazy_file: Optional[SgaFileV2] = None
        self._game_format: Optional[SgaV2GameFormat] = game
        self._name = name
        self._update_stream = editable

        if parse_handle:
            if handle is None:
                raise RelicToolError("Cannot parse a null handle!")

            if self._name is None and hasattr(self._stream, "name"):
                self._name = os.path.basename(self._stream.name)

            self._lazy_file = SgaFileV2(handle, game_format=game)

            if verify_header:
                self._lazy_file.verify_header(error=True)

            if verify_file:
                self._lazy_file.verify_file(error=True)

            self._load_lazy(self._lazy_file)

            self._file_md5 = self._lazy_file.meta.file_md5
            self._header_md5 = self._lazy_file.meta.toc_md5
            self._game_format = self._lazy_file.table_of_contents.game_format

            if in_memory is True:
                self._unlazy()

    def _unlazy(self):
        """
        Converts the filesystem into an in-memory filesystem. Useful for separating the underlying file from the filesystem instance.
        """
        if self._lazy_file is None:
            return  # already in memory

        for drive in self._drives.values():
            drive._unlazy()
            drive._unlazy_children()

        self._lazy_file = None
        self._stream.seek(
            0
        )  # set stream pointer to the start of the file to allow writing to the non-lazy stream

    def load_into_memory(self):
        self._unlazy()

    def save(self, out: Optional[BinaryIO] = None, safe_write: bool = False):
        """
        Saves the FileSystem to the handle provided, if saving in place; the archive will be loaded into memory if it is still lazy
        :param safe_write: Forces the serializer to write to the in-memory stream, before writing to the file. This will protect the file from being written to if the serializer fails midway. This does not protect the file from non-serializer failures (such as OSErrors)
        """

        if self._stream is None and out is None:
            raise RelicToolError("Failed to save, out/handle not specified!")
        if out is None:
            self._unlazy()  # we can't write to a lazily read file, we load the archive into memory; if its in memory this does nothing
            out = self._stream

        SgaFsV2Packer.serialize_sga(self, out, safe_mode=safe_write)

    def getmeta(self, namespace="standard"):  # type: (Text) -> Mapping[Text, object]
        if namespace == NS_ESSENCE:
            return {
                "version": version,
                "name": self._name,
                "file_md5": self._file_md5,
                "header_md5": self._header_md5,
            }

        return super().getmeta(namespace)

    def create_drive(self, name: str, alias: str) -> SubFS:
        drive = SgaFsDriveV2(mem=SgaFsDriveV2Mem(name, alias))
        return self.add_drive(drive)

    def add_drive(self, drive: SgaFsDriveV2) -> SubFS:
        if drive.alias in self._drives:
            raise DriveExistsError(f"Drive Alias '{drive.alias}' already exists!")
        self._drives[drive.alias] = drive
        return self.opendir(SgaPathResolver.build(alias=drive.alias))

    def _load_lazy(self, file: SgaFileV2):
        toc = file.table_of_contents
        name_window = toc.names
        data_window = file.data_block

        files = [
            SgaFsFileV2(
                lazy=SgaFsFileV2Lazy(
                    file,
                    SgaTocFileDataV2Dow(file, name_window, data_window),
                )
            )
            for file in toc.files
        ]
        folders = []
        for folder in toc.folders:
            folders.append(
                SgaFsFolderV2(
                    lazy=SgaFsFolderV2Lazy(
                        folder, name_window, data_window, files, folders
                    )
                )
            )
        drives = [
            SgaFsDriveV2(lazy=SgaFsDriveV2Lazy(drive_info, folders))
            for drive_info in toc.drives
        ]
        for drive in drives:
            self.add_drive(drive)

    @property
    def drives(self) -> List[SgaFsDriveV2]:
        return list(self._drives.values())

    @staticmethod
    def _getnode_from_drive(drive: _SgaFsDriveV2, path: str, exists: bool = False):
        current = drive.root

        # if path == SgaPathResolver.ROOT:
        #     return current

        for part in SgaPathResolver.split_parts(path, include_root=False):
            if current is None:
                raise fs.errors.ResourceNotFound(path)
            if not current.getinfo("basic").get("basic", "is_dir"):
                raise fs.errors.DirectoryExpected(path)
            current = current.get_child(part)

        if exists and current is None:
            raise fs.errors.ResourceNotFound(path)

        return current

    def _getnode(
        self, path: str, exists: bool = False
    ) -> Optional[Union[_SgaFsFileV2, _SgaFsFolderV2]]:
        alias, _path = SgaPathResolver.parse(path)
        if alias is not None:
            if alias not in self._drives:
                raise fs.errors.ResourceNotFound(path)
            return self._getnode_from_drive(self._drives[alias], _path, exists=exists)

        for drive in self.drives:
            try:
                return self._getnode_from_drive(drive, _path, exists=exists)
            except fs.errors.ResourceNotFound:
                continue
        raise fs.errors.ResourceNotFound(path)

    def getinfo(self, path, namespaces=None):
        node = self._getnode(path, exists=True)
        return node.getinfo(namespaces)

    def listdir(self, path):
        node: _SgaFsFolderV2 = self._getnode(path, exists=True)
        if not node.getinfo("basic").get("basic", "is_dir"):
            raise fs.errors.DirectoryExpected(path)
        return node.scandir()

    def _try_enter_parent(self, path: str) -> Tuple[_SgaFsFolderV2, str]:
        alias, _path = SgaPathResolver.parse(path)
        _parent, _child = SgaPathResolver.split(_path)
        parent_path = SgaPathResolver.build(_parent, alias=alias)
        try:
            parent: _SgaFsFolderV2 = self._getnode(parent_path, exists=True)
        except fs.errors.ResourceNotFound as fnf_err:
            fnf_err.path = path  # inject path
            raise

        if not parent.getinfo("basic").get("basic", "is_dir"):
            raise fs.errors.ResourceNotFound(
                path
            )  # Resource not found; we want the child's error, not the dir's error

        return parent, _child

    def makedir(self, path, permissions=None, recreate=False):
        alias, _path = SgaPathResolver.parse(path)
        if alias is not None and _path == SgaPathResolver.ROOT:  # Make Drive
            try:
                self.create_drive("", alias)
            except DriveExistsError as exists_err:
                if not recreate:
                    raise fs.errors.DirectoryExists(path, exists_err)
        else:  # Make Folder
            parent, child_name = self._try_enter_parent(path)

            try:
                parent.add_folder(SgaFsFolderV2Mem(child_name))
            except (
                fs.errors.DirectoryExists
            ) as dir_err:  # Ignore if recreate, otherwise inject path
                if not recreate:
                    dir_err.path = path
                    raise dir_err
            except fs.errors.FileExists as file_err:  # rethrow as a Dir Expected Error
                raise fs.errors.DirectoryExpected(path, file_err)
            except fs.errors.ResourceError as err:  # Inject path into this error
                err.path = path
                raise err

        return self.opendir(path)

    def makedirs(self, path, permissions=None, recreate=False):
        alias, _path = SgaPathResolver.parse(path)
        alias_path = SgaPathResolver.build(alias=alias)

        if alias is not None:
            if recreate:
                current = self.makedir(
                    alias_path, recreate=True
                )  # makedir instead of opendir
            else:
                current = self.opendir(alias_path)
        elif len(self._drives) == 1:
            current = self.opendir(
                SgaPathResolver.build(alias=list(self._drives.keys())[0])
            )
        elif len(self._drives) == 0:
            raise fs.errors.OperationFailed(
                path, msg="Filesystem contains no 'drives' to write to."
            )
        else:
            raise fs.errors.InvalidPath(
                path,
                "An alias must be specified when multiple 'drives' are present in the filesystem.",
            )
        for part in SgaPathResolver.split_parts(_path):
            current = current.makedir(part, permissions, recreate)
        return current

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        parent, child = self._try_enter_parent(path)
        child_node: _SgaFsFileV2 = parent.get_child(child)
        if child_node is None:
            if _mode.create:
                child_node = SgaFsFileV2(mem=SgaFsFileV2Mem(name=child))
                parent.add_file(child_node)
            else:
                raise fs.errors.ResourceNotFound(path)
        elif child_node.getinfo("basic").get("basic", "is_dir"):
            raise fs.errors.FileExpected(path)

        return child_node.openbin(mode)

    def remove(self, path):
        _, path = SgaPathResolver.parse(path)
        if path == SgaPathResolver.ROOT:  # special case; removing root
            raise fs.errors.FileExpected(path)

        parent, child_name = self._try_enter_parent(path)
        try:
            parent.remove_file(child_name)
        except fs.errors.ResourceNotFound as rnf_err:
            rnf_err.path = path
            raise
        except fs.errors.FileExpected as fe_err:
            fe_err.path = path
            raise

    def removedir(self, path):
        _, path = SgaPathResolver.parse(path)
        if path == SgaPathResolver.ROOT:  # special case; removing root
            raise fs.errors.RemoveRootError(path)

        parent, child_name = self._try_enter_parent(path)
        try:
            parent.remove_folder(child_name)
        except fs.errors.ResourceNotFound as rnf_err:
            rnf_err.path = path
            raise
        except fs.errors.DirectoryExpected as de_err:
            de_err.path = path
            raise

    def setinfo(self, path, info):
        node = self._getnode(path, exists=True)
        node.setinfo(info)

    def iterate_fs(self) -> Tuple[str, SubFS[EssenceFSV2]]:
        for alias, _ in self._drives.items():
            yield alias, self.opendir(SgaPathResolver.build(alias=alias))

    def verify_file_crc(self, path: str, error: bool = False) -> bool:
        node: SgaFsFileV2 = self._getnode(path, exists=True)
        if node.getinfo("basic").is_dir:
            raise fs.errors.FileExists(path)
        return node.verify_crc32(error)

    def close(self):  # type: () -> None
        if self._stream is not None:
            self._stream.close()
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._update_stream:
            self.save(safe_write=True)
        self.close()
        return super().__exit__(exc_type, exc_val, exc_tb)

    def scandir(
        self,
        path: str,
        namespaces: Optional[List[str]] = None,
        page: Optional[Tuple[int, int]] = None,
    ) -> Iterator[Info]:
        alias, root = SgaPathResolver.parse(path)
        info = (
            self.getinfo(
                SgaPathResolver.build(root, name, alias=alias), namespaces=namespaces
            )
            for name in self.listdir(path)
        )
        iter_info = iter(info)
        if page is not None:
            start, end = page
            iter_info = itertools.islice(iter_info, start, end)
        return iter_info


# class SgaV2Verifier()

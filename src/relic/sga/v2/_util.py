from __future__ import annotations

import io
import logging
import mmap
import os
import sys
from os import PathLike

from typing import Any, Optional, BinaryIO, TypeAlias, Union

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


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
    return f"<{klass_name} '{name}'{kwarg_line}>"


OmniHandleAccepts: TypeAlias = Union[
    str , PathLike , int , BinaryIO , bytearray , bytes , mmap.mmap, "_OmniHandle"]


class _OmniHandle:
    def __init__(self, handle: OmniHandleAccepts, close_handle: bool = False):
        self._path: str = None
        self._file_descriptor: int = None
        self._close_file_descriptor: bool = False
        self._raw: bytes | bytearray = None
        self._handle: BinaryIO = None
        self._close_handle: bool = False
        self._mmap_handle: mmap.mmap = None  # always close mmap
        self._close_mmap = False
        self._allow_parallel_read: bool = False
        # We dont have an allow_parallel_write, because A) omnihandle is designed for reading and B) i don't think we can EVER parallel write to the same pyobject

        # This is fugly; but it works
        if isinstance(handle, _OmniHandle):
            self._path = handle._path
            self._file_descriptor = handle._file_descriptor
            self._close_file_descriptor = handle._close_file_descriptor
            self._raw = handle._raw
            self._handle = handle._handle
            self._close_handle = handle._close_handle
            self._mmap_handle = handle._mmap_handle
            self._close_mmap = handle._close_mmap
            self._allow_parallel_read = handle._allow_parallel_read
        elif isinstance(handle, str):  # path
            self._path = handle
        elif isinstance(handle, PathLike):
            self._path = handle.__fspath__()
        elif isinstance(handle, int):  # fileno
            self._file_descriptor = handle
            self._close_file_descriptor = close_handle
        elif isinstance(handle, (bytearray, bytes)):  # raw
            self._raw = bytes(handle) # ensure handle's internal data cant change
        elif isinstance(handle, BinaryIO):  # python
            self._handle = handle  # assign handle so we can close
            self._close_handle = close_handle
            try:
                self._file_descriptor = handle.fileno()
            except io.UnsupportedOperation:
                logger.debug(
                    "failed to get file descriptor from BinaryIO, using python's abstraction"
                )
                raise NotImplementedError  # Dont want to worry about this edge case
        elif isinstance(handle, mmap.mmap):
            self._close_mmap = close_handle
            self._mmap_handle = handle
        else:
            raise NotImplementedError(handle.__class__)

        #_path, _file_descriptor, _mmap_handle, _raw all allow parallel reads (because they don't have state)
        # really, only _handle CANT parallel_read, unless we load stream into memory
        self._allow_parallel_read = any(v is not None for v in [self._path, self._file_descriptor, self._mmap_handle, self._raw])

    def safe_handle(self) -> "_OmniHandle":
        if self._path is not None:
            return _OmniHandle(self._path)
        elif self._mmap_handle is not None:
            return _OmniHandle(self._mmap_handle)
        elif self._file_descriptor is not None:
            return _OmniHandle(self._file_descriptor)
        elif self._raw is not None:
            return _OmniHandle(self._raw)
        elif self._handle is not None:  # THE WORST CASE
            self._raw = self._handle.read()
            return _OmniHandle(self._raw)

        raise NotImplementedError

    def open(self):
        if self._path is not None and self._file_descriptor is None:
            self._file_descriptor = os.open(
                self._path, os.O_RDWR | getattr(os, "O_BINARY", 0)
            )
            self._close_file_descriptor = True

        if self._file_descriptor is not None and self._mmap_handle is None:
            self._mmap_handle = mmap.mmap(
                self._file_descriptor, 0
            )  # TODO handle empty mmap failure on windows (its a special case)
            self._close_mmap = True

    def close(self):
        if self._close_file_descriptor and self._file_descriptor is not None:
            logger.debug(f"closing file descriptor '{self._file_descriptor}'")
            os.close(self._file_descriptor)
            self._file_descriptor = None

        if self._close_handle and self._handle is not None:
            self._handle.close()
            self._handle = None

        if self._mmap_handle is not None and self._close_mmap:
            self._mmap_handle.close()
            self._mmap_handle = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, item):
        if self._mmap_handle is not None:
            return self._mmap_handle[item]
        elif self._raw is not None:
            return self._raw[item]
        elif self._handle is not None:
            if isinstance(item, slice):
                start, stop = item.start, item.stop
                if stop < start or start in [-1, None] or stop in [-1]:
                    raise NotImplementedError
                self._handle.seek(start)
                if stop is not None:
                    buffer = self._handle.read(stop - start)
                else:
                    buffer = self._handle.read()
                return buffer[:: item.step]
            else:
                self._handle.seek(item)
                return self._handle.read(1)
        else:
            raise NotImplementedError

    def __setitem__(self, item, value):
        if self._mmap_handle is not None:
            self._mmap_handle[item] = value
        elif self._raw is not None:
            self._raw[item] = value
        elif self._handle is not None:
            raise NotImplementedError
        else:
            raise NotImplementedError

# from __future__ import annotations
#
# import ntpath
# import os
# import pathlib
# import sys
# from dataclasses import dataclass
# from os import PathLike
# from pathlib import PurePath
# from typing import Tuple, Iterable, Optional, List, Union, Generator
#
# import fs.errors
# from fs.base import FS
# from fs.permissions import Permissions
#
#
# @dataclass(frozen=True)
# class _DirEntry:
#     _path: BoundPath
#
#     @property
#     def name(self) -> str:
#         return self._path.name
#
#     @property
#     def path(self) -> str:
#         return str(self._path)
#
#     def inode(self) -> None:
#         raise NotImplementedError
#
#     def is_dir(self, follow_symlinks: bool = True) -> bool:
#         raise NotImplementedError
#
#     def is_file(self, follow_symlinks: bool = True) -> bool:
#         raise NotImplementedError
#
#     def is_symlink(self) -> bool:
#         raise NotImplementedError
#
#     def is_junction(self) -> bool:
#         raise NotImplementedError
#
#     def stat(self, *, follow_symlinks: bool = True) -> object:
#         raise NotImplementedError
#
#
# class SgaFlavour:  # class instead of module
#     sep = "\\"
#     altsep = "/"
#
#     # splitroot = ntpath.splitroot
#     join = ntpath.join
#
#     @classmethod
#     def splitdrive(cls, path: str) -> Tuple[str, str]:
#         parts = path.split(":", 1)
#         if len(parts) == 1:
#             drive = ""
#             tail = path
#         else:
#             drive = parts[0] + ":"
#             tail = parts[1]
#         return drive, tail
#
#     @classmethod
#     def splitroot(cls, path: str) -> Tuple[str, str, str]:
#         drive, tail = cls.splitdrive(path)
#         parts = tail.split(cls.sep, 1)
#         if len(parts[0]) == 0:
#             root = cls.sep
#             tail = parts[1]
#         else:
#             root = ""
#
#         return drive, root, tail
#
#
# class PureSgaPath(PurePath):
#     """PurePath subclass for SGA Archives (V2)."""
#
#     _flavour = SgaFlavour
#     __slots__ = (
#         # The `_raw_paths` slot stores unnormalized string paths. This is set
#         # in the `__init__()` method.
#         "_raw_paths",
#         # The `_drv`, `_root` and `_tail_cached` slots store parsed and
#         # normalized parts of the path. They are set when any of the `drive`,
#         # `root` or `_tail` properties are accessed for the first time. The
#         # three-part division corresponds to the result of
#         # `os.path.splitroot()`, except that the tail is further split on path
#         # separators (i.e. it is a list of strings), and that the root and
#         # tail are normalized.
#         "_drv",
#         "_root",
#         "_tail_cached",
#         # The `_str` slot stores the string representation of the path,
#         # computed from the drive, root and tail when `__str__()` is called
#         # for the first time. It's used to implement `_str_normcase`
#         "_str",
#         # The `_str_normcase_cached` slot stores the string path with
#         # normalized case. It is set when the `_str_normcase` property is
#         # accessed for the first time. It's used to implement `__eq__()`
#         # `__hash__()`, and `_parts_normcase`
#         "_str_normcase_cached",
#         # The `_parts_normcase_cached` slot stores the case-normalized
#         # string path after splitting on path separators. It's set when the
#         # `_parts_normcase` property is accessed for the first time. It's used
#         # to implement comparison methods like `__lt__()`.
#         "_parts_normcase_cached",
#         # The `_lines_cached` slot stores the string path with path separators
#         # and newlines swapped. This is used to implement `match()`.
#         "_lines_cached",
#         # The `_hash` slot stores the hash of the case-normalized string
#         # path. It's set when `__hash__()` is called for the first time.
#         "_hash",
#     )
#
#     def __init__(self, *args: str):
#         super().__init__(*args)
#         self._raw_paths: Iterable[str]
#         self._raw_paths = [_.lower() for _ in self._raw_paths]
#
#     @classmethod
#     def Aliased(cls, *path: str, alias: str | None = None) -> PureSgaPath:
#         if (
#             alias is not None
#             and isinstance(alias, str)
#             and len(alias) > 0
#             and alias[-1] is not ":"
#         ):
#             alias = f"{alias}:"
#         return PureSgaPath(alias, *path) if alias is not None else PureSgaPath(*path)
#
#     @classmethod
#     def _parse_path(cls, path: Optional[str]) -> Tuple[str, str, List[str]]:
#         # We have to copy this
#         # Because it uses class variables
#         # and it's somehow using the nt flavour
#         # I'm sure if i looked harder it'd be obv why
#         # But this is a fast hack that fixes the issue
#         if not path:
#             return "", "", []
#         sep = cls._flavour.sep
#         altsep = cls._flavour.altsep
#         if altsep:
#             path = path.replace(altsep, sep)
#         drv, root, rel = cls._flavour.splitroot(path)
#         parsed = [sys.intern(str(x)) for x in rel.split(sep) if x and x != "."]
#         return drv, root, parsed
#
#     @property
#     def tail(self) -> str:
#         return self._flavour.sep.join(
#             self._tail  # type: ignore # pylint: disable = E1101
#         )
#
#
# class BoundPath(pathlib.Path):
#     def __init__(self, *args: Union[str, PathLike[str]], bind: FS):
#         super().__init__(*args)
#         self._boundfs = bind
#
#     def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
#         ns = "stat" if follow_symlinks else "lstat"
#         info = self._boundfs.getinfo(str(self), [ns])
#         raise NotImplementedError
#
#     def open(self, mode: str = "rb", buffering: int = -1, encoding: Optional[str] = None, errors: Optional[str] = None, newline: str = ""):  # type: ignore
#         return self._boundfs.open(str(self), mode, buffering, encoding, errors, newline)
#
#     def iterdir(self) -> Generator[BoundPath, None, None]:
#         for name in self._boundfs.listdir(str(self)):
#             yield self._make_child_relpath(name)  # type: ignore
#
#     def _scandir(self) -> Iterable[_DirEntry]:
#
#         for name in self._boundfs.listdir(str(self)):
#             yield _DirEntry(self._make_child_relpath(name))  # type: ignore
#
#     def owner(self, *, follow_symlinks: bool = True) -> str:
#         raise NotImplementedError
#
#     def group(self, *, follow_symlinks: bool = True) -> str:
#         raise NotImplementedError
#
#     def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
#         raise NotImplementedError
#
#     def mkdir(
#         self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
#     ) -> None:
#         try:
#             self._boundfs.makedir(str(self), Permissions(mode=mode), exist_ok)
#         except fs.errors.ResourceNotFound:
#             if not parents or self.parent == self:
#                 raise
#             self.parent.mkdir(parents=True, exist_ok=True)
#             self.mkdir(mode, parents=False, exist_ok=exist_ok)
#         except fs.errors.ResourceError:
#             if not exist_ok or not self.is_dir():
#                 raise
#
#     def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
#         raise NotImplementedError
#
#     def unlink(self, missing_ok: bool = False) -> None:
#         try:
#             self._boundfs.remove(str(self))
#         except fs.errors.ResourceNotFound:
#             if not missing_ok:
#                 raise
#
#     def rmdir(self) -> None:
#         self._boundfs.removedir(str(self))
#
#     def rename(self, target: Union[str, PathLike[str]]) -> BoundPath:
#         self._boundfs.move(str(self), str(target), False)
#         return self.with_segments(target)
#
#     def replace(self, target: Union[str, PathLike[str]]) -> BoundPath:
#         self._boundfs.move(str(self), str(target), True)
#         return self.with_segments(target)
#
#     def symlink_to(
#         self,
#         target: Union[str, bytes, PathLike[bytes], PathLike[str]],
#         target_is_directory: bool = False,
#     ) -> None:
#         raise NotImplementedError
#
#     def hardlink_to(
#         self, target: Union[str, bytes, PathLike[bytes], PathLike[str]]
#     ) -> None:
#         raise NotImplementedError
#
#     def with_segments(self, *pathsegments: Union[str, PathLike[str]]) -> BoundPath:
#         return type(self)(*pathsegments, bind=self._boundfs)

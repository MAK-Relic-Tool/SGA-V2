from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from os import PathLike
from typing import Dict, Any, List, Union, Optional

from relic.sga.core.definitions import StorageType
from relic.sga.v2.arciv.errors import ArcivLayoutError
from relic.sga.v2.arciv.writer import _ArcivSpecialEncodable

_module_logger = logging.getLogger(__name__)


@dataclass
class ArchiveHeader:
    """
    The Archive Header *configuration* located in an '.arciv' file
    Contains all valid fields defined in the '.arciv' specification
    """

    ArchiveName: str

    @classmethod
    def default(cls) -> ArchiveHeader:
        return ArchiveHeader("Default Archive Name")


@dataclass
class TocFileItem(_ArcivSpecialEncodable):
    """
    An item in a TocFolderItem, pe
    """

    File: str  # name
    Path: Union[str, PathLike[str]]
    Size: int
    Store: Optional[StorageType]

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TocFileItem:
        _module_logger.debug("Parsing {0} : {1}", cls.__name__, d)
        try:
            storage_value: int = d["Store"]
            if storage_value == -1:
                storage = None
            else:
                storage = StorageType(storage_value)

            kwargs = d.copy()
            kwargs["Store"] = storage

            return cls(**kwargs)
        except KeyError as e:
            raise ArcivLayoutError from e

    def to_parser_dict(self) -> Any:
        _module_logger.debug("Converting {0} to dictionary", self.__class__.__name__)
        obj = dataclasses.asdict(self)
        obj["Store"] = self.Store.value if self.Store is not None else -1
        _module_logger.debug("Converted {0} to {1}", self.__class__.__name__, obj)
        return obj


@dataclass
class TocFolderInfo:
    folder: str  # name
    path: Union[str, PathLike[str]]


@dataclass
class TocFolderItem:
    Files: List[TocFileItem]
    Folders: List[TocFolderItem]
    FolderInfo: TocFolderInfo

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TocFolderItem:
        _module_logger.debug("Parsing {0} : {1}", cls.__name__, d)
        try:
            files = [TocFileItem.from_parser(file) for file in d["Files"]]
            folders = [TocFolderItem.from_parser(folder) for folder in d["Folders"]]
            folder_info = TocFolderInfo(**d["FolderInfo"])

            return cls(Files=files, Folders=folders, FolderInfo=folder_info)
        except KeyError as e:
            raise ArcivLayoutError from e


@dataclass
class TocStorage(_ArcivSpecialEncodable):
    MinSize: int
    MaxSize: int
    Storage: Optional[StorageType]
    Wildcard: str

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TocStorage:
        try:
            storage_value: int = d["Storage"]
            if storage_value == -1:
                storage = None
            else:
                storage = StorageType(storage_value)
            kwargs = d.copy()
            kwargs["Storage"] = storage
            return cls(**kwargs)
        except KeyError as e:
            raise ArcivLayoutError from e

    def to_parser_dict(self) -> Any:
        obj = dataclasses.asdict(self)
        obj["Storage"] = self.Storage.value if self.Storage is not None else -1
        return obj


@dataclass
class TocHeader:
    Alias: str
    Name: str
    RootPath: Union[PathLike[str], str]
    Storage: List[TocStorage]

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TocHeader:
        _module_logger.debug("Parsing {0} : {1}", cls.__name__, d)
        try:
            storage = [TocStorage.from_parser(item) for item in d["Storage"]]
            kwargs = d.copy()
            kwargs["Storage"] = storage
            return cls(**kwargs)
        except KeyError as e:
            raise ArcivLayoutError from e


@dataclass
class TocItem:
    TOCHeader: TocHeader
    RootFolder: TocFolderItem

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TocItem:
        _module_logger.debug("Parsing {0} : {1}", cls.__name__, d)
        try:
            toc_header = TocHeader.from_parser(d["TOCHeader"])
            root_folder = TocFolderItem.from_parser(d["RootFolder"])
            return cls(TOCHeader=toc_header, RootFolder=root_folder)
        except KeyError as e:
            raise ArcivLayoutError from e


@dataclass
class Arciv(_ArcivSpecialEncodable):
    """A class-based approximation of the '.arciv' format."""

    ArchiveHeader: ArchiveHeader
    TOCList: List[TocItem]

    @classmethod
    def default(cls) -> Arciv:
        return Arciv(ArchiveHeader.default(), [])

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> Arciv:
        """Converts a parser result to a formatted."""
        _module_logger.debug("Parsing {0} : {1}", cls.__name__, d)
        try:
            root_dict = d["Archive"]
            header_dict = root_dict["ArchiveHeader"]
            toc_list_dicts = root_dict["TOCList"]

            header = ArchiveHeader(**header_dict)
            toc_list = [
                TocItem.from_parser(toc_item_dict) for toc_item_dict in toc_list_dicts
            ]
            return cls(header, toc_list)
        except KeyError as e:
            raise ArcivLayoutError from e

    def to_parser_dict(self) -> Dict[str, Any]:
        _module_logger.debug("Converting {0} to dictionary", self.__class__.__name__)
        return {"Archive": dataclasses.asdict(self)}

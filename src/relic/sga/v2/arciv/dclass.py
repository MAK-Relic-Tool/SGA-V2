from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from os import PathLike
from typing import Dict, Any, List, Union, Optional

from relic.sga.core.definitions import StorageType

from relic.sga.v2.arciv.writer import _ArcivSpecialEncodable


@dataclass
class ArchiveHeader:
    ArchiveName: str


@dataclass
class TocFileItem(_ArcivSpecialEncodable):
    File: str  # name
    Path: Union[str,PathLike[str]]
    Size: int
    Store: Optional[StorageType]

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> TocFileItem:
        storage_value:int = d["Store"]
        if storage_value == -1:
            storage = None
        else:
            storage = StorageType(storage_value)

        kwargs = d.copy()
        kwargs["Store"] = storage


        return cls(**kwargs)

    def to_parser_dict(self) -> Any:
        obj =dataclasses.asdict(self)
        obj["Store"] = self.Store.value if self.Store is not None else -1
        return obj

@dataclass
class TocFolderInfo:
    folder: str  # name
    path: Union[str,PathLike[str]]



@dataclass
class TocFolderItem:
    Files: List[TocFileItem]
    Folders: List[TocFolderItem]
    FolderInfo: TocFolderInfo

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> TocFolderItem:
        files = [TocFileItem.from_parser(file) for file in d["Files"]]
        folders = [TocFolderItem.from_parser(folder) for folder in d["Folders"]]
        folder_info = TocFolderInfo(**d["FolderInfo"])

        return cls(Files=files,Folders=folders,FolderInfo=folder_info)


@dataclass
class TocStorage(_ArcivSpecialEncodable):
    MinSize: int
    MaxSize: int
    Storage: Optional[StorageType]
    Wildcard: str

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> TocStorage:
        storage_value:int = d["Storage"]
        if storage_value == -1:
            storage = None
        else:
            storage = StorageType(storage_value)
        kwargs = d.copy()
        kwargs["Storage"] = storage
        return cls(**kwargs)

    def to_parser_dict(self) -> Any:
        obj =dataclasses.asdict(self)
        obj["Storage"] = self.Storage.value if self.Storage is not None else -1
        return obj


@dataclass
class TocHeader:
    Alias: str
    Name: str
    RootPath: Union[PathLike[str],str]
    Storage: List[TocStorage]

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> TocHeader:
        storage = [TocStorage.from_parser(item) for item in d["Storage"]]
        kwargs = d.copy()
        kwargs["Storage"] = storage
        return cls(**kwargs)

@dataclass
class TocItem:
    TOCHeader: TocHeader
    RootFolder: TocFolderItem

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> TocItem:
        toc_header = TocHeader.from_parser(d["TOCHeader"])
        root_folder = TocFolderItem.from_parser(d["RootFolder"])
        return cls(TOCHeader=toc_header,RootFolder=root_folder)

@dataclass
class Arciv(_ArcivSpecialEncodable):
    """A class-based approximation of the '.arciv' format."""
    ArchiveHeader: ArchiveHeader
    TOCList: List[TocItem]

    @classmethod
    def from_parser(cls, d:Dict[str,Any]) -> Arciv:
        """Converts a parser result to a formatted """
        
        root_dict = d["Archive"]
        header_dict = root_dict["ArchiveHeader"]
        toc_list_dicts = root_dict["TOCList"]

        header = ArchiveHeader(**header_dict)
        toc_list = [TocItem.from_parser(toc_item_dict) for toc_item_dict in toc_list_dicts]
        return cls(header,toc_list)


    def to_parser_dict(self):
        return {"Archive":dataclasses.asdict(self)}

"""
Archive
TOCStart alias="Data" relativeroot="Data"
FileSettingsStart  defcompression="1"
        Override wildcard=".*$" minsize="-1" maxsize="100" ct="0"
        Override wildcard=".*$" minsize="100" maxsize="4096" ct="2"
        Override wildcard=".*(ttf)|(rgt)$" minsize="-1" maxsize="-1" ct="0"
        Override wildcard=".*(lua)$" minsize="-1" maxsize="-1" ct="2"
FileSettingsEnd
TOCEnd
"""

from __future__ import annotations

import dataclasses
import logging
import re
from dataclasses import dataclass
from os import PathLike
from typing import Dict, Any, List, Union, Optional
from relic.core.logmsg import BraceMessage

from relic.sga.core.definitions import StorageType
from relic.sga.v2.arciv.errors import ArcivLayoutError
from relic.sga.v2.arciv.writer import _ArcivSpecialEncodable

_module_logger = logging.getLogger(__name__)


@dataclass
class _Matchable:
    wildcard: re.Pattern
    min_size: int  # minsize
    max_size: int  # maxsize

    def match(self, path: str, size: int) -> bool:
        min_size, max_size = self.min_size, self.max_size
        if min_size == -1:
            min_size = size
        if max_size == -1:
            max_size = size

        return min_size <= size <= max_size and self.wildcard.match(path) is not None


@dataclass(slots=True)
class Override(_Matchable, _ArcivSpecialEncodable):
    """
    NOTE FROM DOCS
    "Override" entries are evaluated top-down, with the first matching rule taking precedence.
    """

    compression_type: StorageType  # ct

    @classmethod
    def from_parser(cls, d: Dict[str, str]) -> Override:
        _module_logger.debug(BraceMessage("Parsing {0} : {1}", cls.__name__, d))

        return Override(
            re.compile(d["wildcard"]),  # parse to regex?
            int(d["minsize"]),
            int(d["maxsize"]),
            StorageType(int(d["ct"])),
        )


@dataclass(slots=True)
class SkipFile(_Matchable, _ArcivSpecialEncodable):

    @classmethod
    def from_parser(cls, d: Dict[str, str]) -> SkipFile:
        _module_logger.debug(BraceMessage("Parsing {0} : {1}", cls.__name__, d))
        return SkipFile(
            re.compile(d["wildcard"]),  # parse to regex?
            int(d["minsize"]),
            int(d["maxsize"]),
        )


@dataclass(slots=True)
class FileSettings(_ArcivSpecialEncodable):
    default_compression: StorageType  # defcompression
    overrides: List[Override]
    skip_files: List[SkipFile]

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> FileSettings:
        _module_logger.debug(BraceMessage("Parsing {0} : {1}", cls.__name__, d))
        return FileSettings(
            StorageType(int(d["defcompression"])),
            [Override.from_parser(_) for _ in d["override"]],
            [SkipFile.from_parser(_) for _ in d["skip"]],
        )

    def handle(self, file: str, size: int) -> tuple[bool, StorageType | None]:
        if any(skipper.match(file, size) for skipper in self.skip_files):
            return False, None
        for override in self.overrides:
            if override.match(file, size):
                return True, override.compression_type
        return True, self.default_compression


@dataclass(slots=True)
class TableOfContents(_ArcivSpecialEncodable):
    alias: str
    relative_root: str  # relativeroot
    file_settings: FileSettings

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> TableOfContents:
        _module_logger.debug(BraceMessage("Parsing {0} : {1}", cls.__name__, d))
        return TableOfContents(
            d["alias"], d["relativeroot"], FileSettings.from_parser(d["settings"])
        )


@dataclass(slots=True)
class SgaConfig(_ArcivSpecialEncodable):
    """A class-based approximation of the '.sgaconfig' format."""

    tocs: list[TableOfContents]

    @classmethod
    def default(cls) -> SgaConfig:
        return SgaConfig([])

    @classmethod
    def from_parser(cls, d: Dict[str, Any]) -> SgaConfig:
        """Converts a parser result to a SgaConfig object."""
        _module_logger.debug(BraceMessage("Parsing {0} : {1}", cls.__name__, d))
        return SgaConfig([TableOfContents.from_parser(_) for _ in d["tocs"]])

    def to_parser_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

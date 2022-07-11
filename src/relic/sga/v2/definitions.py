"""
Classes & Aliases that Relic's SGA-V2 uses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from typing_extensions import TypeAlias

from relic.sga.core import abstract
from relic.sga.core import Version
from relic.sga.core.serializers import Md5ChecksumHelper


@dataclass
class ArchiveMetadata:
    """
    Metadata for the archive.
    Version 2.0 stores two checksums;
    The File's MD5, used to validate that the archive (whole file) has not changed since creation.
    The Header's MD5, used to validate that the header (folder/file layout) has not changed since creation.
    """

    @property
    def file_md5(self) -> bytes:
        """
        The File's MD5, used to validate that the archive (whole file) has not changed since creation.
        :return: File MD5 hash; 16 bytes long.
        """
        md5: Optional[bytes] = self._file_md5.expected
        if md5 is None:
            raise TypeError("Md5 Checksum was not saved in metadata!")
        return md5

    @property
    def header_md5(self) -> bytes:
        """
        The Header's MD5, used to validate that the header (folder/file layout) has not changed since creation.
        :return: Header MD5 hash; 16 bytes long.
        """
        md5: Optional[bytes] = self._header_md5.expected
        if md5 is None:
            raise TypeError("Md5 Checksum was not saved in metadata!")
        return md5

    _file_md5: Md5ChecksumHelper
    _header_md5: Md5ChecksumHelper


version = Version(2)
Archive: TypeAlias = abstract.Archive[ArchiveMetadata, None]
File: TypeAlias = abstract.File[None]
Folder: TypeAlias = abstract.Folder[None]
Drive: TypeAlias = abstract.Drive[None]

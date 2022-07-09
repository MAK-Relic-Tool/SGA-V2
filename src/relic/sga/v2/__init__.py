"""
Relic's V2.0 Specification for SGA files.

Used in 'Dawn Of War I'.
"""
from relic.sga.v2._core import Archive, Drive, Folder, File, ArchiveMetadata, version
from relic.sga.v2._serializers import archive_serializer as ArchiveIO

__version__ = "1.0.0"

__all__ = [
    "Archive",
    "Drive",
    "Folder",
    "File",
    "ArchiveIO",
    "version",
    "ArchiveMetadata",
]

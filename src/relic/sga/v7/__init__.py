"""
Relic's V7.0 Specification for SGA files.

Used in 'Company Of Heroes 2'.
"""
from relic.sga.v7._serializers import archive_serializer as ArchiveIO
from relic.sga.v7._core import Archive, Drive, Folder, File, ArchiveMetadata, version, FileMetadata

__all__ = [
    "Archive",
    "Drive",
    "Folder",
    "File",
    "ArchiveIO",
    "version",
    "ArchiveMetadata",
    "FileMetadata"
]

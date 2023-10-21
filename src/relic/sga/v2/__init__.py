"""
Relic's V2.0 Specification for SGA files.
"""
from relic.sga.v2.definitions import (
    version,
)

from relic.sga.v2.serialization import essence_fs_serializer as EssenceFSHandler

__version__ = "2.0.0"

__all__ = [
    "EssenceFSHandler",
    "version",
]

"""Relic's V2.0 Specification for SGA files."""

from relic.sga.v2.definitions import (
    version,
)

from relic.sga.v2.essencefs import EssenceFSV2Opener, EssenceFSV2

__version__ = "2.1.0"

__all__ = [
    "EssenceFSV2Opener",
    "EssenceFSV2",
    "version",
]

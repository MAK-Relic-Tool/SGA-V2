"""Relic's V2.0 Specification for SGA files."""

from relic.sga.v2.definitions import (
    version,
)

# what a fool I was; even if it worked; it's not a great idea; parallel cli worth a breaking change for v3
# import relic.sga.v2.pyfilesystem as essencefs  # preserve backwards compatability imports
from relic.sga.v2.pyfilesystem import EssenceFSV2Opener, EssenceFSV2

__version__ = "2.1.0"

__all__ = ["EssenceFSV2Opener", "EssenceFSV2", "version"]

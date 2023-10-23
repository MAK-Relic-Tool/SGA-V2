from typing import BinaryIO

from relic.sga.core.essencesfs import EssenceFS as SgaFS
from relic.sga.core.opener import _FakeSerializer

from relic.sga.v2.definitions import version
from relic.sga.v2.serialization import SgaV2GameFormat
from relic.sga.v2.sgafs import SgaFsV2


class EssenceFSSerializer(_FakeSerializer):
    """
    Serializer to read/write an SGA file to/from a stream from/to a SGA File System
    """

    version = version
    autoclose = False

    def read(self, stream: BinaryIO) -> SgaFS:
        game_format: SgaV2GameFormat = None
        try:
            name = stream.name
            if "Dawn of War" in name:
                game_format = SgaV2GameFormat.DawnOfWar
            elif "Impossible Creatures" in name:
                game_format = SgaV2GameFormat.ImpossibleCreatures
        except:
            pass

        return SgaFsV2(stream, parse_handle=True, game=game_format)

    def write(self, stream: BinaryIO, essence_fs: SgaFS) -> int:
        raise NotImplementedError


essence_fs_serializer = EssenceFSSerializer()
# registry.auto_register(essence_fs_serializer)

__all__ = [
    "essence_fs_serializer",
]

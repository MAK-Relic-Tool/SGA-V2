from typing import BinaryIO, List

from fs.opener.parse import ParseResult
from relic.sga.core import Version
from relic.sga.core.essencefs import EssenceFS as SgaFS

from relic.sga.v2.definitions import version
from relic.sga.v2.serialization import SgaV2GameFormat
from relic.sga.v2.essencefs.definitions import SgaFsV2
from relic.sga.core.essencefs import EssenceFsOpenerPlugin


class EssenceFSV2Opener(EssenceFsOpenerPlugin[SgaFsV2]):
    _PROTO_GENERIC_V2 = "sga-v2"
    _PROTO_DOW = "sga-dow"
    _PROTO_IC = "sga-dow"
    _PROTO2GAME = {
        _PROTO_DOW:SgaV2GameFormat.DawnOfWar,
        _PROTO_IC:SgaV2GameFormat.ImpossibleCreatures
    }
    _PROTOCOLS = [_PROTO_GENERIC_V2, _PROTO_DOW, _PROTO_IC] # we don't include the generic protocl; sga, as that would overwrite it
    _VERSIONS = [version]

    @property
    def protocols(self) -> List[str]:
        return self._PROTOCOLS

    @property
    def versions(self) -> List[Version]:
        return self._VERSIONS

    def __repr__(self) -> str:
        raise NotImplementedError

    def open_fs(
            self,
            fs_url: str,
            parse_result: ParseResult,
            writeable: bool,
            create: bool,
            cwd: str,
    ) -> SgaFsV2:
        game = self._PROTO2GAME.get(parse_result.protocol,None) # Try to make assumptions about game fileae



__all__ = [
    "EssenceFSV2Opener",
]

import os.path
from typing import BinaryIO, List, Optional

from fs.opener.parse import ParseResult
from relic.sga.core.definitions import Version
from relic.sga.core.essencefs.opener import EssenceFsOpenerPlugin

from relic.sga.v2.definitions import version
from relic.sga.v2.pyfilesystem.definitions import EssenceFSV2
from relic.sga.v2.serialization import SgaV2GameFormat


def _guess_format_from_name(name: str) -> Optional[SgaV2GameFormat]:
    if "Dawn of War" in name:
        return SgaV2GameFormat.DawnOfWar
    if "Impossible Creatures" in name:
        return SgaV2GameFormat.ImpossibleCreatures
    return None


class EssenceFSV2Opener(EssenceFsOpenerPlugin[EssenceFSV2]):
    _PROTO_GENERIC_V2 = "sga-v2"
    _PROTO_DOW = "sga-dow"
    _PROTO_IC = "sga-ic"
    _PROTO2GAME = {
        _PROTO_DOW: SgaV2GameFormat.DawnOfWar,
        _PROTO_IC: SgaV2GameFormat.ImpossibleCreatures,
    }
    _PROTOCOLS = [
        _PROTO_GENERIC_V2,
        _PROTO_DOW,
        _PROTO_IC,
    ]  # we don't include the generic protocl; sga, as that would overwrite it
    _VERSIONS = [version]

    @property
    def protocols(self) -> List[str]:
        return self._PROTOCOLS

    @property
    def versions(self) -> List[Version]:
        return self._VERSIONS

    def __repr__(self) -> str:
        raise NotImplementedError

    def open_fs(  # pylint: disable=R0917
        self,
        fs_url: str,
        parse_result: ParseResult,
        writeable: bool,
        create: bool,
        cwd: str = ".",
    ) -> EssenceFSV2:
        game_format: Optional[SgaV2GameFormat] = self._PROTO2GAME.get(
            parse_result.protocol, None
        )  # Try to make assumptions about game file format
        if game_format is None:
            game_format = _guess_format_from_name(parse_result.resource)

        exists = os.path.exists(parse_result.resource)

        # Optimized case; open and parse
        if not exists:
            if not create:
                raise FileNotFoundError(parse_result.resource)
            with open(parse_result.resource, "x") as _:
                pass  # Do nothing; create the blank file

        fmode = "w+b" if writeable else "rb"
        handle: BinaryIO = None  # type: ignore
        try:
            handle: BinaryIO = open(parse_result.resource, fmode)  # type: ignore
            return EssenceFSV2(
                handle, parse_handle=exists, game=game_format, editable=writeable
            )
        except:
            if handle is not None:
                handle.close()
            raise


__all__ = [
    "EssenceFSV2Opener",
]

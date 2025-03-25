import os.path
from typing import BinaryIO, List, Optional

from fs.opener import Opener
from fs.opener.parse import ParseResult
from relic.core.errors import RelicSerializationError, RelicToolError
from relic.sga.core.definitions import Version, MAGIC_WORD
from relic.sga.core.errors import VersionMismatchError
from relic.sga.core.essencefs.opener import EssenceFsOpenerPlugin
from relic.sga.core.serialization import VersionSerializer

from relic.sga.v2.definitions import version as version_sgav2
from relic.sga.v2.essencefs.definitions import EssenceFSV2
from relic.sga.v2.serialization import SgaV2GameFormat


def _guess_format_from_name(name: str) -> Optional[SgaV2GameFormat]:
    if "Dawn of War" in name:
        return SgaV2GameFormat.DawnOfWar
    if "Impossible Creatures" in name:
        return SgaV2GameFormat.ImpossibleCreatures
    return None


class EssenceFSV2Opener(EssenceFsOpenerPlugin[EssenceFSV2], Opener):
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
    _VERSIONS = [version_sgav2]

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

        exists = os.path.exists(parse_result.resource)

        if not exists:
            if not create:
                raise FileNotFoundError(parse_result.resource)
            with open(parse_result.resource, "xb") as _:
                # Write a bare-bones file to ensure that the file can be opened
                # IF the context fails (with open_fs(...) as sga: # do stuff)
                EssenceFSV2(game=game_format).save(_)

        fmode = "w+b" if writeable else "rb"
        handle: BinaryIO = None  # type: ignore
        try:
            handle: BinaryIO = open(parse_result.resource, fmode)  # type: ignore
            MAGIC_WORD.validate(handle, True)
            version: Version
            try:
                version = VersionSerializer.read(handle)
            except RelicToolError as e:
                raise RelicSerializationError from e
            if version != version_sgav2:
                raise VersionMismatchError(received=version, expected=version_sgav2)

            handle.seek(0)
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

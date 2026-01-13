import doctest
import glob
import logging
import re
from io import StringIO
from pathlib import Path
from typing import TextIO, Dict, Any, Optional

from relic.sga.v2.sgaconfig.reader import SgaConfig
from relic.sga.v2.sgaconfig.lexer import build as build_lexer
from relic.sga.v2.sgaconfig.parser import build as build_parser

# from relic.sga.v2.arciv.writer import SgaConfigWriter, SgaConfigWriterSettings, SgaConfigEncoder

from relic.core.logmsg import BraceMessage

logger = logging.getLogger(__name__)


def load(f: TextIO) -> SgaConfig:
    logger.debug(BraceMessage("Loading SgaConfig File: {0}", f))
    data = parse(f)
    return SgaConfig.from_parser(data)


def loads(f: str) -> SgaConfig:
    logger.debug(BraceMessage("Loading SgaConfig string: `{0}`", f))
    data = parses(f)
    return SgaConfig.from_parser(data)


def parse(f: TextIO) -> Dict[str, Any]:
    logger.debug(BraceMessage("Parsing SgaConfig File: `{0}`", f))
    lexer = build_lexer()
    parser = build_parser()
    return parser.parse(f.read(), lexer=lexer)  # type: ignore


def parses(f: str) -> Dict[str, Any]:
    logger.debug(BraceMessage("Parsing SgaConfig string: `{0}`", f))
    with StringIO(f) as h:
        return parse(h)


# def dump(
#     f: TextIO,
#     data: Any,
#     settings: Optional[SgaConfigWriterSettings] = None,
#     encoder: Optional[SgaConfigEncoder] = None,
# ) -> None:
#     logger.debug(BraceMessage("Dumping SgaConfig object `{0}` to `{1}`", data, f))
#     _writer = SgaConfigWriter(settings=settings, encoder=encoder)
#     _writer.writef(f, data)
#
#
# def dumps(
#     data: Any,
#     settings: Optional[SgaConfigWriterSettings] = None,
#     encoder: Optional[SgaConfigEncoder] = None,
# ) -> str:
#     logger.debug(BraceMessage("Dumping SgaConfig object `{0}` to string", data))
#     with StringIO() as h:
#         dump(h, data, settings=settings, encoder=encoder)
#         return h.getvalue()

if __name__ == "__main__":
    d = """Archive
TOCStart alias="Data" relativeroot="Data"
FileSettingsStart  defcompression="1"
	Override wildcard=".*$" minsize="-1" maxsize="100" ct="0"
	Override wildcard=".*$" minsize="100" maxsize="4096" ct="2"	
	Override wildcard=".*(ttf)|(rgt)$" minsize="-1" maxsize="-1" ct="0"
	Override wildcard=".*(lua)$" minsize="-1" maxsize="-1" ct="2" // ignore me
FileSettingsEnd
TOCEnd
"""
    p = parses(d)
    print(p)
    o = loads(d)
    print(o)
    print(
        list(
            Path(
                r"C:\Users\moder\AppData\Roaming\Relic Entertainment\Dawn of War\mods\Test\Mod\Data"
            ).glob(r".*(ttf)|(rgt)$")
        )
    )
    print(
        list(
            glob.glob(
                r"C:\Users\moder\AppData\Roaming\Relic Entertainment\Dawn of War\mods\Test\Mod\Data\.*(ttf)|(rgt)$"
            )
        )
    )

    print(
        list(
            Path(
                r"C:\Users\moder\AppData\Roaming\Relic Entertainment\Dawn of War\mods\Test\Mod\Data"
            ).glob(r".*((ttf)|(rgt))")
        )
    )
    print(
        list(
            glob.glob(
                r"C:\Users\moder\AppData\Roaming\Relic Entertainment\Dawn of War\mods\Test\Mod\Data\.*((ttf)|(rgt))"
            )
        )
    )
    _R = re.compile(r".*((ttf)|(rgt))$")
    for m in """.rgt
.ttf
rgt
ttf
this_rgt_should_be_ignored
this_ttf_should_be_ignored
""".splitlines():
        print(_R.match(m))


__all__ = [
    "parse",
    "parses",
    # "dump",
    # "dumps",
    # "SgaConfigWriter",
    # "SgaConfigWriterSettings",
    "SgaConfig",
]

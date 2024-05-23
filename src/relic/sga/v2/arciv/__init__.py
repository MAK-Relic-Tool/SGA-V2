import logging
from io import StringIO
from typing import TextIO, Dict, Any, Optional

from relic.sga.v2.arciv.definitions import Arciv
from relic.sga.v2.arciv.lexer import build as build_lexer
from relic.sga.v2.arciv.parser import build as build_parser
from relic.sga.v2.arciv.writer import ArcivWriter, ArcivWriterSettings, ArcivEncoder

logger = logging.getLogger(__name__)


def load(f: TextIO) -> Arciv:
    logger.debug(f"Loading Arciv File: {f}")
    data = parse(f)
    return Arciv.from_parser(data)


def loads(f: str) -> Arciv:
    logger.debug(f"Loading Arciv string: `{f}`")
    data = parses(f)
    return Arciv.from_parser(data)


def parse(f: TextIO) -> Dict[str, Any]:
    logger.debug(f"Parsing Arciv File: `{f}`")
    lexer = build_lexer()
    parser = build_parser()
    return parser.parse(f.read(), lexer=lexer)  # type: ignore


def parses(f: str) -> Dict[str, Any]:
    logger.debug(f"Parsing Arciv string: `{f}`")
    with StringIO(f) as h:
        return parse(h)


def dump(
    f: TextIO,
    data: Any,
    settings: Optional[ArcivWriterSettings] = None,
    encoder: Optional[ArcivEncoder] = None,
) -> None:
    logger.debug(f"Dumping Arciv object `{data}` to `{f}`")
    _writer = ArcivWriter(settings=settings, encoder=encoder)
    _writer.writef(f, data)


def dumps(
    data: Any,
    settings: Optional[ArcivWriterSettings] = None,
    encoder: Optional[ArcivEncoder] = None,
) -> str:
    logger.debug(f"Dumping Arciv object `{data}` to string")
    with StringIO() as h:
        dump(h, data, settings=settings, encoder=encoder)
        return h.getvalue()


__all__ = [
    "parse",
    "parses",
    "dump",
    "dumps",
    "ArcivWriter",
    "ArcivWriterSettings",
    "Arciv",
]

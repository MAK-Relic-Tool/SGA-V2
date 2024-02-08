from io import StringIO
from typing import Union, TextIO, Dict, Any, Optional

from relic.sga.v2.arciv.definitions import Arciv
from relic.sga.v2.arciv.lexer import build as build_lexer
from relic.sga.v2.arciv.parser import build as build_parser
from relic.sga.v2.arciv.writer import ArcivWriter, ArcivWriterSettings, ArcivEncoder


def parse(f: TextIO) -> Arciv:
    data = load(f)
    return Arciv.from_parser(data)


def parses(f: str) -> Arciv:
    data = loads(f)
    return Arciv.from_parser(data)


def load(f: TextIO) -> Dict[str, Any]:
    lexer = build_lexer()
    parser = build_parser()
    return parser.parse(f.read(), lexer=lexer)  # type: ignore


def loads(f: str) -> Dict[str, Any]:
    with StringIO(f) as h:
        return load(h)


def dump(
    f: TextIO,
    data: Any,
    settings: Optional[ArcivWriterSettings] = None,
    encoder: Optional[ArcivEncoder] = None,
) -> None:
    _writer = ArcivWriter(settings=settings, encoder=encoder)
    _writer.writef(f, data)


def dumps(
    data: Any,
    settings: Optional[ArcivWriterSettings] = None,
    encoder: Optional[ArcivEncoder] = None,
) -> str:
    with StringIO() as h:
        dump(h, data, settings=settings, encoder=encoder)
        return h.getvalue()


__all__ = [
    "load",
    "loads",
    "dump",
    "dumps",
    "ArcivWriter",
    "ArcivWriterSettings",
    "Arciv",
]

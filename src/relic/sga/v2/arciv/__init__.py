from io import StringIO
from typing import Union, TextIO, Dict, Any, Optional

from relic.sga.v2.arciv.legacy import load as legacy_load, ArcivEncoder
from relic.sga.v2.arciv.lexer import build as build_lexer
from relic.sga.v2.arciv.parser import build as build_parser
from relic.sga.v2.arciv.writer import ArcivWriter, ArcivWriterSettings


def load(f: Union[TextIO, str], *, legacy_mode: bool = False) -> Dict[str, Any]:
    if isinstance(f, str):
        with open(f, "r") as h:
            return load(h)

    if legacy_mode:
        return legacy_load(f)
    else:
        lexer = build_lexer()
        parser = build_parser()
        return parser.parse(f.read(), lexer=lexer)  # type: ignore


def loads(f: str, *, legacy_mode: bool = False) -> Dict[str, Any]:
    with StringIO(f) as h:
        return load(h, legacy_mode=legacy_mode)


def dump(
    f: Union[TextIO, str],
    data: Any,
    settings: Optional[ArcivWriterSettings],
    encoder: Optional[ArcivEncoder],
) -> None:
    _writer = ArcivWriter(settings=settings, encoder=encoder)
    _writer.writef(f, data)


def dumps(
    data: Any, settings: Optional[ArcivWriterSettings], encoder: Optional[ArcivEncoder]
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
    "ArcivEncoder",
]

from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from logging import getLogger
from os import PathLike
from typing import Optional, Iterable, Union, List, Dict, Any, TextIO, Iterator

from relic.core.errors import RelicToolError

logger = getLogger(__name__)


@dataclass
class ArcivWriterSettings:
    indent: Optional[str] = "\t"
    newline: Optional[str] = "\n"
    whitespace: Optional[str] = " "

    @property
    def has_indent(self) -> bool:
        return self.indent is not None and len(self.indent) > 0

    @property
    def has_whitespace(self) -> bool:
        return self.whitespace is not None and len(self.whitespace) > 0

    @property
    def has_newline(self) -> bool:
        return self.newline is not None and len(self.newline) > 0


class ArcivWriterError(RelicToolError): ...


class ArcivEncoderError(RelicToolError): ...


class ArcivWriter:
    def __init__(
        self,
        settings: Optional[ArcivWriterSettings] = None,
        encoder: Optional[ArcivEncoder] = None,
    ):
        self._encoder = encoder or ArcivEncoder()
        self._settings = settings or ArcivWriterSettings()
        self._indent_level = 0

    @contextmanager
    def _enter_indent(self) -> Iterator[None]:
        self._indent_level += 1
        logger.debug(f"Entering Indent `{self._indent_level}`")
        yield None
        self._indent_level -= 1
        logger.debug(f"Exiting Indent `{self._indent_level}`")

    def _formatted(
        self,
        *values: str,
        newline: bool = False,
        comma: bool = False,
        no_indent: bool = False,
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting `{values}` (newline:{newline}, comma:{comma}, no_indent:{no_indent}, _indent_level:{self._indent_level})"
        )

        if (
            not no_indent
            and self._settings.has_indent
            and len(values) > 0
            and self._indent_level > 0
        ):  # Don't indent if we only want comma / newline
            yield self._indent_level * self._settings.indent  # type: ignore
        for i, v in enumerate(values):
            yield v
            if i < len(values) - 1 and self._settings.has_whitespace:
                yield self._settings.whitespace  # type: ignore
        if comma:
            yield ","

        if newline and self._settings.has_newline:
            yield self._settings.newline  # type: ignore

    def _format_str(
        self, value: str, *, in_collection: bool = False, in_assignment: bool = False
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting String `{value}` (in_collection:{in_collection}, in_assignment:{in_assignment})"
        )
        yield from self._formatted(
            f'"{value}"',
            comma=in_collection,
            newline=in_assignment,
            no_indent=in_assignment,
        )

    def _format_number(
        self,
        value: Union[float, int],
        *,
        in_collection: bool = False,
        in_assignment: bool = False,
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting Number `{value}` (in_collection:{in_collection}, in_assignment:{in_assignment})"
        )
        yield from self._formatted(
            str(value),
            comma=in_collection,
            newline=in_assignment,
            no_indent=in_assignment,
        )

    def _format_path(
        self,
        value: Union[str, PathLike[str]],
        *,
        in_collection: bool = False,
        in_assignment: bool = False,
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting Path `{value}` (in_collection:{in_collection}, in_assignment:{in_assignment})"
        )
        yield from self._formatted(
            f"[[{value if not hasattr(value, '__fspath__') else value.__fspath__()}]]",
            comma=in_collection,
            newline=in_assignment,
            no_indent=in_assignment,
        )

    def _format_collection(
        self,
        encoded: Union[List[Any], Dict[str, Any]],
        *,
        in_collection: bool = False,
        in_assignment: bool = False,
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting Collection `{encoded}` (in_collection:{in_collection}, in_assignment:{in_assignment})"
        )
        if in_assignment:
            yield from self._formatted(newline=True)
        if isinstance(encoded, list):
            yield from self._formatted("{", newline=True)
            with self._enter_indent():
                for i, item in enumerate(encoded):
                    yield from self._format_item(
                        item, in_collection=i != len(encoded) - 1
                    )  # Don't add comma to last item
            yield from self._formatted("}", comma=in_collection, newline=True)

        elif isinstance(encoded, dict):
            yield from self._formatted("{", newline=True)
            with self._enter_indent():
                for i, (key, value) in enumerate(encoded.items()):
                    yield from self._format_key_value(
                        key, value, in_collection=i != len(encoded) - 1
                    )  # Don't add comma to last item
            yield from self._formatted("}", comma=in_collection, newline=True)

        else:
            raise ArcivWriterError(
                f"Cannot format '{encoded}' ({encoded.__module__}.{encoded.__qualname__})"
            )

    def _format_item(
        self,
        value: Any,
        *,
        in_collection: bool = False,
        in_assignment: bool = False,
        encode: bool = True,
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting Item `{value}` (in_collection:{in_collection}, in_assignment:{in_assignment}, encode:{encode})"
        )
        encoded = self._encoder.default(value) if encode else value
        if isinstance(encoded, (list, dict)):
            yield from self._format_collection(
                encoded, in_collection=in_collection, in_assignment=in_assignment
            )
        elif isinstance(encoded, str):
            yield from self._format_str(
                encoded, in_collection=in_collection, in_assignment=in_assignment
            )
        elif isinstance(encoded, (int, float)):
            yield from self._format_number(
                encoded, in_collection=in_collection, in_assignment=in_assignment
            )
        elif isinstance(encoded, PathLike):
            yield from self._format_path(
                encoded, in_collection=in_collection, in_assignment=in_assignment
            )
        else:
            raise ArcivWriterError(
                f"Cannot format '{encoded}' ({encoded.__module__}.{encoded.__qualname__})"
            )

    def _format_key_value(
        self, key: str, value: Any, *, in_collection: bool = False
    ) -> Iterable[str]:
        logger.debug(
            f"Formatting Key/Value `{key}`/`{value}` (in_collection:{in_collection})"
        )
        yield from self._formatted(key, "=")
        if self._settings.has_whitespace:
            yield self._settings.whitespace  # type: ignore
        yield from self._format_item(
            value, in_assignment=True, in_collection=in_collection
        )

    def tokens(self, data: Any) -> Iterable[str]:
        logger.debug(f"Iterating Tokens on {data}")
        encoded = self._encoder.default(data)
        if not isinstance(encoded, dict):
            raise RelicToolError(
                "Encoder cannot convert `data` to a dictionary, the root item must be a dictionary."
            )
        for key, value in encoded.items():
            yield from self._format_key_value(key, value)

    def write(self, data: Any) -> str:
        logger.debug(f"Writing Arciv data {data} to string")
        with StringIO() as fp:
            self.writef(fp, data)
            return fp.getvalue()

    def writef(self, fp: TextIO, data: Any) -> None:
        logger.debug(f"Writing Arciv data {data} to file {fp}")
        for token in self.tokens(data):
            fp.write(token)


class ArcivEncoder:
    def default(
        self, obj: Any
    ) -> Union[str, PathLike[str], int, float, Dict[str, Any], List[Any]]:
        if isinstance(obj, _ArcivSpecialEncodable):
            # Special case to handle the _Arciv Dataclass and its parts
            #   These classes may not map 1-1 to the file; such as the root; which has an implied ARCHIVE = field
            return obj.to_parser_dict()
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)  # type: ignore
        if isinstance(obj, (str, int, float, dict, list, PathLike)):
            return obj
        raise ArcivEncoderError(
            f"Cannot encode '{obj}' ({obj.__module__}.{obj.__qualname__})"
        )


class _ArcivSpecialEncodable:
    """Marks the class as needing special handling when automatically being encoded."""

    def to_parser_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

from __future__ import annotations

import dataclasses
import os
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from logging import getLogger
from os import PathLike
from typing import Optional, Iterable, Union, List, Dict, Any, TextIO, Iterator
from relic.core.logmsg import BraceMessage

from relic.core.errors import RelicToolError
from relic.sga.v2.arciv.errors import ArcivWriterError, ArcivEncoderError

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
        """
        A context manager that manages the indent level of the writer
        """
        self._indent_level += 1
        logger.debug(BraceMessage("Entering Indent `{0}`", self._indent_level))
        yield None
        self._indent_level -= 1
        logger.debug(BraceMessage("Exiting Indent `{0}`", self._indent_level))

    def _formatted(
        self,
        *values: str,
        newline: bool = False,
        comma: bool = False,
        no_indent: bool = False,
    ) -> Iterable[str]:
        """
        Returns a list of formatted tokens
        """
        logger.debug(
            BraceMessage(
                "Formatting `{0}` (newline:{1}, comma:{2}, no_indent:{3}, _indent_level:{4})",
                values,
                newline,
                comma,
                no_indent,
                self._indent_level,
            )
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
        """
        Formats a number-like as a string (formatted as an string) in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting String `{0}` (in_collection:{1}, in_assignment:{2})",
                value,
                in_collection,
                in_assignment,
            )
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
        """
        Formats a number-like as a string (formatted as an int object) in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting Number `{0}` (in_collection:{1}, in_assignment:{2})",
                value,
                in_collection,
                in_assignment,
            )
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
        """
        Formats a string-like as a string (formatted as a path object) in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting Path `{0}` (in_collection:{1}, in_assignment:{2})",
                value,
                in_collection,
                in_assignment,
            )
        )
        yield from self._formatted(
            f"[[{os.fspath(value)}]]",
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
        """
        Formats a collection as a string in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting Collection `{0}` (in_collection:{1}, in_assignment:{2})",
                encoded,
                in_collection,
                in_assignment,
            )
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
        """
        Formats an arbitrary value as a string in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting Item `{0}` (in_collection:{1}, in_assignment:{2}, encode:{3})",
                value,
                in_collection,
                in_assignment,
                encode,
            )
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
        """
        Formats a key-value pair into a string in the 'arciv' specification
        """
        logger.debug(
            BraceMessage(
                "Formatting Key/Value `{0}`/`{1}` (in_collection:{2})",
                key,
                value,
                in_collection,
            )
        )
        yield from self._formatted(key, "=")
        if self._settings.has_whitespace:
            yield self._settings.whitespace  # type: ignore
        yield from self._format_item(
            value, in_assignment=True, in_collection=in_collection
        )

    def tokens(self, data: Any) -> Iterable[str]:
        """
        Converts the given data into string tokens that can be written to a file
        """
        logger.debug(BraceMessage("Iterating Tokens on {0}", data))
        encoded = self._encoder.default(data)
        if not isinstance(encoded, dict):
            raise RelicToolError(
                "Encoder cannot convert `data` to a dictionary, the root item must be a dictionary."
            )
        for key, value in encoded.items():
            yield from self._format_key_value(key, value)

    def write(self, data: Any) -> str:
        """
        Writes the data to a string, using the 'arciv' specification.
        """
        logger.debug(BraceMessage("Writing Arciv data {0} to string", data))
        with StringIO() as fp:
            self.writef(fp, data)
            return fp.getvalue()

    def writef(self, fp: TextIO, data: Any) -> None:
        """
        Writes the data to the file handle, using the 'arciv' specification.
        """
        logger.debug(BraceMessage("Writing Arciv data {0} to file {1}", data, fp))
        for token in self.tokens(data):
            fp.write(token)


class ArcivEncoder:  # pylint: disable=R0903
    """
    An object which defines the behavior for converting objects to an encodable type
    """

    def default(
        self, obj: Any
    ) -> Union[str, PathLike[str], int, float, Dict[str, Any], List[Any]]:
        """
        Converts the given object to an encodable type
        """
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


class _ArcivSpecialEncodable:  # pylint: disable=R0903
    """Marks the class as needing special handling when automatically being encoded."""

    def to_parser_dict(self) -> Dict[str, Any]:
        """
        Performs the manual conversion of this object to an Arciv Encodable dictionary
        """
        raise NotImplementedError

from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from os import PathLike
from typing import Optional, Iterable, Union, List, Dict, Any, TextIO

from relic.sga.v2.arciv.legacy import ArcivEncoder


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
    def _enter_indent(self) -> None:
        self._indent_level += 1
        yield None
        self._indent_level -= 1

    def _formatted(
        self,
        *values: str,
        newline: bool = False,
        comma: bool = False,
        no_indent: bool = False,
    ) -> Iterable[str]:
        if (
            not no_indent
            and self._settings.has_indent
            and len(values) > 0
            and self._indent_level > 0
        ):  # Dont indent if we only want comma / newline
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
        yield from self._formatted(
            str(value),
            comma=in_collection,
            newline=in_assignment,
            no_indent=in_assignment,
        )

    def _format_path(
        self,
        value: Union[str,PathLike[str]],
        *,
        in_collection: bool = False,
        in_assignment: bool = False,
    ) -> Iterable[str]:
        yield from self._formatted(
            f"[[{value if not hasattr(value,'__fspath__') else value.__fspath__()}]]",
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
            raise NotImplementedError(
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
            raise NotImplementedError(
                f"Cannot format '{encoded}' ({encoded.__module__}.{encoded.__qualname__})"
            )

    def _format_key_value(
        self, key: str, value: Any, *, in_collection: bool = False
    ) -> Iterable[str]:
        yield from self._formatted(key, "=")
        if self._settings.has_whitespace:
            yield self._settings.whitespace  # type: ignore
        yield from self._format_item(
            value, in_assignment=True, in_collection=in_collection
        )
        # yield from self._formatted(newline=True)

    def tokens(self, data: Any) -> Iterable[str]:
        for key, value in data.items():
            yield from self._format_key_value(key, value)

    def write(self, data: Any) -> str:
        with StringIO() as fp:
            self.writef(fp, data)
            return fp.getvalue()

    def writef(self, fp: Union[str, TextIO], data: Any) -> None:
        if isinstance(fp, str):
            with open(fp, "w") as true_fp:
                return self.writef(
                    true_fp, data
                )  # Return ensures we dont fall into the TextIO block below

        for token in self.tokens(data):
            fp.write(token)

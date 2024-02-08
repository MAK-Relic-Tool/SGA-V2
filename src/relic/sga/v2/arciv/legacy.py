from __future__ import annotations

from enum import Enum
from io import StringIO
from os import PathLike
from pathlib import Path
from typing import TextIO, Tuple, Optional, Iterable, Union, Dict, List, Any, Set

from relic.core.errors import RelicToolError


class Token(Enum):
    TEXT = Any
    EQUAL = "="
    CURLY_BRACE_LEFT = "{"
    CURLY_BRACE_RIGHT = "}"
    QUOTE = '"'
    COMMA = ","
    SPACE = " "
    NEW_LINE = "\n"
    TAB = "\t"
    BRACE_LEFT = "["
    BRACE_RIGHT = "]"
    EOF = None


class TokenStream:
    WS_TOKEN = [Token.SPACE, Token.NEW_LINE, Token.TAB]

    def __init__(self, tokens: Iterable[Tuple[Optional[str], Token]]):
        self._now = 0
        self._reader = iter(tokens)
        self._tokens: List[Tuple[Optional[str], Token]] = []

    def __enter__(self) -> TokenStream:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # TODO specify typing
        ...

    def _read(self, pos: int) -> Optional[Tuple[Optional[str], Token]]:
        if len(self._tokens) <= pos:  # Early exit on empty case
            try:
                read = next(self._reader)
            except StopIteration:
                return None
            if read is None:
                return None
            self._tokens.append(read)
            return read
        return self._tokens[pos]

    def next(
        self, skip_whitespace: bool = True, advance: bool = False
    ) -> Optional[Tuple[Optional[str], Token]]:
        offset = 0
        while skip_whitespace:
            current = self._read(self._now + offset)
            if current is None:
                return None
            if current[1] in self.WS_TOKEN:
                offset += 1
                continue
            skip_whitespace = False

        current = self._read(self._now + offset)
        if current is None:
            return None
        if advance:
            self._now += offset + 1  # add one because we want the NEXT token
        return current

    def read(
        self, skip_whitespace: bool = True
    ) -> Optional[Tuple[Optional[str], Token]]:
        return self.next(skip_whitespace, advance=True)

    def peek(
        self, skip_whitespace: bool = True
    ) -> Optional[Tuple[Optional[str], Token]]:
        return self.next(skip_whitespace, advance=False)

    def empty(self, skip_whitespace: bool = True) -> bool:
        return self.peek(skip_whitespace) is not None


_KiB = 1024
_4KiB = 4 * _KiB


class Lexer:
    def __init__(self, text: Union[TextIO, str], buffer_size: int = _4KiB):
        self._own_handle: Optional[TextIO] = None  # A handle we own

        if isinstance(text, str):
            text = self._own_handle = StringIO(text)

        self._reader: TextIO = text
        self._buffer: Optional[str] = None
        self._buffer_size: int = buffer_size
        self._now: int = 0
        # Catch EOF and TEXT; remove them from symbols
        self._symbols: Set[Token] = set(
            [c for c in Token if c.value is not None and c.value is not Any]
        )
        self._symbol_values: Set[str] = set([c.value for c in self._symbols])

    def __enter__(self) -> Iterable[Tuple[Optional[str], Token]]:
        return self.tokenize()

    def close(self) -> None:
        if self._own_handle is not None:
            self._own_handle.close()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __iter__(self) -> Iterable[Tuple[Optional[str], Token]]:
        yield from self.tokenize()

    def _read_buffer(self) -> Optional[str]:
        if self._buffer is None or self._now >= len(self._buffer):
            self._buffer = self._reader.read(self._buffer_size)
            self._now = 0
        if len(self._buffer) == 0:
            return None
        else:
            return self._buffer[self._now :]

    def _read_until_next(self, include_eof: bool = True) -> Tuple[str, Token]:
        parts: List[str] = []
        while True:
            buffer = self._read_buffer()
            if buffer is None:
                if include_eof:
                    text = "".join(parts)
                    return text, Token.EOF
                else:
                    raise NotImplementedError(
                        "Token stream trying to read past end of file!"
                    )

            # scan
            for i, c in enumerate(buffer):
                if c in self._symbol_values:
                    self._now += i + 1
                    partial_part = buffer[:i]
                    parts.append(partial_part)
                    text = "".join(parts)
                    return text, Token(c)

            parts.append(buffer)
            self._now += len(buffer)
            continue

    def tokenize(self) -> Iterable[Tuple[Optional[str], Token]]:
        while True:
            block, token = self._read_until_next()
            if len(block) > 0:
                yield block, Token.TEXT
            yield token.value, token
            if token is Token.EOF:  # EOF; break out of loop
                break


class Parser:
    def __init__(self, stream: TokenStream):
        self.stream: TokenStream = stream

    def __enter__(self) -> Dict[str, Any]:
        return self.parse()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        ...

    def parse(self) -> Dict[str, Any]:
        return self._parse_dict_content(is_root=True)

    def get_next(
        self, *expected: Token, skip_whitespace: bool = True, advance: bool = True
    ) -> Tuple[Optional[str], Token]:
        pair = self.stream.next(skip_whitespace=skip_whitespace, advance=advance)
        if pair is None:
            raise RelicToolError("EoF reached!")

        value, token = pair
        if token not in expected:
            X = 64
            _GATHER_LAST_X = self.stream._tokens[-X:]
            _NONWS = [t for t in _GATHER_LAST_X if t[1] not in TokenStream.WS_TOKEN]

            def _escape(_v: Optional[str]) -> Optional[str]:
                return (
                    _v.replace("\t", "\\t").replace("\n", "\\n")
                    if _v is not None
                    else _v
                )

            LINES = [f"\t\t{t.name.ljust(16)} : {_escape(v)}" for v, t in _NONWS]
            TOKEN_STR = "\n".join(LINES)
            raise RelicToolError(
                f"Recieved unexpected token '{token}', expected any of '{expected}'!\n\tLast '{X}' tokens (ignoring whitespace '{len(_NONWS)}'):\n{TOKEN_STR}"
            )

        return value, token

    def check_next(self, *expected: Token, skip_whitespace: bool = True) -> bool:
        pair = self.stream.next(skip_whitespace=skip_whitespace, advance=False)
        if pair is None:
            return False

        value, token = pair
        return token in expected

    def _eat_optional_comma(self, skip_whitespace: bool = True) -> None:
        pair = self.stream.next(skip_whitespace, advance=False)
        if pair is not None and pair[1] == Token.COMMA:
            self.stream.next(skip_whitespace, advance=True)

    def _parse_list_content(self) -> List[Any]:
        list_items = []
        while True:
            if self.check_next(Token.CURLY_BRACE_RIGHT):
                break  # Early exit

            value = self._parse_block()
            list_items.append(value)

            if self.check_next(Token.CURLY_BRACE_RIGHT):
                break  # Assume implied comma

            self.get_next(Token.COMMA)  # Eat Comma
        return list_items

    def _parse_dict_content(self, is_root: bool = False) -> Dict[str, Any]:
        dict_items = {}
        while True:
            if self.check_next(Token.CURLY_BRACE_RIGHT):
                break  # Early exit

            name, value = self._parse_assignment()
            dict_items[name] = value

            if self.check_next(Token.CURLY_BRACE_RIGHT):  # Parent's end brace
                break  # Assume implied comma

            if is_root and self.check_next(
                Token.EOF
            ):  # Root wont find a parent's end brace; and tries to consume a comma; terminate if eof reached
                break  # No Comma in root dict

            self.get_next(Token.COMMA)  # Eat Comma
        return dict_items

    def _parse_block(self) -> Union[Dict[str, Any], List[Any]]:
        _ = self.get_next(Token.CURLY_BRACE_LEFT)
        _, token = self.get_next(
            Token.CURLY_BRACE_LEFT, Token.TEXT, Token.CURLY_BRACE_RIGHT, advance=False
        )
        value: Union[Dict[str, Any], List[Any]]
        if token == Token.CURLY_BRACE_RIGHT:  # empty dict
            _ = self.get_next(Token.CURLY_BRACE_RIGHT)  # Eat Dict End
            return {}
        elif token == Token.CURLY_BRACE_LEFT:  # List
            # _ = self._next_is(Token.CURLY_BRACE_LEFT)  # Eat list start
            value = self._parse_list_content()
            # _ = self._next_is(Token.CURLY_BRACE_RIGHT)  # Eat List end

        elif token == Token.TEXT:  # Dict
            value = self._parse_dict_content()
        else:
            raise NotImplementedError(token)
        _ = self.get_next(Token.CURLY_BRACE_RIGHT)
        return value

    def _parse_path(self) -> PathLike[str]:
        parts: List[str] = []
        _ = self.get_next(Token.BRACE_LEFT)
        _ = self.get_next(Token.BRACE_LEFT)
        while True:
            part = self.stream.read(False)
            if part is None:
                raise RelicToolError("Ran out of tokens!")
            content, token_type = part
            if token_type == Token.BRACE_RIGHT:
                break
            parts.append(content)
        _ = self.get_next(Token.BRACE_RIGHT)
        full_string = "".join(parts)
        return Path(full_string)

    def _parse_string(self) -> str:
        parts: List[str] = []
        _ = self.get_next(Token.QUOTE)
        while True:
            part = self.stream.read(False)
            if part is None:
                raise RelicToolError("Ran out of tokens!")
            content, token_type = part
            if token_type == Token.QUOTE:
                break
            parts.append(content)
        return "".join(parts)

    def _parse_numeric(self) -> Union[float, int]:
        text, _ = self.get_next(Token.TEXT)
        value = float(
            text
        )  # arciv doesn't have any float fields; but we parse as float first
        if int(value) == value:
            value = int(value)  # coerce to int if applicable
        return value

    def _parse_assignment(
        self,
    ) -> Tuple[str, Union[str, Dict[str, Any], PathLike[str], float, int, List[Any]]]:
        name, _ = self.get_next(Token.TEXT)
        _ = self.get_next(Token.EQUAL)

        _, assign_type = self.get_next(
            Token.BRACE_LEFT,
            Token.CURLY_BRACE_LEFT,
            Token.QUOTE,
            Token.TEXT,
            advance=False,
        )
        value: Union[str, Dict[str, Any], PathLike[str], float, int, List[Any]]
        if assign_type == Token.BRACE_LEFT:  # Path (String)
            value = self._parse_path()
        elif assign_type == Token.CURLY_BRACE_LEFT:  # Dict | List
            value = self._parse_block()
        elif assign_type == Token.QUOTE:  # String
            value = self._parse_string()
        elif assign_type == Token.TEXT:  # Number / Float
            value = self._parse_numeric()
        else:
            raise NotImplementedError(assign_type)

        return name, value


def load(f: Union[TextIO, str]) -> Dict[str, Any]:
    if isinstance(f, str):
        with open(f, "r") as h:
            return load(h)

    with Lexer(f) as tokens:
        with TokenStream(tokens) as stream:
            with Parser(stream) as parsed:
                return parsed


def loads(f: str) -> Dict[str, Any]:
    with StringIO(f) as h:
        return load(h)


#
#
# def dump(f: Union[TextIO, str], data: Any, **settings:Any) -> None:
#     formatter = Writer(**settings)
#     formatter.writef(f, data)
#
#
# def dumps(data: Any, **settings) -> str:
#     with StringIO() as h:
#         dump(h, data, **settings)
#         return h.getvalue()

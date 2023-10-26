from enum import Enum
from io import StringIO
from typing import TextIO, Tuple, Optional, Iterable, Union, Dict, List, Any

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

    def __init__(self, tokens: Iterable[Tuple[str, Token]]):
        self._now = 0
        self._reader = iter(tokens)
        self._tokens = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    def _read(self, pos) -> Optional[Tuple[str, Token]]:
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

    def next(self, skip_whitespace: bool = True, advance: bool = False) -> Optional[Tuple[str, Token]]:
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

    def read(self, skip_whitespace: bool = True) -> Optional[Tuple[str, Token]]:
        return self.next(skip_whitespace, advance=True)

    def peek(self, skip_whitespace: bool = True) -> Optional[Tuple[str, Token]]:
        return self.next(skip_whitespace, advance=False)

    def empty(self, skip_whitespace: bool = True) -> bool:
        return self.peek(skip_whitespace) is not None


_KiB = 1024
_4KiB = 4 * _KiB


class Lexer:
    def __init__(self, text: Union[TextIO, str], buffer_size: int = _4KiB):
        if isinstance(text, str):
            text = self._handle = StringIO(text)
        else:
            self._handle = None

        self._reader = text
        self._buffer = None
        self._buffer_size = buffer_size
        self._now = 0
        self._symbols = [c for c in Token if c.value is not None and c.value is not Any]  # Catch EOF and TEXT; remove them from symbols
        self._symbol_values = [c.value for c in self._symbols]

    def __enter__(self):
        return self.tokenize()

    def close(self):
        if self._handle is not None:
            self._handle.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        ...

    def __iter__(self):
        yield from self.tokenize()

    def _read_buffer(self) -> Optional[str]:
        if self._buffer is None or self._now >= len(self._buffer):
            self._buffer = self._reader.read(self._buffer_size)
            self._now = 0
        if len(self._buffer) == 0:
            return None
        else:
            return self._buffer[self._now:]

    def _read_until_next(self, include_eof: bool = True) -> Tuple[str, Token]:
        parts = []
        while True:
            buffer = self._read_buffer()
            if buffer is None:
                if include_eof:
                    text = "".join(parts)
                    return text, Token.EOF
                else:
                    raise NotImplementedError("Token stream trying to read past end of file!")

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

            # index
            # min_symbol = len(buffer)
            # found = None
            # for symbol in self._symbols:
            #     try:
            #         min_symbol = buffer.index(symbol.value, 0, min_symbol)
            #         found = symbol
            #     except ValueError:
            #         continue
            #
            # # No matches in this block; read next
            # if found is None:
            #     parts.append(buffer)
            #     self._now += len(buffer)
            #     continue
            #
            # self._now += min_symbol + 1
            # partial_part = buffer[:min_symbol]
            # parts.append(partial_part)
            # text = "".join(parts)
            # return text, found

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
        self.stream = stream

    def __enter__(self):
        return self.parse()

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    def parse(self) -> Dict[str, Any]:
        return self._parse_dict_content(is_root=True)

    def get_next(self, *expected: Token, skip_whitespace: bool = True, advance: bool = True):
        pair = self.stream.next(skip_whitespace=skip_whitespace, advance=advance)
        if pair is None:
            raise RelicToolError("EoF reached!")

        value, token = pair
        if token not in expected:
            X = 64
            _GATHER_LAST_X = self.stream._tokens[-X:]
            _NONWS = [t for t in _GATHER_LAST_X if t[1] not in TokenStream.WS_TOKEN]
            def _escape(_v:str) -> str:
                return _v.replace("\t","\\t").replace("\n","\\n") if _v is not None else _v
            LINES = [f"\t\t{t.name.ljust(16)} : {_escape(v)}" for v, t in _NONWS]
            TOKEN_STR = "\n".join(LINES)
            raise RelicToolError(f"Recieved unexpected token '{token}', expected any of '{expected}'!\n\tLast '{X}' tokens (ignoring whitespace '{len(_NONWS)}'):\n{TOKEN_STR}")

        return value, token

    def check_next(self, *expected: Token, skip_whitespace: bool = True):
        pair = self.stream.next(skip_whitespace=skip_whitespace, advance=False)
        if pair is None:
            return False

        value, token = pair
        return token in expected

    def _eat_optional_comma(self, skip_whitespace: bool = True):
        if self.stream.next(skip_whitespace, advance=False)[1] == Token.COMMA:
            self.stream.next(skip_whitespace, advance=True)

    def _parse_list_content(self) -> List:
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

    def _parse_dict_content(self, is_root: bool = False) -> Dict:
        dict_items = {}
        while True:
            if self.check_next(Token.CURLY_BRACE_RIGHT):
                break  # Early exit

            name, value = self._parse_assignment()
            dict_items[name] = value

            if self.check_next(Token.CURLY_BRACE_RIGHT):  # Parent's end brace
                break  # Assume implied comma

            if is_root and self.check_next(Token.EOF):  # Root wont find a parent's end brace; and tries to consume a comma; terminate if eof reached
                break  # No Comma in root dict

            self.get_next(Token.COMMA)  # Eat Comma
        return dict_items

    def _parse_block(self) -> Union[Dict, List]:
        _ = self.get_next(Token.CURLY_BRACE_LEFT)
        _, token = self.get_next(Token.CURLY_BRACE_LEFT, Token.TEXT, Token.CURLY_BRACE_RIGHT, advance=False)
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

    def _parse_path(self):
        parts = []
        _ = self.get_next(Token.BRACE_LEFT)
        _ = self.get_next(Token.BRACE_LEFT)
        while True:
            content, token_type = self.stream.read(False)
            if token_type == Token.BRACE_RIGHT:
                break
            parts.append(content)
        _ = self.get_next(Token.BRACE_RIGHT)
        return "".join(parts)

    def _parse_string(self):
        parts = []
        _ = self.get_next(Token.QUOTE)
        while True:
            content, token_type = self.stream.read(False)
            if token_type == Token.QUOTE:
                break
            parts.append(content)
        return "".join(parts)

    def _parse_numeric(self):
        text, _ = self.get_next(Token.TEXT)
        value = float(text)  # arciv doesn't have any float fields; but we parse as float first
        if int(value) == value:
            value = int(value)  # coerce to int if applicable
        return value

    def _parse_assignment(self) -> Tuple[str, Union[str, Dict, List]]:
        name, _ = self.get_next(Token.TEXT)
        _ = self.get_next(Token.EQUAL)

        _, assign_type = self.get_next(Token.BRACE_LEFT, Token.CURLY_BRACE_LEFT, Token.QUOTE, Token.TEXT, advance=False)

        if assign_type == Token.BRACE_LEFT:  # Path (String)
            value = f'"{self._parse_path()}"'
        elif assign_type == Token.CURLY_BRACE_LEFT:  # Dict | List
            value = self._parse_block()
        elif assign_type == Token.QUOTE:  # String
            value = self._parse_string()
        elif assign_type == Token.TEXT:  # Number / Float
            value = self._parse_numeric()
        else:
            raise NotImplementedError(assign_type)

        return name, value



class Formatter:
    def __init__(self, indent: str = "\t"):
        self._indent = indent

    def format(self, d: str):
        ...


def load(f: Union[TextIO, str]):
    if isinstance(f, str):
        with open(f, "r") as h:
            return load(h)

    with Lexer(f) as tokens:
        with TokenStream(tokens) as stream:
            with Parser(stream) as parsed:
                return parsed


def loads(f: str) -> Dict:
    with StringIO(f) as h:
        return load(h)


def dump(f: Union[TextIO, str], d: Dict[str, Any], **settings):
    if isinstance(f, str):
        with open(f, "w") as h:
            return dump(h, d)

    with Formatter(**settings) as formatter:
        for token in formatter:
            f.write(token)

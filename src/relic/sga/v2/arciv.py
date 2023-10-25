from enum import Enum, auto
from io import StringIO
from typing import TextIO, Tuple, Optional, Iterable, Iterator, Union, Dict, List, IO, Any

from relic.core.errors import RelicToolError


class Token(Enum):
    TEXT = auto()
    EQUAL = auto()
    CURLY_BRACE_LEFT = auto()
    CURLY_BRACE_RIGHT = auto()
    QUOTE = auto()
    COMMA = auto()
    SPACE = auto()
    NEW_LINE = auto()
    TAB = auto()
    BRACE_LEFT = auto()
    BRACE_RIGHT = auto()
    EOF = auto()


class TokenValues(Enum):
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

    @classmethod
    def all(cls):
        return [_.value for _ in cls]

    @classmethod
    def get_value(cls, token: Token):
        return getattr(cls, token.name) if token is not Token.TEXT else None

    @classmethod
    def get_token(cls, value: str):
        for t in cls:
            if t.value == value:
                return getattr(Token, t.name)
        raise KeyError(value)


class Lexer:
    class TokenStream:
        WS_TOKEN = [Token.SPACE, Token.NEW_LINE, Token.TAB]

        def __init__(self, tokens: Iterable[Tuple[str, Token]]):
            self._now = 0
            self._reader = iter(tokens)
            self._tokens = []
            self._nonws_token = []  # FOR DEBUGGING

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
                if read[1] not in self.WS_TOKEN:
                    self._nonws_token.append(read)
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

    def __init__(self, text: Union[TextIO, str]):
        if isinstance(text, str):
            with StringIO(text) as handle:
                tokens = self._tokenize(handle)
        else:
            tokens = self._tokenize(text)

        self.stream = self.create_stream(tokens)  # tokens are iterable

    def __enter__(self):
        return self.stream

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    @classmethod
    def create_stream(cls, tokens: Iterable[Tuple[str, Token]]) -> TokenStream:
        return cls.TokenStream(tokens)

    @classmethod
    def _read_until_next(cls, file: TextIO, *chars: str, include_eof: bool = True) -> Tuple[
        str, Optional[str]]:
        parts = []
        while True:
            c = file.read(1)
            if c is None or len(c) == 0:
                break
            if c in chars:
                return "".join(parts), c
            parts.append(c)
        if include_eof:
            return "".join(parts), None
        raise NotImplementedError

    @classmethod
    def _tokenize(cls, file: TextIO) -> Iterable[Tuple[str, Token]]:
        symbols = TokenValues.all()
        while True:
            block, symbol = cls._read_until_next(file, *symbols)
            if len(block) > 0:
                yield block, Token.TEXT
            token = TokenValues.get_token(symbol)
            yield symbol, token
            if symbol is None:  # EOF; break out of loop
                break


class Parser:
    def __init__(self, stream: Lexer.TokenStream):
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
            N = 64
            TOKEN_DBG_PARTS = [f"\t\t{t[1].name.ljust(len('CURLY_BRACE_LEFT'))} :\t'{t[0]}'" for t in self.stream._nonws_token[-N:]]
            TOKEN_DBG = "\n".join(TOKEN_DBG_PARTS)
            raise RelicToolError(f"Recieved unexpected token '{token}', expected any of '{expected}'!\n\tLast '{N}' tokens:\n{TOKEN_DBG}")

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

            if self.check_next(Token.CURLY_BRACE_RIGHT): # Parent's end brace
                break  # Assume implied comma

            if is_root and self.check_next(Token.EOF): # Root wont find a parent's end brace; and tries to consume a comma; terminate if eof reached
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

        # parse
        stack_ptr = 0  # Count {}

class Formatter:
    def __init__(self, indent:str="\t"):
        self._indent = indent

    def format(self, d:str):


def load(f: Union[TextIO, str]):
    if isinstance(f, str):
        with open(f, "r") as h:
            return load(h)

    with Lexer(f) as tokens:
        with Parser(tokens) as parsed:
            return parsed


def loads(f: str):
    with StringIO(f) as h:
        return load(h)

def dump(f: Union[TextIO, str], d:Dict[str,Any]):
    if isinstance(f, str):
        with open(f, "w") as h:
            return dump(h)

    with Lexer(f) as tokens:
        with Parser(tokens) as parsed:
            return parsed

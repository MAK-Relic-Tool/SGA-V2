from __future__ import annotations

from relic.core.errors import RelicToolError


class ArcivError(RelicToolError): ...


class ArcivWriterError(ArcivError): ...


class ArcivEncoderError(ArcivError): ...


class ArcivLayoutError(ArcivError): ...


class ArcivParsingError(ArcivError):
    def __init__(self, token_type: str, token_value: str, linepoos: int, charpos: int):
        self.token_type = token_type
        self.token_value = token_value
        self.line_pos = linepoos
        self.char_pos = charpos

    def __str__(self) -> str:
        return f"Failed to parse Arciv, unexpected token; '{self.token_type}' ('{self.token_value}') at Line {self.line_pos}, Char {self.char_pos}"

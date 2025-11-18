"""
Lexer for '.sgaconfig' file
"""

from typing import Any

from ply import lex
from ply.lex import LexToken, Lexer, LexError


reserved = {
    "Archive": "KW_ARCHIVE",
    "TOCStart": "KW_TOC_START",
    "TOCEnd": "KW_TOC_END",
    "FileSettingsStart": "KW_FILE_SETTINGS_START",
    "FileSettingsEnd": "KW_FILE_SETTINGS_END",
    "SkipFile": "KW_SKIP_FILE",
    "Override": "KW_OVERRIDE",
    # these should delegate to name
    # "alias": "KW_ALIAS",
    # "relativeroot": "KW_RELATIVE_ROOT",
    # "defcompression": "KW_DEFAULT_COMPRESSION",
    # "maxsize": "KW_MAX_SIZE",
    # "minsize": "KW_MIN_SIZE",
    # "wildcard": "KW_WILDCARD",
    # "ct": "KW_COMPRESSION_TYPE",
}

tokens = [
    "NAME",
    "VALUE",
    # "COMMENT",
    "NEWLINE",
] + list(reserved.values())

literals = ["="]


def _add_linepos(t: LexToken):
    linepos = getattr(t.lexer, "linepos", 0)
    t.linepos = t.lexpos - linepos


def t_NAME(t: LexToken) -> LexToken:  # pylint: disable=C0103
    r"""[A-Za-z]+"""
    t.type = reserved.get(t.value, "NAME")
    _add_linepos(t)
    return t


def t_equal(t: LexToken) -> LexToken:
    r"""="""
    t.type = "="
    _add_linepos(t)
    return t


def t_comment(t: LexToken) -> None:
    r"""//.*"""  # match '//'
    # t.type = "COMMENT"
    pass


def t_VALUE(t: LexToken) -> LexToken:  # pylint: disable=C0103
    r"""\".*?\" """
    stripped = t.value[1:-1]  # strip quote
    t.value = stripped
    _add_linepos(t)
    return t


t_ignore = " \t"  # pylint: disable=C0103


# Define a rule so we can track line numbers
def t_NEWLINE(t: LexToken) -> LexToken:
    r"\n+"
    t.type = "NEWLINE"
    t.lexer.lineno += len(t.value)
    _add_linepos(t)  # add linepos BEFORE updating lexer's linepos
    t.lexer.linepos = t.lexer.lexpos
    return t


# Error handling rule
def t_error(t: LexToken) -> None:
    """
    Fallback for when a token is read that fails to match the list of defined tokens
    """
    _add_linepos(t)
    raise LexError(
        f"Scanning error. Illegal character '{t.value[0]}' found at L{t.lineno}:{getattr(t,"linepos",t.lexpos)}",
        t.value[0],
    )


# Build the lexer
def build(**kwargs: Any) -> Lexer:
    """
    Build the lexer object, using the schema provided in this file
    """
    return lex.lex(**kwargs)

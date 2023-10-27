from typing import Any

from ply import lex
from ply.lex import LexToken, Lexer, LexError

tokens = (
    "NAME",
    "EQUAL",
    "CURLY_BRACE_LEFT",
    "CURLY_BRACE_RIGHT",
    "COMMA",
    "NUMBER",
    "PATH",
    "STRING",
    # "WHITESPACE",
)
# t_WHITESPACE = r"\s+"
t_NAME = r"\w+"


literals = ["{", "}", ",", "="]


def t_CURLY_BRACE_LEFT(t: LexToken) -> LexToken:
    r"""\{"""
    t.type = "{"
    return t


def t_CURLY_BRACE_RIGHT(t: LexToken) -> LexToken:
    r"""\}"""
    t.type = "}"
    return t


def t_EQUAL(t: LexToken) -> LexToken:
    r"""="""
    t.type = "="
    return t


def t_COMMA(t: LexToken) -> LexToken:
    r""","""
    t.type = ","
    return t


def t_STRING(t: LexToken) -> LexToken:
    r"\".*?\" "
    stripped = t.value[1:-1]  # strip quote
    t.value = stripped
    return t


def t_NUMBER(t: LexToken) -> LexToken:
    """-?\d+(?:\.\d+)?"""
    # arciv doesn't have any float fields; but we parse as float first for completeness
    t.value = float(t.value)
    if int(t.value) == t.value:
        t.value = int(t.value)  # coerce to int if applicable
    return t


def t_PATH(t: LexToken) -> LexToken:
    r"\[\[.*?\]\]"
    stripped = t.value[2:-2]  # strip brackets
    t.value = stripped
    return t


t_ignore = " \t"


# Define a rule so we can track line numbers
def t_newline(t: LexToken) -> None:
    r"\n+"
    t.lexer.lineno += len(t.value)


# Error handling rule
def t_error(t: LexToken) -> None:
    raise LexError(
        f"Scanning error. Illegal character '{t.value[0]}' found at L{t.lineno}:{t.lexpos}",
        t,
    )
    # print("Illegal character '%s'" % t.value[0])
    # t.lexer.close()
    # t.lexer.skip(1)


# Build the lexer
def build(**kwargs: Any) -> Lexer:
    return lex.lex(**kwargs)

from typing import Any

from ply import lex
from ply.lex import LexToken, Lexer, LexError

reserved = {
    "Archive":"KW_ARCHIVE",

    "ArchiveHeader":"KW_ARCHIVE_HEADER",
    "ArchiveName":"KW_ARCHIVE_NAME",

    "TOCList":"KW_TOC_LIST",
    "RootFolder":"KW_ROOT_FOLDER",
    "Files":"KW_FILES",
    "FolderInfo":"KW_FOLDER_INFO",
    "Folders":"KW_FOLDERS",

    "Size": "KW_SIZE",
    "File": "KW_FILE",
    "Store": "KW_STORE",

    "TOCHeader":"KW_TOC_HEADER",
    "Alias":"KW_ALIAS",
    "Name":"KW_NAME",
    "RootPath":"KW_ROOT_PATH",

    "Storage":"KW_STORAGE",
    "MaxSize":"KW_MAX_SIZE",
    "MinSize": "KW_MIN_SIZE",
    "Wildcard":"KW_WILDCARD",

    "folder":"KW_FOLDER",
    "path":"KW_PATH",
}

tokens = [
    "NAME",
    "NUMBER",
    "PATH",
    "STRING",
] + list(reserved.values())

literals = ["{", "}", ",", "="]

def t_NAME(t: LexToken) -> LexToken:
    r"[A-Za-z]+"
    t.type = reserved.get(t.value,"NAME")
    return t

def t_curly_brace_left(t: LexToken) -> LexToken:
    r"""\{"""
    t.type = "{"
    return t


def t_curly_brace_right(t: LexToken) -> LexToken:
    r"""\}"""
    t.type = "}"
    return t


def t_equal(t: LexToken) -> LexToken:
    r"""="""
    t.type = "="
    return t


def t_comma(t: LexToken) -> LexToken:
    r""","""
    t.type = ","
    return t


def t_STRING(t: LexToken) -> LexToken:
    r"\".*?\" "
    stripped = t.value[1:-1]  # strip quote
    t.value = stripped
    return t


def t_NUMBER(t: LexToken) -> LexToken:
    r"""-?\d+(?:\.\d+)?"""
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
        t.value[0],
    )
    # print("Illegal character '%s'" % t.value[0])
    # t.lexer.close()
    # t.lexer.skip(1)


# Build the lexer
def build(**kwargs: Any) -> Lexer:
    return lex.lex(**kwargs)

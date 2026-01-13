from ply import yacc
from ply.yacc import LRParser
from ply.yacc import YaccError
from relic.core.errors import RelicToolError

from relic.sga.v2.sgaconfig import lexer

tokens = lexer.tokens


def p_root(p):
    """
    expression  : KW_ARCHIVE NEWLINE tocs
    """
    p[0] = {"tocs": p[3]}  # tocs


def p_tocs(p):
    """
    tocs    : toc
            | tocs toc
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        l = p[1]
        l.append(p[2])
        p[0] = l


def p_toc(p):
    """
    toc   : toc_start toc_args toc_end
    """
    d = p[1]
    d["settings"] = p[2]
    p[0] = d


def p_toc_start(p):
    """
    toc_start : KW_TOC_START kvps NEWLINE
    """
    p[0] = p[2]  # return kvps


def p_toc_args(p):
    """
    toc_args : file_settings
             | toc_args file_settings
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        raise RelicToolError(
            "Multiple `FileSetting` blocks found!"
            " Please ensure `.sgaconfig' has a single `FileSettingStart/End` section per `TOCStart/TOCEnd`. "
            " Mod Assistant handles this by silently failing and ignores ALL FileSettings."
            " If you believe this is a mistake (or Mod Assistant changes this behaviour),"
            " please create an issue on the github issue tracker: https://github.com/MAK-Relic-Tool/Issue-Tracker"
        )


def p_file_settings(p):
    """
    file_settings : file_settings_start file_settings_args file_settings_end
    """
    d = p[1]  # file_settings_starts kvps
    d.update(p[2])  # file_settings_args (skip/override)
    p[0] = d


def p_file_settings_start(p):
    """
    file_settings_start : KW_FILE_SETTINGS_START kvps NEWLINE
    """
    p[0] = p[2]  # copy kvs


def p_file_settings_end(p):
    """
    file_settings_end : KW_FILE_SETTINGS_END NEWLINE
    """
    pass  # only for context handling; dont automatically via the grammer


def p_file_settings_args(p):
    """
    file_settings_args  : file_settings_override
                        | file_settings_skip_file
                        | file_settings_args file_settings_override
                        | file_settings_args file_settings_skip_file
    """
    if len(p) == 2:  # override/skip
        d = {"skip": [], "override": []}
        _type, kvps = p[1]
    else:  # dict, override/skip
        d = p[1]
        _type, kvps = p[2]
    d[_type].append(kvps)
    p[0] = d


def p_toc_end(p):
    """
    toc_end : KW_TOC_END NEWLINE
    """
    pass  # context handling


def p_file_settings_override(p):
    """
    file_settings_override : KW_OVERRIDE kvps NEWLINE
    """
    p[0] = ("override", p[2])  # return kvps


def p_file_settings_skip_file(p):
    """
    file_settings_skip_file : KW_SKIP_FILE kvps NEWLINE
    """
    p[0] = ("skip", p[2])


def p_kvps(p):
    """
    kvps : NAME '=' VALUE
         | kvps NAME '=' VALUE
    """
    if len(p) == 4:  # key=value
        k, v = p[1], p[3]
        p[0] = {k: v}
    else:  # {} key=value
        d, k, v = p[1], p[2], p[4]
        d[k] = v
        p[0] = d


def p_error(p):
    raise RelicToolError(f"Parsing Error: `{p}`") from YaccError(p)


# Build the lexer
def build(**kwargs) -> LRParser:
    return yacc.yacc(**kwargs)

from ply import yacc
from ply.yacc import LRParser

from relic.sga.v2.arciv import lexer

tokens = lexer.tokens


#
def p_expression_assignment(p):
    """
    expression  : NAME = { key_value_list }
    """
    v = [f"'{_}'" for _ in p]
    p[0] = {p[1]: p[4]}
    # printt(f"ROOT | ", *v, "\n\t", p[0])


def p_curly_block(p):
    """
    curly_block : { key_value_list }
                | { curly_block_list }
                | { }
    """
    v = [f"'{_}'" for _ in p]
    if len(p) == 3:
        p[0] = {}
    else:
        p[0] = p[2]
    # printt(f"CURLY BLOCK | ", *v, "\n\t", p[0])


def p_curly_block_list(p):
    """
    curly_block_list    : curly_block
                        | curly_block_list ,
                        | curly_block_list , curly_block
    """
    v = (f"'{_}'" for _ in p)
    p[0] = p[1]
    if len(p) == 4:
        if isinstance(p[0], dict):
            p[0].update(p[3])
        else:
            p[0] += p[3]

    # printt(f"CURLY BLOCK LIST | ", *v, "\n\t", p[0])


def p_key_value(p):
    """
    key_value   : NAME = STRING
                | NAME = PATH
                | NAME = NUMBER
                | NAME = curly_block
    """

    v = [f"'{_}'" for _ in p]
    p[0] = {p[1]: p[3]}
    # v2 = [f"'{_}'" for _ in p]

    # printt(f"KV | ", *v, "\n\t", p[0])


def p_key_value_list(p):
    """
    key_value_list  : key_value_list , key_value
                    | key_value_list ,
                    | key_value
    """
    v = [f"'{_}'" for _ in p]
    p[0] = p[1]
    if len(p) == 4:
        p[0].update(p[3])

    # printt(f"KV List | ", *v, "\n\t", p[0])


# Build the lexer
def build(**kwargs) -> LRParser:
    return yacc.yacc(**kwargs)

import json
import logging
import os
from json import JSONEncoder
from pathlib import Path
from typing import Any

from ply import lex, yacc

from relic.sga.v2.arciv import Formatter


class Lexer:
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
    t_EQUAL = r"="
    t_CURLY_BRACE_LEFT = r"{"
    t_CURLY_BRACE_RIGHT = r"}"
    t_COMMA = r","
    # t_WHITESPACE = r"\s+"
    t_NUMBER = r"-?\d+(?:\.\d+)?"
    t_NAME = r"\w+"

    def t_STRING(self, t):
        r'\".*\"'
        stripped = t.value[1:-1]  # strip quote
        t.value = stripped
        return t

    def t_PATH(self, t):
        r"\[\[.*\]\]"
        stripped = t.value[2:-2]  # strip brackets
        # We use path to differentiate the data from a string; also it does need to exist for packing so...
        t.value = Path(stripped)
        return t

    t_ignore = " \t"

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    # Error handling rule
    def t_error(self, t):
        print("Illegal character '%s'" % t.value[0])
        t.lexer.skip(1)

    # Build the lexer
    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)


class Parser:
    tokens = Lexer.tokens

    #
    def p_expression_assignment(self, p):
        """
        expression  : NAME EQUAL CURLY_BRACE_LEFT key_value_list CURLY_BRACE_RIGHT
        """
        v = [f"'{_}'" for _ in p]
        p[0] = {p[1]: p[4]}
        # printt(f"ROOT | ", *v, "\n\t", p[0])

    def p_curly_block(self, p):
        """
        curly_block : CURLY_BRACE_LEFT key_value_list CURLY_BRACE_RIGHT
                    | CURLY_BRACE_LEFT curly_block_list CURLY_BRACE_RIGHT
                    | CURLY_BRACE_LEFT CURLY_BRACE_RIGHT
        """
        v = [f"'{_}'" for _ in p]
        if len(p) == 3:
            p[0] = {}
        else:
            p[0] = p[2]
        # printt(f"CURLY BLOCK | ", *v, "\n\t", p[0])

    def p_curly_block_list(self, p):
        """
        curly_block_list    : curly_block
                            | curly_block_list COMMA
                            | curly_block_list COMMA curly_block
        """
        v = (f"'{_}'" for _ in p)
        p[0] = p[1]
        if len(p) == 4:
            if isinstance(p[0], dict):
                p[0].update(p[3])
            else:
                p[0] += p[3]

        # printt(f"CURLY BLOCK LIST | ", *v, "\n\t", p[0])

    def p_key_value(self, p):
        """
        key_value   : NAME EQUAL STRING
                    | NAME EQUAL PATH
                    | NAME EQUAL NUMBER
                    | NAME EQUAL curly_block
        """

        v = [f"'{_}'" for _ in p]
        p[0] = {p[1]: p[3]}
        # v2 = [f"'{_}'" for _ in p]

        # printt(f"KV | ", *v, "\n\t", p[0])

    def p_key_value_list(self, p):
        """
        key_value_list  : key_value_list COMMA key_value
                        | key_value_list COMMA
                        | key_value
        """
        v = [f"'{_}'" for _ in p]
        p[0] = p[1]
        if len(p) == 4:
            p[0].update(p[3])

        # printt(f"KV List | ", *v, "\n\t", p[0])

    # Build the lexer
    def build(self, **kwargs):
        self.parser = yacc.yacc(module=self, **kwargs)


if __name__ == "__main__":
    log = logging.getLogger()

    lexer = Lexer()
    lexer.build(debug=True, debuglog=log)

    parser = Parser()
    parser.build(debug=True, debuglog=log)

    with open(r"C:\Users\moder\Downloads\2.arciv.txt", "r") as h:
        data = h.read()



    # while True:
    class Encoder(JSONEncoder):
        def default(self, o: Any) -> str:
            if isinstance(o,os.PathLike):
                return repr(o)
            else:
                return super(Encoder, self).encode(o)
    print("===========================")
    print(data)
    r = parser.parser.parse(data, lexer=lexer.lexer)
    print("===========================")
    print(json.dumps(r,indent=4,cls=Encoder))
    print("===========================")
    print(Formatter().format(r))
    print("===========================")
    # # lexer.lexer.input(data)
    # while True:
    #     token = lexer.lexer.token()
    #     if not token:
    #         break
    #     print(token)

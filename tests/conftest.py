import builtins
import inspect
import sys
from itertools import chain
from itertools import islice
from typing import Iterator

import pprintpp
import sqlparse
from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.python import Python3Lexer
from pygments.lexers.sql import PostgresLexer


def formatter():
    return Terminal256Formatter(style='vim')


def ppsql(qry):
    sql = str(qry) % qry.compile().params
    sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    sql = highlight(sql, PostgresLexer(), formatter())
    print(sql)


def pp(obj):
    if isinstance(obj, Iterator):
        out = list(islice(obj, 10))
        out = '<generator> ' + pprintpp.pformat(out)
        obj = chain(out, obj)
    else:
        out = pprintpp.pformat(obj)
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[1]
    line = inspect.getframeinfo(frame[0]).code_context[0].strip()
    if line.endswith(')'):
        arg = line[line.find('(') + 1:-1]
        out = f'{arg} = {out}'
    out = highlight(out, Python3Lexer(), formatter())
    print(out, file=sys.__stderr__)
    return obj


pp.sql = ppsql
builtins.pp = pp

pytest_plugins = ['spinta.testing.pytest']

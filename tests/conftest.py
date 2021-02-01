import builtins
import inspect
import re
import sys
from itertools import chain
from itertools import islice
from typing import Any
from typing import Iterator
from typing import Type

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


na = object()
arg_re = re.compile(r'pp\(([^,)]+)')


def pp(
    obj: Any,
    v: Any = na,
    t: Type = na,
    on: bool = True,
    throw: bool = False,
    prefix: str = '',
    suffix: str = '',
):
    if not on:
        return obj
    if v is not na and obj is not v:
        return obj
    if t is not na and not isinstance(obj, t):
        return obj
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
    if prefix:
        print(prefix, end='')
    print(out, file=sys.__stderr__)
    if suffix:
        print(suffix, end='')
    if throw:
        raise RuntimeError('pp')
    return obj


pp.sql = ppsql
builtins.pp = pp

pytest_plugins = ['spinta.testing.pytest']

import builtins
import inspect
import os
import re
import sys
import time as time_module
from itertools import chain
from itertools import islice
from traceback import format_stack
from typing import Any
from typing import Dict
from typing import Iterator
from typing import TextIO
from typing import Type
from urllib.parse import parse_qs

import objprint
import pprintpp
import sqlparse
from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.python import Python3Lexer
from pygments.lexers.python import Python3TracebackLexer
from pygments.lexers.sql import PostgresLexer
from sqlalchemy.sql import ClauseElement
from requests_mock.adapter import _Matcher


objprint.config(honor_existing=False, depth=1)


def get_request_context(mocked_request: _Matcher, with_text: bool = False) -> list[dict[str, Any]]:
    """Helper method to build context of what the mocked URL was called with (Content, query params, URL)."""
    calls = []
    for request in mocked_request.request_history:
        data = {"method": request.method, "url": request.url, "params": request.qs, "data": parse_qs(request.text)}
        if with_text:
            data.update({"text": request.text.replace("\r\n", "\n").rstrip("\n")})
        calls.append(data)
    return calls


def formatter():
    return Terminal256Formatter(style="vim")


def ppsql(qry):
    sql = str(qry) % qry.compile().params
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    sql = highlight(sql, PostgresLexer(), formatter())
    print(sql)


na = object()
arg_re = re.compile(r"pp\(([^,)]+)")


def pp(
    obj: Any = na,
    *args,
    v: Any = na,
    t: Type = na,
    on: bool = True,  # print if on condition is true
    st: bool = False,
    tb: bool = False,
    time: bool = False,
    file: TextIO = sys.__stderr__,
    prefix: str = "\n",
    suffix: str = "",
    kwargs: Dict[str, Any] = None,
) -> Any:
    if obj is na:
        ret = None
    else:
        ret = obj
    if not on:
        return ret
    if obj is Ellipsis:
        print(file=file)
        print("_" * 72, file=file)
        return ret
    if time:
        start = time_module.time()
        ret = obj(*args, **kwargs)
        delta = time_module.time() - start
    else:
        delta = None
    if v is not na and obj is not v:
        return ret
    if t is not na and not isinstance(obj, t):
        return ret
    if obj is na:
        out = ""
        lexer = None
    elif isinstance(obj, Iterator):
        out = list(islice(obj, 10))
        ret = chain(out, obj)
        out = "<generator> " + pprintpp.pformat(out)
        lexer = Python3Lexer()
    elif isinstance(obj, ClauseElement):
        out = str(obj.compile(compile_kwargs={"literal_binds": True}))
        out = sqlparse.format(out, reindent=True, keyword_case="upper")
        out = "\n" + out
        lexer = PostgresLexer()
    else:
        out = pprintpp.pformat(obj)
        lexer = Python3Lexer()
    if obj is not na:
        frame = inspect.currentframe()
        frame = inspect.getouterframes(frame)[1]
        line = inspect.getframeinfo(frame[0]).code_context[0].strip()
        _, line = line.split("pp(", 1)
        arg = []
        stack = []
        term = {
            "(": ")",
            "[": "]",
            "{": "}",
            '"': '"',
            "'": "'",
        }
        for c in line:
            if (c == "\\" and (not stack or stack[-1] != "\\")) or c in term:
                stack.append(c)
            elif stack:
                if stack[-1] == "\\" or c == term[stack[-1]]:
                    stack.pop()
            elif c in ",)":
                break
            arg.append(c)
        arg = "".join(arg)
        out = f"{arg} = {out}"
    if lexer:
        out = highlight(out, lexer, formatter())
    if prefix:
        print(prefix, end="", file=file)
    if st:
        stack = ["Stack trace (pp):\n"]
        cwd = os.getcwd() + "/"
        for item in format_stack():
            if "/_pytest/" in item:
                continue
            if "/site-packages/pluggy/" in item:
                continue
            if "/multipledispatch/dispatcher.py" in item:
                continue
            item = item.replace(cwd, "")
            stack.append(item)
        stack = "".join(stack)
        stack = highlight(stack, Python3TracebackLexer(), formatter())
        print(stack, end="", file=file)
    print(out.strip(), file=file)
    if suffix:
        print(suffix, end="", file=file)
    if time:
        print(f"Time: {delta}s", file=file)
    if tb:
        raise RuntimeError("pp")
    return ret


builtins.pp = pp
builtins.op = objprint.op

pytest_plugins = ["spinta.testing.pytest"]

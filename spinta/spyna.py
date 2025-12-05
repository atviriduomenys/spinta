from __future__ import annotations

import functools
from typing import List
from typing import Optional
from typing import TypedDict

import lark

from spinta.utils.schema import NA

GRAMMAR = r"""
?start: testlist
?testlist: test ("," test)* [","]
?test: or
?or: and (("|" | "&_or.") and)*
?and: not (("&" | "&_and.") not)*
?not: "!" not | comp
?comp: expr (COMP expr)*
?expr: term (TERM term)*
?term: factor (FACTOR factor)*
?factor: SIGN factor | composition
?composition: atom trailer*
?specialarg: expr                
specialarglist: specialarg ("," specialarg)* [","]
countfunc: COUNT
limitfunc: LIMIT "=" specialarglist

COUNT: "_count"
SELECT: "_select"
SORT: "_sort"
LIMIT: "_limit"

sortfunc: SORT "=" specialarglist
selectfunc: SELECT "=" specialarglist
?atom: "(" group? ")" | "[" list? "]" | func | limitfunc | countfunc | selectfunc | sortfunc| value | name
group: test ("," test)* [","]
list: test ("," test)* [","]
?trailer: "[" filter? "]" | method | attr | gtmethod | gemethod | ltmethod | lemethod | swmethod | comethod

gtmethod: "." "_gt" "=" expr
gemethod: "." "_ge" "=" expr
ltmethod: "." "_lt" "=" expr
lemethod: "." "_le" "=" expr
swmethod: "." "_sw" "=" expr
comethod: "." "_co" "=" expr

func: NAME call
method: "." NAME call
?call: "(" arglist? ")"
arglist: argument ("," argument)*  [","]
?argument: test | kwarg
kwarg: NAME ":" test
filter: test ("," test)* [","]
attr: "." NAME | "@" NAME
name: NAME
value: NULL | BOOL | INT | FLOAT | STRING | ALL

COMP: ">=" | "<=" | "!=" | "=" | "<" | ">"
TERM: "+" | "-"
FACTOR: "*" | "/" | "%"
SIGN: "+" | "-"
      
NAME: /[a-z_][a-z0-9_]*/i

ALL: "*"
STRING: /"(?!"").*?(?<!\\)(\\\\)*?"|'(?!'').*?(?<!\\)(\\\\)*?'/i
FLOAT: /(\d+)?\.\d+/
INT: /0|[1-9]\d*/
BOOL: "false" | "true"
NULL: "null"

COMMENT: /#[^\n]*/
WS: /[ \t\f\r\n]+/

%ignore WS
%ignore COMMENT
"""

_parser = lark.Lark(GRAMMAR, parser="lalr")


class SpynaAST(TypedDict):
    name: str
    args: List[SpynaAST]


def parse(rql) -> Optional[SpynaAST]:
    if not isinstance(rql, str):
        return rql

    if not rql:
        return None

    if rql.startswith("_id=") and len(rql) == 42:
        # Performance optimization for
        # _id='d6ee6808-6fc8-43eb-b1a5-cfb21c4be906' case.
        _id = rql[5:41]

    elif rql.startswith("eq(_id,") and len(rql) == 47:
        # Performance optimization for
        # eq(_id, 'd6ee6808-6fc8-43eb-b1a5-cfb21c4be906') case.
        _id = rql[9:45]

    else:
        ast = _parser.parse(rql)
        visit = Visitor()
        return visit(ast)

    return {"name": "eq", "args": [{"name": "bind", "args": ["_id"]}, _id]}


class Visitor:
    METHOD_NAMES = {
        "gtmethod": "gt",
        "ltmethod": "lt",
        "gemethod": "ge",
        "lemethod": "le",
        "swmethod": "startswith",
        "comethod": "contains",
    }

    def __call__(self, node):
        if isinstance(node, lark.Tree):
            return getattr(self, node.data, self.__default__)(node, *node.children)
        elif isinstance(node, lark.Token):
            return getattr(self, node.type)(node)
        else:
            return node

    def __default__(self, node, *args):
        if node.data in ("and", "or"):
            return {
                "type": "expression",
                "name": node.data,
                "args": self._args(*args),
            }
        else:
            return {
                "name": node.data,
                "args": self._args(*args),
            }

    def __getattr__(self, name: str) -> dict:
        if name.endswith("_comp"):
            rule_name = name[:-5]
            if rule_name in self.METHOD_NAMES:
                return lambda node, arg, expr: {
                    "type": "method",
                    "name": self.METHOD_NAMES[rule_name],
                    "args": self._args(arg, expr),
                }
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _name(self, node):
        assert node.data == "name", node
        return [c.value for c in node.children]

    def _args(self, *args):
        return [self(arg) for arg in args]

    def comp(self, node, left, op, right):
        ops = {
            "=": "eq",
            "!=": "ne",
            "<": "lt",
            "<=": "le",
            ">": "gt",
            ">=": "ge",
        }
        return {
            "type": "expression",
            "name": ops[op.value],
            "args": self._args(left, right),
        }

    def name(self, node, token):
        if token.value in ("null", "false", "true"):
            return self._const(token.value)
        return {
            "name": "bind",
            "args": [token.value],
        }

    def attr(self, node, name):
        return {
            "name": "bind",
            "args": [name.value],
        }

    def NAME(self, token):
        return {
            "name": "bind",
            "args": [token.value],
        }

    def kwarg(self, node, name, value):
        return {
            "name": "bind",
            "args": [name.value, self(value)],
        }

    def value(self, node, token):
        if token.type == "STRING":
            # Escape characters like "\"", etc
            escaped = token.value[1:-1].encode("utf-8").decode("unicode-escape")
            # When we escape, we get back Unicode encoding which does not support special characters (like 'ą')
            # We need to bridge it to another encoding, which can restore bytes (like `latin1`, it directly maps first 256
            # characters to Unicode code points) and decode it back to `utf-8`.
            return escaped.encode("latin1").decode("utf-8")
        if token.type == "INT":
            return int(token.value)
        if token.type == "FLOAT":
            return float(token.value)
        if token.type == "NULL":
            return None
        if token.type == "BOOL":
            return self._const(token.value)
        if token.type == "ALL":
            return {
                "name": "op",
                "args": ["*"],
            }
        raise Exception(f"Unknown token type: {token.type}")

    def _const(self, name: str):
        return {
            "null": None,
            "false": False,
            "true": True,
        }[name]

    def func(self, node, name, args):
        return {
            "name": name.value,
            "args": self._args(*args.children),
        }

    def countfunc(self, _, __) -> dict:
        return {
            "name": "count",
            "args": [],
        }

    def selectfunc(self, _, __, args: lark.Tree) -> dict:
        return {"name": "select", "args": self._args(*args.children)}

    def sortfunc(self, _, __, args: lark.Tree) -> dict:
        return {"name": "sort", "args": self._args(*args.children)}

    def limitfunc(self, _, __, args: lark.Tree) -> dict:
        return {"name": "limit", "args": self._args(*args.children)}

    def gtmethod_comp(self, _, arg: dict, expr: lark.Tree) -> dict:
        return {
            "type": "method",
            "name": "gt",
            "args": self._args(arg, expr),
        }

    def method_comp(self, _, arg: dict, name: lark.lexer.Token, args: lark.tree.Tree) -> dict:
        return {
            "type": "method",
            "name": name.value,
            "args": self._args(arg, *args.children),
        }

    def attr_comp(self, _, arg: dict, name: lark.lexer.Token) -> dict:
        return {
            "name": "getattr",
            "args": self._args(arg, name),
        }

    def filter_comp(self, _, arg: dict, *args: lark.tree.Tree) -> dict:
        return {
            "name": "filter",
            "args": self._args(arg, self._args(*args)),
        }

    def composition(self, node, *args):
        res = None
        for arg in args:
            if res is None:
                res = arg
            else:
                handler = getattr(self, arg.data + "_comp")
                res = handler(arg, res, *arg.children)
        return res

    def factor(self, node, sign, expr):
        names = {
            "+": "positive",
            "-": "negative",
        }
        return {
            "name": names[sign.value],
            "args": [self(expr)],
        }

    def expr(self, node, *args):
        names = {
            "+": "add",
            "-": "sub",
        }
        args = iter(args)
        left = self(next(args))
        for term in args:
            right = self(next(args))
            left = {
                "name": names[term.value],
                "args": [left, right],
            }
        return left

    def term(self, node, *args):
        names = {
            "*": "mul",
            "/": "div",
            "%": "mod",
        }
        args = iter(args)
        left = self(next(args))
        for term in args:
            right = self(next(args))
            left = {
                "name": names[term.value],
                "args": [left, right],
            }
        return left


def unparse(rql, *, pretty=False, raw=False):
    if rql is NA:
        return "<NA>"
    if rql is None:
        return "null"
    if rql is True:
        return "true"
    if rql is False:
        return "false"
    if not isinstance(rql, dict) or not set(rql) >= {"name", "args"}:
        return repr(rql)

    ops = {
        "eq": "=",
        "ne": "!=",
        "lt": "<",
        "le": "<=",
        "gt": ">",
        "ge": ">=",
        "and": "&",
        "or": "|",
    }

    typ = rql.get("type")
    name = rql["name"]
    _unparse = functools.partial(unparse, raw=raw)

    if not raw:
        if typ == "expression":
            op = ops[name]
            args = (_unparse(arg) for arg in rql["args"])
            return op.join(args)

        if name == "bind":
            if len(rql["args"]) == 2:
                bind, value = rql["args"]
                return bind + ": " + _unparse(value)
            else:
                (bind,) = rql["args"]
                return bind

        if name == "getattr":
            obj, key = rql["args"]
            return _unparse(obj) + "." + _unparse(key)

        if name == "filter":
            obj, group = rql["args"]
            return _unparse(obj) + "[" + ", ".join(_unparse(arg) for arg in group) + "]"

        if name == "positive":
            (arg,) = rql["args"]
            return "+" + _unparse(arg)

        if name == "negative":
            (arg,) = rql["args"]
            return "-" + _unparse(arg)

        if name == "testlist":
            return ", ".join(_unparse(arg) for arg in rql["args"])

        if name == "group":
            return "(" + ", ".join(_unparse(arg) for arg in rql["args"]) + ")"

        if name == "list":
            return "[" + ", ".join(_unparse(arg) for arg in rql["args"]) + "]"

        if name == "op":
            return rql["args"][0]

        if name in ("add", "sub", "mul", "div", "mod"):
            symbols = {
                "add": "+",
                "sub": "-",
                "mul": "*",
                "div": "/",
                "mod": "%",
            }
            symbol = symbols[name]
            left, right = rql["args"]
            return _unparse(left) + f" {symbol} " + _unparse(right)

    args = [_unparse(arg) for arg in rql["args"]]
    if not raw and typ == "method":
        attr, args = args[0], args[1:]
        name = f"{attr}.{name}"
    sig = ", ".join(args)
    if pretty and len(sig) > 42 and len(args) > 1:
        sig = "\n    " + ",\n    ".join(args) + "\n"
    return f"{name}({sig})"

from __future__ import annotations

import functools
from typing import List
from typing import Optional
from typing import TypedDict

import lark

from spinta.utils.schema import NA

GRAMMAR = r'''
?start: testlist
?testlist: test ("," test)* [","]
?test: or
?or: and ("|" and)*
?and: not ("&" not)*
?not: "!" not | comp
?comp: expr (COMP expr)*
?expr: term (TERM term)*
?term: factor (FACTOR factor)*
?factor: SIGN factor | composition
?composition: atom trailer*
?atom: "(" group? ")" | "[" list? "]" | func | value | name
group: test ("," test)* [","]
list: test ("," test)* [","]
?trailer: "[" filter? "]" | method | attr
func: NAME call
method: "." NAME call
?call: "(" arglist? ")"
arglist: argument ("," argument)*  [","]
?argument: test | kwarg
kwarg: NAME ":" test
filter: test ("," test)* [","]
attr: "." NAME
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
'''

_parser = lark.Lark(GRAMMAR, parser='lalr')


class SpynaAST(TypedDict):
    name: str
    args: List[SpynaAST]


def parse(rql) -> Optional[SpynaAST]:
    if not isinstance(rql, str):
        return rql

    if not rql:
        return None

    if rql.startswith('_id=') and len(rql) == 42:
        # Performance optimization for
        # _id='d6ee6808-6fc8-43eb-b1a5-cfb21c4be906' case.
        _id = rql[5:41]

    elif rql.startswith('eq(_id,') and len(rql) == 47:
        # Performance optimization for
        # eq(_id, 'd6ee6808-6fc8-43eb-b1a5-cfb21c4be906') case.
        _id = rql[9:45]

    else:
        ast = _parser.parse(rql)
        visit = Visitor()
        return visit(ast)

    return {'name': 'eq', 'args': [{'name': 'bind', 'args': ['_id']}, _id]}


class Visitor:

    def __call__(self, node):
        if isinstance(node, lark.Tree):
            return getattr(self, node.data, self.__default__)(node, *node.children)
        elif isinstance(node, lark.Token):
            return getattr(self, node.type)(node)
        else:
            return node

    def __default__(self, node, *args):
        if node.data in ('and', 'or'):
            return {
                'type': 'expression',
                'name': node.data,
                'args': self._args(*args),
            }
        else:
            return {
                'name': node.data,
                'args': self._args(*args),
            }

    def _name(self, node):
        assert node.data == 'name', node
        return [c.value for c in node.children]

    def _args(self, *args):
        return [self(arg) for arg in args]

    def comp(self, node, left, op, right):
        ops = {
            '=': 'eq',
            '!=': 'ne',
            '<': 'lt',
            '<=': 'le',
            '>': 'gt',
            '>=': 'ge',
        }
        return {
            'type': 'expression',
            'name': ops[op.value],
            'args': self._args(left, right),
        }

    def name(self, node, token):
        if token.value in ('null', 'false', 'true'):
            return self._const(token.value)
        return {
            'name': 'bind',
            'args': [token.value],
        }

    def attr(self, node, name):
        return {
            'name': 'bind',
            'args': [name.value],
        }

    def NAME(self, token):
        return {
            'name': 'bind',
            'args': [token.value],
        }

    def kwarg(self, node, name, value):
        return {
            'name': 'bind',
            'args': [name.value, self(value)],
        }

    def value(self, node, token):
        if token.type == 'STRING':
            return token.value[1:-1]
        if token.type == 'INT':
            return int(token.value)
        if token.type == 'FLOAT':
            return float(token.value)
        if token.type == 'NULL':
            return None
        if token.type == 'BOOL':
            return self._const(token.value)
        if token.type == 'ALL':
            return {
                'name': 'op',
                'args': ['*'],
            }
        raise Exception(f"Unknown token type: {token.type}")

    def _const(self, name: str):
        return {
            'null': None,
            'false': False,
            'true': True,
        }[name]

    def func(self, node, name, args):
        return {
            'name': name.value,
            'args': self._args(*args.children),
        }

    def method_comp(self, node, arg, name, args):
        return {
            'type': 'method',
            'name': name.value,
            'args': self._args(arg, *args.children),
        }

    def attr_comp(self, node, arg, name):
        return {
            'name': 'getattr',
            'args': self._args(arg, name),
        }

    def filter_comp(self, node, arg, *args):
        return {
            'name': 'filter',
            'args': self._args(arg, self._args(*args)),
        }

    def composition(self, node, *args):
        res = None
        for arg in args:
            if res is None:
                res = arg
            else:
                handler = getattr(self, arg.data + '_comp')
                res = handler(arg, res, *arg.children)
        return res

    def factor(self, node, sign, expr):
        names = {
            '+': 'positive',
            '-': 'negative',
        }
        return {
            'name': names[sign.value],
            'args': [self(expr)],
        }

    def expr(self, node, *args):
        names = {
            '+': 'add',
            '-': 'sub',
        }
        args = iter(args)
        left = self(next(args))
        for term in args:
            right = self(next(args))
            left = {
                'name': names[term.value],
                'args': [left, right],
            }
        return left

    def term(self, node, *args):
        names = {
            '*': 'mul',
            '/': 'div',
            '%': 'mod',
        }
        args = iter(args)
        left = self(next(args))
        for term in args:
            right = self(next(args))
            left = {
                'name': names[term.value],
                'args': [left, right],
            }
        return left


def unparse(rql, *, pretty=False, raw=False):
    if rql is NA:
        return '<NA>'
    if rql is None:
        return 'null'
    if rql is True:
        return 'true'
    if rql is False:
        return 'false'
    if not isinstance(rql, dict) or not set(rql) >= {'name', 'args'}:
        return repr(rql)

    ops = {
        'eq': '=',
        'ne': '!=',
        'lt': '<',
        'le': '<=',
        'gt': '>',
        'ge': '>=',
        'and': '&',
        'or': '|',
    }

    typ = rql.get('type')
    name = rql['name']
    _unparse = functools.partial(unparse, raw=raw)

    if not raw:
        if typ == 'expression':
            op = ops[name]
            args = (_unparse(arg) for arg in rql['args'])
            return op.join(args)

        if name == 'bind':
            if len(rql['args']) == 2:
                bind, value = rql['args']
                return bind + ': ' + _unparse(value)
            else:
                bind, = rql['args']
                return bind

        if name == 'getattr':
            obj, key = rql['args']
            return _unparse(obj) + '.' + _unparse(key)

        if name == 'filter':
            obj, group = rql['args']
            return _unparse(obj) + '[' + ', '.join(_unparse(arg) for arg in group) + ']'

        if name == 'positive':
            arg, = rql['args']
            return '+' + _unparse(arg)

        if name == 'negative':
            arg, = rql['args']
            return '-' + _unparse(arg)

        if name == 'testlist':
            return ', '.join(_unparse(arg) for arg in rql['args'])

        if name == 'group':
            return '(' + ', '.join(_unparse(arg) for arg in rql['args']) + ')'

        if name == 'list':
            return '[' + ', '.join(_unparse(arg) for arg in rql['args']) + ']'

        if name == 'op':
            return rql['args'][0]

        if name in ('add', 'sub', 'mul', 'div', 'mod'):
            symbols = {
                'add': '+',
                'sub': '-',
                'mul': '*',
                'div': '/',
                'mod': '%',
            }
            symbol = symbols[name]
            left, right = rql['args']
            return _unparse(left) + f' {symbol} ' + _unparse(right)

    args = [_unparse(arg) for arg in rql['args']]
    if not raw and typ == 'method':
        attr, args = args[0], args[1:]
        name = f'{attr}.{name}'
    sig = ', '.join(args)
    if pretty and len(sig) > 42 and len(args) > 1:
        sig = '\n    ' + ',\n    '.join(args) + '\n'
    return f'{name}({sig})'

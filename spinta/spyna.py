import lark


GRAMMAR = r'''
?start: test
?test: or
?or: and ("|" and)*
?and: not ("&" not)*
?not: "!" not | comp
?comp: expr (COMP expr)*
?expr: term (TERM term)*
?term: factor (FACTOR factor)*
?factor: SIGN factor | composition
?composition: atom trailer*
?atom: "(" group? ")" | func | name | value
group: test ("," test)* [","]
?trailer: "[" filter? "]" | method | attr
func: NAME call
method: "." NAME call
?call: "(" arglist? ")"
arglist: argument ("," argument)*  [","]
?argument: test | kwarg
kwarg: NAME ":" test
filter: test
attr: "." NAME
name: NAME
value: "null" | BOOL | INT | FLOAT | STRING

COMP: ">=" | "<=" | "!=" | "=" | "<" | ">"
TERM: "+" | "-"
FACTOR: "*" | "/" | "%"
SIGN: "+" | "-"

NAME: /[a-z][a-z0-9_]*/i
STRING : /"(?!"").*?(?<!\\)(\\\\)*?"|'(?!'').*?(?<!\\)(\\\\)*?'/i
INT: /0|[1-9]\d*/
FLOAT: /\d+(\.\d+)?/
BOOL: "false" | "true"

COMMENT: /#[^\n]*/
WS: /[ \t\f\r\n]+/

%ignore WS
%ignore COMMENT
'''


def parse(rql):
    parser = lark.Lark(GRAMMAR)
    ast = parser.parse(rql)
    visit = Visitor()
    return visit(ast)


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

    def name(self, node, name):
        return {
            'name': 'bind',
            'args': [name.value],
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
            return int(token)
        if token.type == 'FLOAT':
            return float(token)
        raise Exception("Unknown token type: {token.type}")

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

    def filter_comp(self, node, arg, filter_):
        return {
            'name': 'filter',
            'args': self._args(arg, filter_),
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


def unparse(rql):
    if not isinstance(rql, dict):
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

    if typ == 'expression':
        op = ops[name]
        args = (unparse(arg) for arg in rql['args'])
        return op.join(args)

    if name == 'bind':
        if len(rql['args']) == 2:
            bind, value = rql['args']
            return bind + ': ' + unparse(value)
        else:
            bind, = rql['args']
            return bind

    if name == 'getattr':
        obj, key = rql['args']
        return unparse(obj) + '.' + unparse(key)

    if name == 'filter':
        obj, filter_ = rql['args']
        return unparse(obj) + '[' + unparse(filter_) + ']'

    if name == 'positive':
        arg, = rql['args']
        return '+' + unparse(arg)

    if name == 'negative':
        arg, = rql['args']
        return '-' + unparse(arg)

    if name == 'group':
        return '(' + ', '.join(unparse(arg) for arg in rql['args']) + ')'

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
        return unparse(left) + f' {symbol} ' + unparse(right)

    args = [unparse(arg) for arg in rql['args']]
    if typ == 'method':
        attr, args = args[0], args[1:]
        name = f'{attr}.{name}'
    sig = ', '.join(args)
    return f'{name}({sig})'

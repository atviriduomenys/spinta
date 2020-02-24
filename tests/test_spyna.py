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
?atom: "(" group? ")" | func | filter | name | value
?group: expr ("," expr)* [","]
?trailer: method | filter | attr
func: NAME call
method: "." NAME call
?call: "(" arglist? ")"
?arglist: argument ("," argument)*  [","]
?argument: test | NAME ":" test
filter: "[" test? "]"
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
    print(ast.pretty())
    visit = Visitor()
    return visit(ast)


class Visitor:

    def __call__(self, node):
        if isinstance(node, lark.Tree):
            return getattr(self, node.data, self.__default__)(node, *node.children)
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

    def unary(self, node, sign, name):
        return {
            'name': 'name',
            'args': self._name(name),
            'sign': sign.value,
        }

    def name(self, node, name):
        return {
            'name': 'name',
            'args': [name.value],
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

    def composition(self, node, *args):
        res = {'args': []}
        for arg in reversed(args):
            if arg.data == 'method':
                return {
                    'type': 'method',
                    'name': args[-1].children[0].value,
                    'args': self._args(atom, *trailer.children[1:]),
                }
        return res['args'][0]


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

    if typ == 'expression':
        op = ops[rql['name']]
        args = (unparse(arg) for arg in rql['args'])
        return op.join(args)

    if rql['name'] == 'name':
        name = '.'.join(rql['args'])
        if 'value' in rql:
            return name + ': ' + unparse(rql['value'])
        else:
            sign = rql.get('sign', '')
            return sign + name

    name = rql['name']
    args = [unparse(arg) for arg in rql['args']]
    if typ == 'method':
        attr, args = args[0], args[1:]
        name = f'{attr}.{name}'
    sig = ', '.join(args)
    return f'{name}({sig})'


def check(rql):
    ast = parse(rql)
    pp(rql, ast)
    assert unparse(ast) == rql


def test_eq():
    check("foo='bar'")


def test_ne():
    check("foo!='bar'")


def test_gt():
    check("foo>42")


def test_lt():
    check("foo<42")


def test_ge():
    check("foo>=42")


def test_le():
    check("foo<=42")


def test_contains():
    check("contains(foo, 'bar')")


def test_startswith():
    check("foo.startswith('bar')")


def test_startswith_nested_name():
    check("foo.bar.startswith('baz')")


def test_startswith_call():
    check("startswith(foo.bar, 'baz')")


def test_chained_call():
    check("this.strip().startswith('baz')")


def test_nested_name():
    check("foo.bar.baz=42")


def test_sort():
    check("sort(+foo, -bar, baz)")


def test_sort_nested():
    check("sort(+foo.bar.baz)")


def test_and():
    check('a=1&b=2&c=3')


def test_and_and_or():
    check('a=1&b=2|c=3')


def test_or_and_and():
    check('a=1|b=2&c=3')


def test_empty_string():
    check("a=''")


def test_kwargs():
    check("select(foo: 'bar')")


def test_args_and_kwargs():
    check("select(foo, bar: 42, baz)")

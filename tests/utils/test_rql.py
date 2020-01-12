import lark

GRAMMAR = r'''
?start: test

?test: or
?or: and ("|" and)*
?and: compare ("&" compare)*
?compare: expr (op expr)*
?expr: group | chain | name | value
op: OP
?group: "(" test ")"
?chain: call ("." call)*
call: name "(" [args] ")"
name: NAME ("." NAME)*
args: arg ("," arg)*
?arg: test

_value: VALUE | ESCAPE
value: _value+

OP: "=" | "!=" | ">" | "<" | ">=" | "<="
VALUE: /[^=!<>():&|,.]+/
ESCAPE: /\\./
NAME: /:?[a-z][a-z0-9_]*/i

%import common.WS
%ignore WS
'''


def parse(rql):
    p = lark.Lark(GRAMMAR)
    print(p.parse('a(42, :this).b(lower(:foo.bar.baz.ok()))=1=ok()=4&b>2|c=3&x').pretty())
    print(p.parse('((a=1|b=2)&c=3)').pretty())
    assert False


def unparse(rql):
    if not isinstance(rql, dict):
        return repr(rql)

    ops = {
        'eq': '==',
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

    if rql['name'] == 'bind':
        return rql['kwargs'].get('sign', '') + rql['args'][0]

    name = rql['name']
    args = [unparse(arg) for arg in rql['args']]
    kwargs = [k + '=' + unparse(v) for k, v in rql['kwargs'].items()]
    if typ == 'method':
        attr, args = args[0], args[1:]
        name = f'{attr}.{name}'
    sig = ','.join(args + kwargs)
    return f'{name}({sig})'


def check(rql):
    prql = parse(rql)
    assert unparse(prql) == rql


def test_eq():
    check("foo=='bar'")


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
    check("contains(foo,'bar')")


def test_startswith():
    check("foo.startswith('bar')")


def test_startswith_nested_name():
    check("foo.bar.startswith('baz')")


def test_startswith_call():
    check("startswith(foo.bar,'baz')")


def test_nested_name():
    check("foo.bar.baz==42")


def test_sort():
    check("sort(+foo,-bar,baz)")


def test_sort_nested():
    check("sort(+foo.bar.baz)")


def test_and():
    check('a==1&b==2&c==3')


def test_and_and_or():
    check('a==1&b==2|c==3')


def test_or_and_and():
    check('a==1|b==2&c==3')

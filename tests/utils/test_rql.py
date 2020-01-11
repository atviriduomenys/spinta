import ast


def is_bitwise(node):
    return (
        isinstance(node, ast.BinOp) and
        isinstance(node.op, (ast.BitAnd, ast.BitOr))
    )


def rewrite_bitwise_ops(node: ast.Compare):
    res = [
        ast.Compare(left=node.left, ops=[], comparators=[])
    ]
    for op, comp in zip(node.ops, node.comparators):
        if (
            isinstance(comp, ast.BinOp) and
            isinstance(comp.op, (ast.BitAnd, ast.BitOr))
        ):
            res[-1].ops.append(op)
            res[-1].comparators.append(comp.left)
            res.append(
                ast.Compare(left=comp.right, ops=[], comparators=[])
            )
        else:
            res[-1].ops.append(op)
            res[-1].comparators.append(comp)
    if len(res) == 1:
        return res[0]
    else:
        return ast.BoolOp(
            op=ast.And(),
            values=res,
        )


class Parse:

    def __init__(self, rql):
        tree = ast.parse(rql, mode='eval')
        self.result = self.parse(tree)

    def parse(self, node):
        if isinstance(node, ast.Compare):
            node = rewrite_bitwise_ops(node)

        if isinstance(node, ast.Expression):
            return self.parse(node.body)

        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Str):
            return node.s

        if isinstance(node, ast.Num):
            return node.n

        if isinstance(node, (ast.Name, ast.Attribute)):
            return self.parse_bind(node)

        if isinstance(node, ast.UnaryOp):
            ops = {
                ast.UAdd: '+',
                ast.USub: '-',
            }
            if type(node.op) not in ops:
                raise Exception("Don't know how to handle %s." % ast.dump(node))
            return self.parse_bind(node.operand, sign=ops[type(node.op)])

        if isinstance(node, ast.BoolOp):
            ops = {
                ast.And: 'and',
                ast.Or: 'or',
            }
            if type(node.op) not in ops:
                raise Exception("Don't know how to handle %s." % ast.dump(node))
            name = ops[type(node.op)]
            return {
                'name': name,
                'args': [self.parse(value) for value in node.values],
                'kwargs': {},
            }

        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            ops = {
                ast.Eq: 'eq',
                ast.NotEq: 'ne',
                ast.Gt: 'gt',
                ast.Lt: 'lt',
                ast.GtE: 'ge',
                ast.LtE: 'le',
            }
            op = node.ops[0]
            left = node.left
            right = node.comparators[0]
            if type(op) not in ops:
                raise Exception("Don't know how to handle %s." % ast.dump(node))
            name = ops[type(op)]
            return {
                'name': name,
                'args': [
                    self.parse(left),
                    self.parse(right),
                ],
                'kwargs': {},
            }

        if isinstance(node, ast.Call):

            if isinstance(node.func, ast.Attribute):
                name = node.func.attr
                args = [self.parse(node.func.value)]
            elif isinstance(node.func, ast.Name):
                name = node.func.id
                args = []
            else:
                raise Exception("Don't know how to handle %s." % ast.dump(node))

            return {
                'name': name,
                'args': args + [self.parse(arg) for arg in node.args],
                'kwargs': {},
            }

        raise Exception("Don't know how to handle %s." % ast.dump(node))

    def parse_bind(self, node: ast.AST, **kwargs):
        return {
            'name': 'bind',
            'args': [self.parse_bind_name(node)],
            'kwargs': kwargs,
        }

    def parse_bind_name(self, node: ast.AST):
        if isinstance(node, ast.Name):
            return node.id

        if isinstance(node, ast.Attribute):
            return self.parse_bind_name(node.value) + '.' + node.attr

        raise Exception("Don't know how to handle %s." % ast.dump(node))


def parse(rql):
    return Parse(rql).result


def test_eq():
    assert parse('foo == "bar"') == {
        'name': 'eq',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            'bar',
        ],
        'kwargs': {},
    }


def test_ne():
    assert parse('foo != "bar"') == {
        'name': 'ne',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            'bar',
        ],
        'kwargs': {},
    }


def test_gt():
    assert parse('foo > 42') == {
        'name': 'gt',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            42,
        ],
        'kwargs': {},
    }


def test_lt():
    assert parse('foo < 42') == {
        'name': 'lt',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            42,
        ],
        'kwargs': {},
    }


def test_ge():
    assert parse('foo >= 42') == {
        'name': 'ge',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            42,
        ],
        'kwargs': {},
    }


def test_le():
    assert parse('foo <= 42') == {
        'name': 'le',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            42,
        ],
        'kwargs': {},
    }


def test_contains():
    assert parse('contains(foo, "bar")') == {
        'name': 'contains',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            'bar',
        ],
        'kwargs': {},
    }


def test_startswith():
    assert parse('foo.startswith("bar")') == {
        'name': 'startswith',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {}},
            'bar',
        ],
        'kwargs': {},
    }


def test_startswith_nested_name():
    assert parse('foo.bar.startswith("baz")') == {
        'name': 'startswith',
        'args': [
            {'name': 'bind', 'args': ['foo.bar'], 'kwargs': {}},
            'baz',
        ],
        'kwargs': {},
    }


def test_startswith_call():
    assert parse('startswith(foo.bar, "baz")') == {
        'name': 'startswith',
        'args': [
            {'name': 'bind', 'args': ['foo.bar'], 'kwargs': {}},
            'baz',
        ],
        'kwargs': {},
    }


def test_nested_name():
    assert parse('foo.bar.baz == 42') == {
        'name': 'eq',
        'args': [
            {'name': 'bind', 'args': ['foo.bar.baz'], 'kwargs': {}},
            42,
        ],
        'kwargs': {},
    }


def test_sort():
    assert parse('sort(+foo, -bar, baz)') == {
        'name': 'sort',
        'args': [
            {'name': 'bind', 'args': ['foo'], 'kwargs': {'sign': '+'}},
            {'name': 'bind', 'args': ['bar'], 'kwargs': {'sign': '-'}},
            {'name': 'bind', 'args': ['baz'], 'kwargs': {}},
        ],
        'kwargs': {},
    }


def test_sort_nested():
    assert parse('sort(+foo.bar.baz)') == {
        'name': 'sort',
        'args': [
            {'name': 'bind', 'args': ['foo.bar.baz'], 'kwargs': {'sign': '+'}},
        ],
        'kwargs': {},
    }


def test_and():
    assert parse('a==1&b==2&c==3') == {
        'name': 'and',
        'args': [
            {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['a'], 'kwargs': {}},
                    1,
                ],
                'kwargs': {},
            },
            {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['b'], 'kwargs': {}},
                    2,
                ],
                'kwargs': {},
            },
            {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['c'], 'kwargs': {}},
                    3,
                ],
                'kwargs': {},
            },
        ],
        'kwargs': {},
    }


"""
Compare(
    left=Name(id='a', ctx=Load()),
    ops=[Eq(), Eq()],
    comparators=[
        BinOp(
            left=Constant(value=1, kind=None),
            op=BitAnd(),
            right=Name(id='b', ctx=Load()),
        ),
        Constant(value=2, kind=None),
    ]
)

Compare(
    left=Name(id='a', ctx=Load()),
    ops=[Eq(), Eq(), Eq()],
    comparators=[
        BinOp(
            left=Constant(value=1, kind=None),
            op=BitAnd(),
            right=Name(id='b', ctx=Load()),
        ),
        BinOp(
            left=Constant(value=2, kind=None),
            op=BitAnd(),
            right=Name(id='c', ctx=Load()),
        ),
        Constant(value=3, kind=None),
    ],
)



BoolOp(
    op=And(),
    values=[
        Compare(
            left=Name(id='a', ctx=Load()),
            ops=[Eq()],
            comparators=[Constant(value=1, kind=None)],
        ),
        Compare(
            left=Name(id='b', ctx=Load()),
            ops=[Eq()],
            comparators=[Constant(value=2, kind=None)],
        ),
        Compare(
            left=Name(id='c', ctx=Load()),
            ops=[Eq()],
            comparators=[Constant(value=3, kind=None)],
        ),
    ],
)


and -> ?
  & -> and
?   -> &



"""


def dump(node, annotate_fields=True, include_attributes=False, *, indent=None):
    """
    Return a formatted dump of the tree in node.  This is mainly useful for
    debugging purposes.  If annotate_fields is true (by default),
    the returned string will show the names and the values for fields.
    If annotate_fields is false, the result string will be more compact by
    omitting unambiguous field names.  Attributes such as line
    numbers and column offsets are not dumped by default.  If this is wanted,
    include_attributes can be set to true.  If indent is a non-negative
    integer or string, then the tree will be pretty-printed with that indent
    level. None (the default) selects the single line representation.
    """
    def _format(node, level=0):
        if indent is not None:
            level += 1
            prefix = '\n' + indent * level
            sep = ',\n' + indent * level
        else:
            prefix = ''
            sep = ', '
        if isinstance(node, ast.AST):
            args = []
            allsimple = True
            keywords = annotate_fields
            for field in node._fields:
                try:
                    value = getattr(node, field)
                except AttributeError:
                    keywords = True
                else:
                    value, simple = _format(value, level)
                    allsimple = allsimple and simple
                    if keywords:
                        args.append('%s=%s' % (field, value))
                    else:
                        args.append(value)
            if include_attributes and node._attributes:
                for attr in node._attributes:
                    try:
                        value = getattr(node, attr)
                    except AttributeError:
                        pass
                    else:
                        value, simple = _format(value, level)
                        allsimple = allsimple and simple
                        args.append('%s=%s' % (attr, value))
            if allsimple and len(args) <= 3:
                return '%s(%s)' % (node.__class__.__name__, ', '.join(args)), not args
            return '%s(%s%s)' % (node.__class__.__name__, prefix, sep.join(args)), False
        elif isinstance(node, list):
            if not node:
                return '[]', True
            return '[%s%s]' % (prefix, sep.join(_format(x, level)[0] for x in node)), False
        return repr(node), True

    if not isinstance(node, ast.AST):
        raise TypeError('expected AST, got %r' % node.__class__.__name__)
    if indent is not None and not isinstance(indent, str):
        indent = ' ' * indent
    return _format(node)[0]

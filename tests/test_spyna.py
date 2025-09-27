from spinta.core.ufuncs import asttoexpr
from spinta.core.ufuncs import unparse
from spinta.spyna import parse


def check(rql):
    ast = parse(rql)
    expr = asttoexpr(ast)
    assert unparse(expr) == rql, ast


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


def test_contains_lithuanian_letters():
    check("contains(foo, 'ąČ')")


def test_startswith():
    check("foo.startswith('bar')")


def test_startswith_nested_name():
    check("foo.bar.startswith('baz')")


def test_startswith_call():
    check("startswith(foo.bar, 'baz')")


def test_chained_call():
    check("this.strip().startswith('baz')")


def test_chained_func_call():
    check("strip(this).startswith(('baz', 'bar'))")


def test_nested_name():
    check("foo.bar.baz=42")


def test_sort():
    check("sort(+foo, -bar, baz)")


def test_sort_nested():
    check("sort(+foo.bar.baz)")


def test_and():
    check("a=1&b=2&c=3")


def test_and_and_or():
    check("a=1&b=2|c=3")


def test_or_and_and():
    check("a=1|b=2&c=3")


def test_or_and_and_and_group():
    check("(a=1|b=2)&c=3")


def test_empty_string():
    check("a=''")


def test_kwargs():
    check("select(foo: 'bar')")


def test_args_and_kwargs():
    check("select(foo, bar: 42, baz)")


def test_add():
    check("2 + 2")


def test_add_and_subtract():
    check("2 + 2 - 2")


def test_multiply():
    check("2 * 2")


def test_add_and_multiply():
    check("(2 + 2) * 2")


def test_filter():
    check("foo[bar]")


def test_filter_condition():
    check("foo[a=2]")


def test_attr_filter():
    check("foo.bar.baz[a=2]")


def test_filter_group():
    check("foo[bar, baz]")


def test_null():
    check("null")


def test_array():
    check("[1, 'a']")


def test_select_all():
    check("select(*)")


def test_group():
    check("foo, bar, baz")


def test_zero():
    check("0")


def test_negative():
    check("-2")


def test_true():
    check("true")


def test_false():
    check("false")


def test_false_expr():
    ast = parse("false")
    assert ast is False


def test_true_expr():
    ast = parse("true")
    assert ast is True


def test_null_expr():
    ast = parse("null")
    assert ast is None


def test_escale_quotes_one():
    check("swap('TEST', '\"TEST\" \\'TEST\\'')")


def test_escale_quotes_both():
    check("swap('\\'TEST\\' \"TEST\"', '\"TEST\" \\'TEST\\'')")


def test_normal_text():
    check("'TEST'")

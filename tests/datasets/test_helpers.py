import pytest

from spinta import spyna
from spinta.core.ufuncs import asttoexpr
from spinta.core.ufuncs import unparse
from spinta.datasets.helpers import add_is_null_checks


@pytest.mark.parametrize(
    "a, b",
    [
        ("a=42", "a=42"),
        ("a.b", "(a.b=null|a.b)"),
        ("a.b=42", "(a.b=null|a.b=42)"),
        ("a.b=42&x.y=42", "(a.b=null|a.b=42)&(x.y=null|x.y=42)"),
    ],
)
def test_add_is_null_checks(a, b):
    ast = spyna.parse(a)
    expr = asttoexpr(ast)
    expr_ = add_is_null_checks(expr)
    assert unparse(expr_) == b

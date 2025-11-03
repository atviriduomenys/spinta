import pytest

from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Param
from spinta.exceptions import InvalidArgumentInExpression
from spinta.spyna import parse
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.utils.schema import NA


@pytest.mark.parametrize(
    "param_source, param_prepare, result",
    [
        ("foo", 'input("default")', {"foo": "default"}),
        ("foo/bar", 'input("default")', {"foo/bar": "default"}),
        ("foo", 'input("default1", "default2", "default3")', {"foo": "default1"}),
        ("foo", "input()", {"foo": NA}),
    ],
)
def test_input_command_populates_param_soap_body(
    context: Context, param_source: str, param_prepare: str, result: dict
) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this=param_source, param=param)

    expr = asttoexpr(parse(param_prepare))
    env.call("input", expr)

    assert param.soap_body == result


def test_input_command_value_can_be_unresolved_expr(context: Context) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="foo", param=param)

    expr = asttoexpr(parse("foo_func().input('default')"))
    env.call("input", expr)

    assert param.soap_body == {"foo": asttoexpr(parse("foo_func()"))}


def test_header_command_populates_param_http_header(context: Context) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="header_name", param=param)

    expr = asttoexpr(parse('header("header_value")'))
    env.call("header", expr)

    assert param.http_header == {"header_name": "header_value"}


@pytest.mark.parametrize("param_prepare", ["header()", 'header("foo", "bar")'])
def test_header_command_raise_error_if_number_of_header_values(context: Context, param_prepare: str) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="header_name", param=param)

    expr = asttoexpr(parse(param_prepare))

    with pytest.raises(InvalidArgumentInExpression):
        env.call("header", expr)

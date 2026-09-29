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


@pytest.mark.parametrize(
    "param_prepare, result",
    [
        ('header("header_value")', {"header_name": "header_value"}),
        ('creds("foo").header()', {"header_name": asttoexpr(parse('creds("foo")'))}),
    ],
)
def test_header_command_populates_param_http_header(context: Context, param_prepare: str, result: dict) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="header_name", param=param)

    expr = asttoexpr(parse(param_prepare))
    env.call("header", expr)

    assert param.http_header == result


@pytest.mark.parametrize("param_prepare", ["header()", 'header("foo", "bar")'])
def test_header_command_raise_error_if_number_of_header_values_not_one(context: Context, param_prepare: str) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="header_name", param=param)

    expr = asttoexpr(parse(param_prepare))

    with pytest.raises(InvalidArgumentInExpression):
        env.call("header", expr)


@pytest.mark.parametrize("param_prepare", ["cdata().input()", "input().cdata()", "cdata()"])
def test_cdata_command_sets_param_soap_body_value_type(context: Context, param_prepare: str) -> None:
    param = Param()

    env = LoadBuilder(context)
    env.update(this="source", param=param)

    expr = asttoexpr(parse(param_prepare))
    env.resolve(expr)

    assert param.soap_body_value_type == "cdata"

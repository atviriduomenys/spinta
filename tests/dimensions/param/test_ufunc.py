import pytest

from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.dimensions.param.components import ParamBuilder, ResolvedResourceParam
from spinta.spyna import parse


@pytest.mark.parametrize(
    "param_source, param_prepare, result",
    [
        ("foo", 'input("default")', ResolvedResourceParam(target="target", source="foo", value="default")),
        ("foo/bar", 'input("default")', ResolvedResourceParam(target="target", source="foo/bar", value="default")),
        (
            "foo",
            'input("default1", "default2", "default3")',
            ResolvedResourceParam(target="target", source="foo", value="default1"),
        ),
        ("foo", 'input()', ResolvedResourceParam(target="target", source="foo", value=None)),
    ]
)
def test_input_command_returns_dict(context: Context, param_source: str, param_prepare: str, result: dict) -> None:
    env = ParamBuilder(context)
    env.update(this=param_source, target_param="target")

    expr = asttoexpr(parse(param_prepare))
    assert env.call("input", expr) == result

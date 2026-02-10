import pytest

from spinta.components import Context
from spinta.components import UrlParams
from spinta.components import Version
from spinta.commands import prepare
from spinta.exceptions import InvalidValue
from spinta.testing.request import make_get_request


def _parse(
    context: Context,
    query: str,
    accept="application/json",
) -> UrlParams:
    request = make_get_request("", query, {"Accept": accept})
    return prepare(context, UrlParams(), Version(), request)


def test_format(context):
    assert _parse(context, "format(csv,width(42))").format == "csv"
    assert _parse(context, "format(width(42))").formatparams == {
        "width": 42,
    }
    assert _parse(context, "format(csv,width(42))").formatparams == {
        "width": 42,
    }


def test_limit(context):
    assert _parse(context, "limit(1)").limit == 1
    with pytest.raises(InvalidValue):
        _parse(context, "limit(0)")
    with pytest.raises(InvalidValue):
        _parse(context, "limit(-1)")


@pytest.mark.parametrize(
    "url_query",
    [
        "format(csv,title(%27%3c%3fxml+version%3d%221.0%22+encoding%3d%22UTF-8%22+standalone%3d%22yes%22%3f%3e%27))",
        "format(csv,title(%27%3c%3fxml version%3d%221.0%22 encoding%3d%22UTF-8%22 standalone%3d%22yes%22%3f%3e%27))",
        (
            "format(csv,title(%27%3c%3fxml%20version%3d%221.0%22%20encoding%3d%22UTF-8%22%20"
            "standalone%3d%22yes%22%3f%3e%27))"
        ),
    ],
)
def test_encoded_plus_for(context: Context, url_query: str):
    params_space = _parse(context, url_query)
    assert params_space.formatparams["title"] == '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'

from spinta.components import Context
from spinta.components import UrlParams
from spinta.components import Version
from spinta.commands import prepare
from spinta.testing.request import make_get_request


def _parse(
    context: Context,
    query: str,
    accept='application/json',
) -> UrlParams:
    request = make_get_request('', query, {'Accept': accept})
    return prepare(context, UrlParams(), Version(), request)


def test_format(context):
    assert _parse(context, 'format(csv,width(42))').format == 'csv'
    assert _parse(context, 'format(width(42))').formatparams == {
        'width': 42,
    }
    assert _parse(context, 'format(csv,width(42))').formatparams == {
        'width': 42,
    }

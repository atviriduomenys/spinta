from spinta.utils.url import Operator
from spinta.utils.url import parse_url_path
from spinta.utils.url import build_url_path


def test_parse_url_path(context):
    context.load()

    assert parse_url_path(context, 'foo/bar') == {'path': 'foo/bar'}
    assert parse_url_path(context, 'foo/bar/:dataset/deeply/nested/name/:resource/res') == {'path': 'foo/bar', 'dataset': 'deeply/nested/name', 'resource': 'res'}
    assert parse_url_path(context, 'foo/bar/d12a126e085db85e78379284006d369a8247bfc7/:dataset/vrk/:resource/data') == {
        'path': 'foo/bar',
        'id': 'd12a126e085db85e78379284006d369a8247bfc7',
        'dataset': 'vrk',
        'resource': 'data',
    }
    assert parse_url_path(context, 'foo/bar/:limit/100') == {'path': 'foo/bar', 'limit': 100}


def test_build_url_path(context):
    context.load()

    assert build_url_path({'path': 'foo/bar', 'id': '42'}) == 'foo/bar/42'
    assert build_url_path({'path': 'foo/bar', 'id': '42', 'dataset': 'gov/org', 'resource': 'data'}) == 'foo/bar/42/:dataset/gov/org/:resource/data'


def test_optional_argument(context):
    context.load()

    assert parse_url_path(context, 'foo/bar/:changes') == {'path': 'foo/bar', 'changes': None}
    assert parse_url_path(context, 'foo/bar/:changes/42') == {'path': 'foo/bar', 'changes': 42}
    assert parse_url_path(context, 'foo/bar/:changes/-42') == {'path': 'foo/bar', 'changes': -42}

    assert build_url_path({'path': 'foo/bar', 'changes': None}) == 'foo/bar/:changes'
    assert build_url_path({'path': 'foo/bar', 'changes': 42}) == 'foo/bar/:changes/42'
    assert build_url_path({'path': 'foo/bar', 'changes': -42}) == 'foo/bar/:changes/-42'


def test_sort(context):
    context.load()

    string = 'foo/bar/:sort/a/-b'
    params = {
        'path': 'foo/bar',
        'sort': [
            {'name': 'a', 'ascending': True},
            {'name': 'b', 'ascending': False},
        ],
    }
    assert parse_url_path(context, string) == params
    assert build_url_path(params) == string


def test_id_integer(context):
    context.load()

    string = 'foo/bar/d12a126e085db85e78379284006d369a8247bfc7'
    params = {
        'path': 'foo/bar',
        'id': 'd12a126e085db85e78379284006d369a8247bfc7',
    }
    assert parse_url_path(context, string) == params
    assert build_url_path(params) == string


def test_id_sha1(context):
    context.load()

    string = 'foo/bar/69a33b149af7a7eeb25026c8cdc09187477ffe21'
    params = {
        'path': 'foo/bar',
        'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
    }
    assert parse_url_path(context, string) == params
    assert build_url_path(params) == string


def test_format(context):
    context.load()

    string = 'foo/bar/:format/csv'
    params = {
        'path': 'foo/bar',
        'format': 'csv',
    }
    assert parse_url_path(context, string) == params
    assert build_url_path(params) == string


def test_no_args(context):
    context.load()

    string = 'foo/bar/:count'
    params = {
        'path': 'foo/bar',
        'count': [],
    }
    assert parse_url_path(context, string) == params
    assert build_url_path(params) == string


def test_properties(context):
    context.load()

    assert parse_url_path(context, 'model/bcc3a598-286e-4105-ba12-36e3a2202792/property') == {
        'path': 'model',
        'id': 'bcc3a598-286e-4105-ba12-36e3a2202792',
        'properties': ['property'],
    }

    assert parse_url_path(context, 'model/bcc3a598-286e-4105-ba12-36e3a2202792/p1/p2/p3') == {
        'path': 'model',
        'id': 'bcc3a598-286e-4105-ba12-36e3a2202792',
        'properties': ['p1', 'p2', 'p3'],
    }

    assert parse_url_path(context, 'model/bcc3a598-286e-4105-ba12-36e3a2202792') == {
        'path': 'model',
        'id': 'bcc3a598-286e-4105-ba12-36e3a2202792',
    }

    assert parse_url_path(context, 'model') == {
        'path': 'model',
    }

    assert parse_url_path(context, 'model/bcc3a598-286e-4105-ba12-36e3a2202792/:format/csv') == {
        'path': 'model',
        'id': 'bcc3a598-286e-4105-ba12-36e3a2202792',
        'format': 'csv',
    }


def test_query_params(context):
    context.load()

    params = {
        'path': 'model',
        'query_params': [
            {
                'key': 'prop',
                'name': 'exact',
                'operator': Operator.EXACT,
                'value': 'value',
            },
        ],
    }

    assert parse_url_path(context, 'model/:exact/prop/value') == params
    assert build_url_path(params) == 'model/:exact/prop/value'

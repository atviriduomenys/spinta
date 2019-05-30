from spinta.utils.url import parse_url_path
from spinta.utils.url import build_url_path


def test_parse_url_path():
    assert parse_url_path('foo/bar') == {'path': 'foo/bar'}
    assert parse_url_path('foo/bar/:ds/deeply/nested/name/:rs/res') == {'path': 'foo/bar', 'ds': 'deeply/nested/name', 'rs': 'res'}
    assert parse_url_path('foo/bar/3/:ds/vrk/:rs/data') == {'path': 'foo/bar', 'id': {'value': '3', 'type': 'integer'}, 'ds': 'vrk', 'rs': 'data'}
    assert parse_url_path('foo/bar/:limit/100') == {'path': 'foo/bar', 'limit': 100}


def test_build_url_path():
    assert build_url_path({'path': 'foo/bar', 'id': {'value': '42', 'type': 'sha1'}}) == 'foo/bar/42'
    assert build_url_path({'path': 'foo/bar', 'id': {'value': '42', 'type': 'sha1'}, 'ds': 'gov/org', 'rs': 'data'}) == 'foo/bar/42/:ds/gov/org/:rs/data'


def test_optional_argument():
    assert parse_url_path('foo/bar/:changes') == {'path': 'foo/bar', 'changes': None}
    assert parse_url_path('foo/bar/:changes/42') == {'path': 'foo/bar', 'changes': 42}
    assert parse_url_path('foo/bar/:changes/-42') == {'path': 'foo/bar', 'changes': -42}

    assert build_url_path({'path': 'foo/bar', 'changes': None}) == 'foo/bar/:changes'
    assert build_url_path({'path': 'foo/bar', 'changes': 42}) == 'foo/bar/:changes/42'
    assert build_url_path({'path': 'foo/bar', 'changes': -42}) == 'foo/bar/:changes/-42'


def test_sort():
    string = 'foo/bar/:sort/a/-b'
    params = {
        'path': 'foo/bar',
        'sort': [
            {'name': 'a', 'ascending': True},
            {'name': 'b', 'ascending': False},
        ],
    }
    assert parse_url_path(string) == params
    assert build_url_path(params) == string


def test_id_integer():
    string = 'foo/bar/42'
    params = {
        'path': 'foo/bar',
        'id': {'value': '42', 'type': 'integer'},
    }
    assert parse_url_path(string) == params
    assert build_url_path(params) == string


def test_id_sha1():
    string = 'foo/bar/69a33b149af7a7eeb25026c8cdc09187477ffe21'
    params = {
        'path': 'foo/bar',
        'id': {
            'value': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'type': 'sha1',
        },
    }
    assert parse_url_path(string) == params
    assert build_url_path(params) == string


def test_format():
    string = 'foo/bar/:format/csv'
    params = {
        'path': 'foo/bar',
        'format': 'csv',
    }
    assert parse_url_path(string) == params
    assert build_url_path(params) == string


def test_no_args():
    string = 'foo/bar/:count'
    params = {
        'path': 'foo/bar',
        'count': [],
    }
    assert parse_url_path(string) == params
    assert build_url_path(params) == string

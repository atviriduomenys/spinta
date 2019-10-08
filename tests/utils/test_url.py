import pytest

from spinta.utils.url import parse_url_path
from spinta.utils.url import build_url_path


@pytest.mark.parametrize('path, query', [
    ('', [{'name': 'path', 'args': []}]),
    (':dataset/test', [
        {'name': 'dataset', 'args': ['test']},
    ]),
    ('foo/bar', [
        {'name': 'path', 'args': ['foo', 'bar']},
    ]),
    ('foo/bar/:dataset/deeply/nested/name/:resource/res', [
        {'name': 'path', 'args': ['foo', 'bar']},
        {'name': 'dataset', 'args': ['deeply', 'nested', 'name']},
        {'name': 'resource', 'args': ['res']},
    ]),
    ('foo/bar/d12a126e085db85e78379284006d369a8247bfc7/:dataset/vrk/:resource/data', [
        {'name': 'path', 'args': ['foo', 'bar', 'd12a126e085db85e78379284006d369a8247bfc7']},
        {'name': 'dataset', 'args': ['vrk']},
        {'name': 'resource', 'args': ['data']},
    ])
])
def test_parse_url_path(path, query):
    assert parse_url_path(path) == query
    assert build_url_path(query) == path


def test_unknown_name():
    with pytest.raises(Exception) as e:
        parse_url_path('foo/bar/:unknown/name')
    assert str(e.value) == "Unknown request parameter 'unknown'.\n  Context:\n    name: unknown\n"


def test_mingargs():
    with pytest.raises(Exception) as e:
        parse_url_path('foo/bar/:dataset')
    assert str(e.value) == "At least 1 argument is required for 'dataset' URL parameter."


def test_maxgargs():
    with pytest.raises(Exception) as e:
        parse_url_path('foo/bar/:ns/arg')
    assert str(e.value) == "URL parameter 'ns' can only have 0 arguments."

import pytest

from spinta.utils.url import parse_url_path
from spinta.utils.url import build_url_path


@pytest.mark.parametrize(
    "path, query",
    [
        ("", [{"name": "path", "args": []}]),
        (
            "foo/bar",
            [
                {"name": "path", "args": ["foo", "bar"]},
            ],
        ),
        (
            "foo/bar/:format/csv",
            [
                {"name": "path", "args": ["foo", "bar"]},
                {"name": "format", "args": ["csv"]},
            ],
        ),
        (
            "foo/bar/d12a126e085db85e78379284006d369a8247bfc7/:format/csv",
            [
                {"name": "path", "args": ["foo", "bar", "d12a126e085db85e78379284006d369a8247bfc7"]},
                {"name": "format", "args": ["csv"]},
            ],
        ),
    ],
)
def test_parse_url_path(path, query):
    assert parse_url_path(path) == query
    assert build_url_path(query) == path


def test_unknown_name():
    with pytest.raises(Exception) as e:
        parse_url_path("foo/bar/:unknown/name")
    assert str(e.value) == "Unknown request parameter 'unknown'.\n  Context:\n    name: unknown\n"


def test_mingargs():
    with pytest.raises(Exception) as e:
        parse_url_path("foo/bar/:format")
    assert str(e.value) == "At least 1 argument is required for 'format' URL parameter."


def test_maxgargs():
    with pytest.raises(Exception) as e:
        parse_url_path("foo/bar/:ns/arg")
    assert str(e.value) == "URL parameter 'ns' can only have 0 arguments."


def test_parse_url_path_recognizes_custom_scope():
    result = parse_url_path("example/City/@ltu")
    assert result == [
        {"name": "path", "args": ["example", "City"]},
        {"name": "custom-scope", "args": ["ltu"]},
    ]


def test_build_url_path_serializes_custom_scope():
    query = [
        {"name": "path", "args": ["example", "City"]},
        {"name": "custom-scope", "args": ["ltu"]},
    ]
    assert build_url_path(query) == "example/City/@ltu"


def test_custom_scope_round_trip():
    path = "example/City/@ltu"
    assert build_url_path(parse_url_path(path)) == path

from spinta.utils.url import parse_url_path


def test_parse_url_path():
    assert parse_url_path('foo/bar') == {'path': 'foo/bar'}
    assert parse_url_path('foo/bar/3') == {'path': 'foo/bar', 'id': 3}
    assert parse_url_path('foo/bar/:source/vrk') == {'path': 'foo/bar', 'source': 'vrk'}
    assert parse_url_path('foo/bar/3/:source/vrk') == {'path': 'foo/bar', 'id': 3, 'source': 'vrk'}

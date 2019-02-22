import pathlib

from responses import GET


def test_json(store, responses):
    responses.add(
        GET, 'http://example.com/data.json',
        status=200, content_type='application/json; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.json').read_bytes(),
    )

    assert len(store.pull('json')) == 10
    assert list(store.getall('rinkimai/:source/json')) == [
    ]

from spinta.testing.dtypes import path, nest, flat


def test_path():
    assert path('backends/mongo/dtypes/string') == 'string'
    assert path('backends/postgres/dtypes/object/string') == 'object.string'


def test_path_array():
    assert path('backends/postgres/dtypes/array/string') == 'array'
    assert path('backends/postgres/dtypes/array/object/string') == 'array.string'


def test_nest():
    data = {
        '_type': '',
        '_id': '',
        '_revision': '',
        'string': 'test',
    }
    assert nest('backends/mongo/dtypes/string', data) == {
        '_type': '',
        '_id': '',
        '_revision': '',
        'string': 'test',
    }
    assert nest('backends/postgres/dtypes/object/string', data) == {
        '_type': '',
        '_id': '',
        '_revision': '',
        'object': {
            'string': 'test',
        }
    }


def test_nest_array():
    data = {
        '_type': '',
        '_id': '',
        '_revision': '',
        'string': 'test',
    }
    assert nest('backends/postgres/dtypes/array/string', data) == {
        '_type': '',
        '_id': '',
        '_revision': '',
        'array': ['test'],
    }


def test_flat():
    data = {
        '_type': '',
        '_id': '',
        '_revision': '',
        'string': 'test',
    }
    assert flat('backends/postgres/dtypes/string', data) == data
    assert flat('backends/postgres/dtypes/object/string', {
        '_type': '',
        '_id': '',
        '_revision': '',
        'object': {
            'string': 'test',
        }
    }) == data


def test_flat_array():
    data = {
        '_type': '',
        '_id': '',
        '_revision': '',
        'string': 'test',
    }
    assert flat('backends/postgres/dtypes/array/string', {
        '_type': '',
        '_id': '',
        '_revision': '',
        'array': ['test'],
    }) == data

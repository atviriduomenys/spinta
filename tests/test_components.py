from typing import Optional


METADATA = {
    'id': None,
    'pk': 'id',
    'revision': None,
    'transaction': None,
    'pos': None,
}


class Data:
    """Small wrapper around data object.

    This wrapper separates metadata from real data.
    """

    def __init__(self, payload: Optional[dict] = None):
        self.__dict__['payload'] = payload or {}

    def __getattr__(self, name):
        if name == 'payload':
            return self.__dict__['payload']
        else:
            name = METADATA[name] or name
            return self.__dict__['payload'].get(f'_{name}')

    def __setattr__(self, name, value):
        assert name != 'payload'
        name = METADATA[name] or name
        self.__dict__['payload'][f'_{name}'] = value

    def __getitem__(self, name):
        return self.__dict__['payload'][name]

    def __setitem__(self, name, value):
        self.__dict__['payload'][name] = value

    def copy(self):
        return Data(self.__dict__['payload'].copy())


def test_data():
    d = Data({
        '_id': 42,
        'foo': 'bar',
    })
    assert d.pk == 42
    assert d.revision is None
    assert d['foo'] == 'bar'

    d.pos = 1
    d['baz'] = 'bar'
    assert d.payload == {
        '_id': 42,
        '_pos': 1,
        'foo': 'bar',
        'baz': 'bar',
    }

    c = d.copy()
    c.pos = 2
    c['baz'] = 'foo'
    assert c.pos == 2
    assert c['baz'] == 'foo'

    assert d.pos == 1
    assert d['baz'] == 'bar'

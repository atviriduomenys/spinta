from spinta import commands
from spinta.components import Context
from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap as KeyMap


def test_scalar():
    context = Context('test')
    kmap = KeyMap('sqlite:///:memory:')
    commands.prepare(context, kmap)
    with kmap:
        # val = 42
        # key = kmap.encode('test', val, None)
        # assert kmap.decode('test', key) == 42
        pkey = kmap.encode('datasets/gov/example/Country', [1, 'lt'], None, None)
        xkey = kmap.encode('datasets/gov/example/Country.code', 1, 'lt', 'datasets/gov/example/Country')
        assert pkey == xkey
        assert kmap.decode('datasets/gov/example/Country', pkey) == [1, 'lt']
        assert kmap.decode('datasets/gov/example/Country.code', pkey) == 'lt'

def test_list():
    context = Context('test')
    kmap = KeyMap('sqlite:///:memory:')
    commands.prepare(context, kmap)
    with kmap:
        val = [42, 'foo', 'bar']
        key = kmap.encode('test', val, None, None)
        assert kmap.decode('test', key) == val

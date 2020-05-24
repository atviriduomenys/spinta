from spinta import commands
from spinta.components import Context
from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap as KeyMap


def test_scalar():
    context = Context('test')
    kmap = KeyMap('sqlite:///:memory:')
    commands.prepare(context, kmap)
    with kmap:
        val = 42
        key = kmap.encode('test', val)
        assert kmap.decode('test', key) == val


def test_list():
    context = Context('test')
    kmap = KeyMap('sqlite:///:memory:')
    commands.prepare(context, kmap)
    with kmap:
        val = [42, 'foo', 'bar']
        key = kmap.encode('test', val)
        assert kmap.decode('test', key) == val

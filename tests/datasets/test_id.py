from spinta import commands
from spinta.components import Context
from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap as KeyMap


def test_scalar():
    context = Context("test")
    kmap = KeyMap("sqlite:///:memory:")
    commands.prepare(context, kmap)
    with kmap:
        pkey = kmap.encode("datasets/gov/example/Country", [1, "lt"])
        xkey = kmap.encode("datasets/gov/example/Country.code", "lt", pkey)
        assert pkey == xkey
        assert kmap.decode("datasets/gov/example/Country", pkey) == [1, "lt"]
        assert kmap.decode("datasets/gov/example/Country.code", xkey) == "lt"


def test_list():
    context = Context("test")
    kmap = KeyMap("sqlite:///:memory:")
    commands.prepare(context, kmap)
    with kmap:
        val = [42, "foo", "bar"]
        key = kmap.encode("test", val)
        assert kmap.decode("test", key) == val

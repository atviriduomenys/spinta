from spinta.core.config import RawConfig
from spinta.core.config import configure_rc
from spinta.testing.context import create_test_context
from spinta.cli.grep import _grep


def test_grep(rc: RawConfig):
    rc = configure_rc(rc, ['''
    d | r | b | m | property | type    | ref     | access
    example                  |         |         |
      |   |   | City         |         |         |
      |   |   |   | name     | string  |         | open
      |   |   |   | country  | ref     | Country | open
    '''])
    context = create_test_context(rc)
    result = _grep(context, ['property'])
    assert list(result) == []

from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.commands.read import _get_row_value
from spinta.testing.manifest import load_manifest_and_context
from spinta.datasets.backends.sql.commands.query import Selected


def test__get_row_value_null(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | ref | source | prepare | access
    example                  |         |     |        |         |
      |   |   | City         |         |     |        |         |
      |   |   |   | name     | string  |     |        |         | open
      |   |   |   | rating   | integer |     |        |         | open
      |   |   |   |          | enum    |     | 1      | 1       |
      |   |   |   |          |         |     | 2      | 2       |
    ''')
    row = ["Vilnius", None]
    model = manifest.models['example/City']
    sel = Selected(1, model.properties['rating'])
    assert _get_row_value(context, row, sel) is None

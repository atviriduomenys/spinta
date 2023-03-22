from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.utils.data import take


def test_prepare(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         |           | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
      |   |   | Country        |         |           | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
      |   |   |   | continent  | ref     | Continent | 4     | open
      |   |   | City           |         |           | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
      |   |   |   | country    | ref     | Country   | 3     | open
    ''')
    model = manifest.models['example/City']
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted(take(table.columns).keys()) == [
        'country._id',
        'id',
        'name',
    ]
    assert [type(c).__name__ for c in table.constraints] == [
        'PrimaryKeyConstraint',
    ]

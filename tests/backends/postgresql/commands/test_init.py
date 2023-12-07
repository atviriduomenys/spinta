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


def test_prepare_base_under_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b         | m           | property | type    | ref       | level | access
    example/base_under                         |         |           |       |
      |   |           | BaseModel   |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   | BaseModel |             |          |         |           | 3     |
      |   |           | NormalModel |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   |           |             | test     | string  |           | 3     | open

    ''')
    model = manifest.models['example/base_under/NormalModel']
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == [
        'PrimaryKeyConstraint'
    ]


def test_prepare_base_over_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b         | m           | property | type    | ref       | level | access
    example/base_over                          |         |           |       |
      |   |           | BaseModel   |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   | BaseModel |             |          |         |           | 4     |
      |   |           | NormalModel |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   |           |             | test     | string  |           | 3     | open

    ''')
    model = manifest.models['example/base_over/NormalModel']
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == sorted([
        'PrimaryKeyConstraint',
        'ForeignKeyConstraint'
    ])


def test_prepare_base_no_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b         | m           | property | type    | ref       | level | access
    example/base_no                            |         |           |       |
      |   |           | BaseModel   |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   | BaseModel |             |          |         |           |       |
      |   |           | NormalModel |          |         |           | 4     |
      |   |           |             | id       | integer |           | 3     | open
      |   |           |             | name     | string  |           | 3     | open
      |   |           |             | test     | string  |           | 3     | open

    ''')
    model = manifest.models['example/base_no/NormalModel']
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == sorted([
        'PrimaryKeyConstraint',
        'ForeignKeyConstraint'
    ])


def test_prepare_model_ref_unique_constraint(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         | id        | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
      |   |   | Country        |         | id, name  | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
    ''')
    model_single_unique = manifest.models['example/Continent']
    backend = model_single_unique.backend
    commands.prepare(context, backend, model_single_unique)
    table = backend.get_table(model_single_unique)
    assert any(
        [table.c['id']] == list(constraint.columns) for constraint in table.constraints if
        type(constraint).__name__ == 'UniqueConstraint')

    model_multiple_unique = manifest.models['example/Country']
    commands.prepare(context, backend, model_multiple_unique)
    table = backend.get_table(model_multiple_unique)
    assert any(
        [table.c['id'], table.c['name']] == list(constraint.columns) for constraint in table.constraints if
        type(constraint).__name__ == 'UniqueConstraint')

import pytest
from sqlalchemy.types import String, BigInteger

from spinta import commands
from spinta.core.config import RawConfig
from spinta.exceptions import UnsupportedDataTypeConfiguration
from spinta.testing.manifest import load_manifest_and_context
from spinta.utils.data import take


def test_prepare(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
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
    """,
    )
    model = commands.get_model(context, manifest, "example/City")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted(take(table.columns).keys()) == [
        "country._id",
        "id",
        "name",
    ]
    assert [type(c).__name__ for c in table.constraints] == [
        "PrimaryKeyConstraint",
    ]


def test_prepare_base_under_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
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

    """,
    )
    model = commands.get_model(context, manifest, "example/base_under/NormalModel")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == ["PrimaryKeyConstraint"]


def test_prepare_base_over_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
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

    """,
    )
    model = commands.get_model(context, manifest, "example/base_over/NormalModel")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == sorted(
        ["PrimaryKeyConstraint", "ForeignKeyConstraint"]
    )


def test_prepare_base_no_level(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
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

    """,
    )
    model = commands.get_model(context, manifest, "example/base_no/NormalModel")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert sorted([type(c).__name__ for c in table.constraints]) == sorted(
        ["PrimaryKeyConstraint", "ForeignKeyConstraint"]
    )


def test_prepare_model_ref_unique_constraint(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         | id        | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
      |   |   | Country        |         | id, name  | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
    """,
    )
    model_single_unique = commands.get_model(context, manifest, "example/Continent")
    backend = model_single_unique.backend
    commands.prepare(context, backend, model_single_unique)
    table = backend.get_table(model_single_unique)
    assert any(
        [table.c["id"]] == list(constraint.columns)
        for constraint in table.constraints
        if type(constraint).__name__ == "UniqueConstraint"
    )

    model_multiple_unique = commands.get_model(context, manifest, "example/Country")
    commands.prepare(context, backend, model_multiple_unique)
    table = backend.get_table(model_multiple_unique)
    assert any(
        [table.c["id"], table.c["name"]] == list(constraint.columns)
        for constraint in table.constraints
        if type(constraint).__name__ == "UniqueConstraint"
    )


def test_prepare_model_custom_property_type(rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Continent": {
                    "properties": {
                        "id": {
                            "type": "sqlalchemy.types.BigInteger",
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         | id        | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert isinstance(table.c["id"].type, BigInteger)


def test_prepare_model_custom_property_type_with_params(rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Continent": {
                    "properties": {
                        "name": {
                            "type": {
                                "name": "sqlalchemy.types.String",
                                "length": 10,
                            },
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref       | level | access
    example                    |         |           |       |
      |   |   | Continent      |         | id        | 4     |
      |   |   |   | id         | integer |           | 3     | open
      |   |   |   | name       | string  |           | 3     | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")
    backend = model.backend
    commands.prepare(context, backend, model)
    table = backend.get_table(model)
    assert isinstance(table.c["name"].type, String)
    assert table.c["name"].type.length == 10


@pytest.mark.parametrize("complex_type", ["file", "array", "object", "text"])
def test_prepare_complex_custom_type_error(complex_type: str, rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Continent": {
                    "properties": {
                        "test": {
                            "type": "sqlalchemy.types.BigInteger",
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        f"""
    d | r | b | m | property   | type           | ref       | level | access
    example                    |                |           |       |
      |   |   | Continent      |                | id        | 4     |
      |   |   |   | id         | integer        |           | 4     | open
      |   |   |   | test       | {complex_type} |           | 4     | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")
    backend = model.backend
    with pytest.raises(UnsupportedDataTypeConfiguration, match=complex_type):
        commands.prepare(context, backend, model)


@pytest.mark.parametrize("ref_level", [3, 4])
def test_prepare_ref_custom_type_error(ref_level: str, rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Continent": {
                    "properties": {
                        "test": {
                            "type": "sqlalchemy.types.BigInteger",
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        f"""
    d | r | b | m | property   | type    | ref     | level       | access
    example                    |         |         |             |
      |   |   | Country        |         | id      | 4           |
      |   |   |   | id         | integer |         | 4           | open
      |   |   | Continent      |         | id      | 4           |
      |   |   |   | id         | integer |         | 4           | open
      |   |   |   | test       | ref     | Country | {ref_level} | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")
    backend = model.backend
    with pytest.raises(UnsupportedDataTypeConfiguration, match="ref"):
        commands.prepare(context, backend, model)


def test_prepare_backref_custom_type_error(rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Country": {
                    "properties": {
                        "test": {
                            "type": "sqlalchemy.types.BigInteger",
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property   | type    | ref     | level       | access
    example                    |         |         |             |
      |   |   | Country        |         | id      | 4           |
      |   |   |   | id         | integer |         | 4           | open
      |   |   |   | test       | backref | Continent | 4           | open
      |   |   | Continent      |         | id      | 4           |
      |   |   |   | id         | integer |         | 4           | open
      |   |   |   | test       | ref     | Country | 4 | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Country")
    backend = model.backend
    with pytest.raises(UnsupportedDataTypeConfiguration, match="backref"):
        commands.prepare(context, backend, model)


def test_prepare_denorm_custom_type_error(rc: RawConfig):
    rc = rc.fork(
        {
            "models": {
                "example/Continent": {
                    "properties": {
                        "test.name": {
                            "type": "sqlalchemy.types.BigInteger",
                        },
                    },
                },
            },
        }
    )

    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property  | type    | ref     | level       | access
    example                   |         |         |             |
      |   |   | Country       |         | id      | 4           |
      |   |   |   | id        | integer |         | 4           | open
      |   |   |   | name      | string  |         | 4           | open
      |   |   | Continent     |         | id      | 4           |
      |   |   |   | id        | integer |         | 4           | open
      |   |   |   | test      | ref     | Country | 4 | open
      |   |   |   | test.name |         |         | 4 | open
    """,
    )
    model = commands.get_model(context, manifest, "example/Continent")
    backend = model.backend
    with pytest.raises(UnsupportedDataTypeConfiguration, match="denorm"):
        commands.prepare(context, backend, model)

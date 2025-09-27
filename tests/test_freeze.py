import pathlib

import pytest

from spinta.testing.cli import SpintaCliRunner
from spinta.testing.utils import create_manifest_files
from spinta.testing.utils import update_manifest_files
from spinta.testing.utils import read_manifest_files
from spinta.testing.utils import readable_manifest_files


@pytest.fixture()
def rc(rc, tmp_path: pathlib.Path):
    return rc.fork().add(
        "test",
        {
            "manifests.default": {
                "type": "backend",
                "backend": "default",
                "sync": "yaml",
            },
            "manifests.yaml.path": str(tmp_path),
        },
    )


def test_create_model(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
        },
    )

    assert cli.invoke(rc, ["freeze"]).exit_code == 0

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        "country.yml": [
            {
                "type": "model",
                "name": "Country",
                "id": "Country:0",
                "version": "Country:1",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            {
                "id": "Country:1",
                "parents": [],
                "migrate": [
                    {
                        "type": "schema",
                        "upgrade": [
                            "create_table(",
                            "    'Country',",
                            "    column('_id', pk()),",
                            "    column('_revision', string()),",
                            "    column('name', string())",
                            ")",
                        ],
                        "downgrade": [
                            "drop_table('Country')",
                        ],
                    },
                ],
            },
        ],
    }


def test_add_column(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/code",
                    "value": {
                        "type": "string",
                    },
                }
            ],
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        "country.yml": [
            {
                "type": "model",
                "name": "Country",
                "id": "Country:0",
                "version": "Country:2",
                "properties": {
                    "name": {"type": "string"},
                    "code": {"type": "string"},
                },
            },
            {
                "id": "Country:2",
                "parents": [],
                "migrate": [
                    {
                        "type": "schema",
                        "upgrade": [
                            "add_column('Country', column('code', string()))",
                        ],
                        "downgrade": [
                            "drop_column('Country', 'code')",
                        ],
                    },
                ],
            },
        ],
    }


def test_freeze_no_changes(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        "country.yml": [
            {
                "type": "model",
                "name": "Country",
                "id": "Country:0",
                "version": "Country:1",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            {
                "id": "Country:1",
                "parents": [],
                "migrate": [
                    {
                        "type": "schema",
                        "upgrade": [
                            "create_table(",
                            "    'Country',",
                            "    column('_id', pk()),",
                            "    column('_revision', string()),",
                            "    column('name', string())",
                            ")",
                        ],
                        "downgrade": [
                            "drop_table('Country')",
                        ],
                    },
                ],
            },
        ],
    }


def test_freeze_array(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "names": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                    },
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:list/names',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('Country._id', ondelete: 'CASCADE')),",
                "    column('names', string())",
                ")",
            ],
            "downgrade": [
                "drop_table('Country/:list/names')",
            ],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('names', json())",
                ")",
            ],
            "downgrade": [
                "drop_table('Country')",
            ],
        },
    ]


def test_freeze_array_with_object(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "notes": {"type": "array", "items": {"type": "object", "properties": {"note": {"type": "string"}}}}
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('Country/:list/notes')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:list/notes',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('Country._id', ondelete: 'CASCADE')),",
                "    column('notes.note', string())",
                ")",
            ],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('notes', json())",
                ")",
            ],
            "downgrade": [
                "drop_table('Country')",
            ],
        },
    ]


def test_freeze_object(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "report.yml": {
                "type": "model",
                "name": "Report",
                "properties": {
                    "str": {"type": "string"},
                    "note": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string"},
                            "number": {"type": "integer"},
                            "list": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["report.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('Report/:list/note.list')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Report/:list/note.list',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('Report._id', ondelete: 'CASCADE')),",
                "    column('note.list', string())",
                ")",
            ],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Report',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('note.list', json()),",
                "    column('note.number', integer()),",
                "    column('note.text', string()),",
                "    column('str', string())",
                ")",
            ],
            "downgrade": [
                "drop_table('Report')",
            ],
        },
    ]


def test_freeze_file(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "flag": {"type": "file"},
                    "anthem": {"type": "file", "backend": "fs"},
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('Country/:file/flag')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:file/flag',",
                "    column('_id', uuid()),",
                "    column('_block', binary())",
                ")",
            ],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('anthem._id', string()),",
                "    column('anthem._content_type', string()),",
                "    column('anthem._size', integer()),",
                "    column('flag._id', string()),",
                "    column('flag._content_type', string()),",
                "    column('flag._size', integer()),",
                "    column('flag._bsize', integer()),",
                "    column('flag._blocks', array(uuid()))",
                ")",
            ],
            "downgrade": [
                "drop_table('Country')",
            ],
        },
    ]


def test_freeze_list_of_files(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "flags": {"type": "array", "items": {"type": "file"}},
                },
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('Country/:file/flags')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:file/flags',",
                "    column('_id', uuid()),",
                "    column('_block', binary())",
                ")",
            ],
        },
        {
            "downgrade": ["drop_table('Country/:list/flags')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:list/flags',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('Country._id', ondelete: 'CASCADE')),",
                "    column('flags._id', string()),",
                "    column('flags._content_type', string()),",
                "    column('flags._size', integer()),",
                "    column('flags._bsize', integer()),",
                "    column('flags._blocks', array(uuid()))",
                ")",
            ],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('flags', json())",
                ")",
            ],
            "downgrade": [
                "drop_table('Country')",
            ],
        },
    ]


def test_freeze_ref(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            "city.yml": {
                "type": "model",
                "name": "City",
                "properties": {"country": {"type": "ref", "model": "Country"}, "name": {"type": "string"}},
            },
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["city.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('City')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'City',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('country._id', ref('Country._id')),",
                "    column('name', string())",
                ")",
            ],
        },
    ]
    assert manifest_files["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('name', string())",
                ")",
            ],
            "downgrade": ["drop_table('Country')"],
        },
    ]


def test_add_reference_column(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            "city.yml": {
                "type": "model",
                "name": "City",
                "properties": {"name": {"type": "string"}},
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/city",
                    "value": {
                        "type": "ref",
                        "model": "City",
                    },
                }
            ]
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["country.yml"][-1]["migrate"] == [
        {
            "downgrade": [
                "drop_column('Country', 'city._id')",
            ],
            "type": "schema",
            "upgrade": [
                "add_column(",
                "    'Country',",
                "    column('city._id', ref('City._id'))",
                ")",
            ],
        },
    ]


def test_change_ref_model(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            "continent.yml": {
                "type": "model",
                "name": "Continent",
                "properties": {
                    "name": {"type": "string"},
                },
            },
            "city.yml": {
                "type": "model",
                "name": "City",
                "properties": {
                    "country": {
                        "type": "ref",
                        "model": "Country",
                    },
                    "name": {"type": "string"},
                },
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "city.yml": [
                {
                    "op": "replace",
                    "path": "/properties/country/model",
                    "value": "Continent",
                }
            ]
        },
    )

    with pytest.raises(NotImplementedError):
        cli.invoke(rc, ["freeze"], catch_exceptions=False)


def test_freeze_ref_in_array(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "cities": {"type": "array", "items": {"type": "ref", "model": "City"}},
                },
            },
            "city.yml": {"type": "model", "name": "City", "properties": {"name": {"type": "string"}}},
        },
    )

    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["city.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_table('City')"],
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'City',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('name', string())",
                ")",
            ],
        },
    ]
    assert manifest_files["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country/:list/cities',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('Country._id', ondelete: 'CASCADE')),",
                "    column('cities._id', ref('City._id'))",
                ")",
            ],
            "downgrade": ["drop_table('Country/:list/cities')"],
        },
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('cities', json())",
                ")",
            ],
            "downgrade": ["drop_table('Country')"],
        },
    ]


def test_change_field_type_in_object(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "population": {
                        "type": "object",
                        "properties": {"amount": {"type": "string"}},
                    },
                },
            }
        },
    )

    cli.invoke(rc, ["freeze"])
    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "replace",
                    "path": "/properties/population/properties/amount/type",
                    "value": "integer",
                },
            ]
        },
    )

    cli.invoke(rc, ["freeze"])
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": [
                "alter_column(",
                "    'Country',",
                "    'population.amount',",
                "    type_: string()",
                ")",
            ],
            "type": "schema",
            "upgrade": [
                "alter_column(",
                "    'Country',",
                "    'population.amount',",
                "    type_: integer()",
                ")",
            ],
        },
    ]


def test_change_field_type_in_list(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {"flags": {"type": "array", "items": {"type": "string"}}},
            }
        },
    )

    cli.invoke(rc, ["freeze"])
    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "replace",
                    "path": "/properties/flags/items/type",
                    "value": "integer",
                }
            ]
        },
    )

    cli.invoke(rc, ["freeze"])
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": [
                "alter_column(",
                "    'Country/:list/flags',",
                "    'flags',",
                "    type_: string()",
                ")",
            ],
            "type": "schema",
            "upgrade": [
                "alter_column(",
                "    'Country/:list/flags',",
                "    'flags',",
                "    type_: integer()",
                ")",
            ],
        },
    ]


def test_add_field_to_object(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "population": {
                        "type": "object",
                        "properties": {"amount": {"type": "string"}},
                    }
                },
            }
        },
    )

    cli.invoke(rc, ["freeze"])
    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {"op": "add", "path": "/properties/population/properties/code", "value": {"type": "string"}},
            ]
        },
    )

    cli.invoke(rc, ["freeze"])
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": ["add_column(", "    'Country',", "    column('population.code', string())", ")"],
            "downgrade": ["drop_column('Country', 'population.code')"],
        },
    ]


def test_add_field(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir, {"country.yml": {"type": "model", "name": "Country", "properties": {"name": {"type": "string"}}}}
    )

    cli.invoke(rc, ["freeze"])
    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/cities",
                    "value": {"type": "object", "properties": {"name": {"type": "string"}}},
                }
            ]
        },
    )

    cli.invoke(rc, ["freeze"])
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": ["add_column('Country', column('cities.name', string()))"],
            "downgrade": ["drop_column('Country', 'cities.name')"],
        }
    ]


def test_freeze_nullable(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {"name": {"type": "string", "nullable": True}},
            }
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "type": "schema",
            "upgrade": [
                "create_table(",
                "    'Country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('name', string(), nullable: true)",
                ")",
            ],
            "downgrade": ["drop_table('Country')"],
        }
    ]


def test_change_nullable(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {"name": {"type": "string", "nullable": True}},
            }
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir, {"country.yml": [{"op": "replace", "path": "/properties/name/nullable", "value": False}]}
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)["country.yml"][-1]["migrate"] == [
        {
            "downgrade": ["alter_column('Country', 'name', nullable: true)"],
            "type": "schema",
            "upgrade": ["alter_column('Country', 'name', nullable: false)"],
        },
    ]


def test_add_nullable_column(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/flag",
                    "value": {
                        "type": "string",
                        "nullable": True,
                    },
                }
            ]
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["country.yml"][-1]["migrate"] == [
        {
            "downgrade": [
                "drop_column('Country', 'flag')",
            ],
            "type": "schema",
            "upgrade": [
                "add_column(",
                "    'Country',",
                "    column('flag', string(), nullable: true)",
                ")",
            ],
        },
    ]


def test_delete_property(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {"name": {"type": "string"}, "flag": {"type": "string"}},
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "remove",
                    "path": "/properties/flag",
                }
            ]
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["country.yml"][-1]["migrate"] == []


def test_delete_property_from_object(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                    "info": {
                        "type": "object",
                        "properties": {
                            "flag": {"type": "string"},
                            "capital": {"type": "string"},
                        },
                    },
                },
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "remove",
                    "path": "/properties/info/properties/flag",
                }
            ]
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["country.yml"][-1]["migrate"] == []


def test_replace_all_properties(rc, cli: SpintaCliRunner):
    tmpdir = rc.get("manifests", "yaml", "path", cast=pathlib.Path)

    create_manifest_files(
        tmpdir,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {"name": {"type": "string"}, "flag": {"type": "string"}},
            },
        },
    )
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmpdir,
        {
            "country.yml": [
                {
                    "op": "replace",
                    "path": "/properties",
                    "value": {"capital": {"type": "string"}},
                }
            ]
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files["country.yml"][-1]["migrate"] == [
        {
            "downgrade": ["drop_column('Country', 'capital')"],
            "type": "schema",
            "upgrade": ["add_column('Country', column('capital', string()))"],
        }
    ]

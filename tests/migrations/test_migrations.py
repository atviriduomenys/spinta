import pytest

from spinta.testing.cli import SpintaCliRunner
from spinta.testing.utils import (
    create_manifest_files,
    update_manifest_files,
    read_manifest_files,
)


def configure(rc, path):
    return rc.fork().add(
        "test",
        {
            "manifests.default": {
                "type": "backend",
                "backend": "default",
                "sync": "yaml",
            },
            "manifests.yaml.path": str(path),
        },
    )


def test_create_table(rc, cli: SpintaCliRunner, tmp_path):
    rc = configure(rc, tmp_path)

    create_manifest_files(
        tmp_path,
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

    manifest = read_manifest_files(tmp_path)
    freezed = manifest["country.yml"][-1]
    assert freezed == {
        "id": freezed["id"],
        "date": freezed["date"],
        "parents": [],
        "changes": freezed["changes"],
        "migrate": [
            {
                "type": "schema",
                "upgrade": "create_table(\n"
                "    'Country',\n"
                "    column('_id', pk()),\n"
                "    column('_revision', string()),\n"
                "    column('name', string())\n"
                ")",
                "downgrade": "drop_table('Country')",
            },
        ],
    }


@pytest.mark.skip("TODO")
def test_add_column(rc, cli: SpintaCliRunner, tmp_path):
    create_manifest_files(
        tmp_path,
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
    rc = configure(rc, tmp_path)
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmp_path,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/code",
                    "value": {
                        "type": "string",
                    },
                },
            ],
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmp_path)
    freezed = manifest["country.yml"][-1]
    assert freezed == {
        "version": {
            "id": freezed["version"]["id"],
            "date": freezed["version"]["date"],
            "parents": [],
        },
        "changes": freezed["changes"],
        "migrate": [
            {
                "type": "schema",
                "upgrade": "add_column(\n    'Country',\n    column('code', string())\n)",
                "downgrade": "drop_column('Country', 'code')",
            },
        ],
    }


@pytest.mark.skip("TODO")
def test_alter_column(rc, cli: SpintaCliRunner, tmp_path):
    create_manifest_files(
        tmp_path,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                    "area": {"type": "integer"},
                },
            },
        },
    )
    rc = configure(rc, tmp_path)
    cli.invoke(rc, ["freeze"])

    update_manifest_files(
        tmp_path,
        {
            "country.yml": [
                {"op": "replace", "path": "/properties/area/type", "value": "number"},
            ],
        },
    )
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmp_path)
    assert manifest["country.yml"][-1] == {}


@pytest.mark.skip("TODO")
def test_schema_with_multiple_head_nodes(rc, cli: SpintaCliRunner, tmp_path):
    # test schema creator when resource has a diverging scheme version history
    # i.e. 1st (root) schema migration has 2 diverging branches
    #
    # have model with 3 schema versions, with 2nd and 3rd diverging from 1st:
    # 1st version: create table with title: string and area: integer
    create_manifest_files(
        tmp_path,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "title": {"type": "string"},
                    "area": {"type": "number"},
                    "code": {"type": "string"},
                },
            },
        },
    )
    rc = configure(rc, tmp_path)
    cli.invoke(rc, ["freeze"])
    manifest = read_manifest_files(tmp_path)
    version = manifest["country.yml"][-1]["version"]["id"]

    # 2nd version (parent 1st): add column - code: string
    update_manifest_files(
        tmp_path,
        {
            "country.yml": [
                {
                    "op": "add",
                    "path": "/properties/code",
                    "value": {
                        "type": "string",
                    },
                },
            ],
        },
    )
    cli.invoke(rc, ["freeze"])

    # 3rd version (parent 1st): alter column - area: number
    update_manifest_files(
        tmp_path,
        {
            "country.yml": [
                {"op": "replace", "path": "/properties/area/type", "value": "number"},
            ],
        },
    )
    cli.invoke(rc, ["freeze", "-p", version])
    manifest = read_manifest_files(tmp_path)
    assert manifest["country.yml"][-1]["version"]["id"] == version

    cli.invoke(rc, ["migrate"])


@pytest.mark.skip("TODO")
def test_build_schema_relation_graph(rc, cli: SpintaCliRunner, tmp_path):
    # test new schema version, when multiple models have no foreign keys
    create_manifest_files(
        tmp_path,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "title": {"type": "string"},
                },
            },
            "org.yml": {
                "type": "model",
                "name": "Org",
                "properties": {
                    "title": {"type": "string"},
                    "country": {"type": "ref", "model": "Country"},
                },
            },
            "report.yml": {
                "type": "model",
                "name": "Report",
                "properties": {
                    "title": {"type": "string"},
                    "org": {"type": "ref", "model": "Org"},
                },
            },
        },
    )
    rc = configure(rc, tmp_path)
    cli.invoke(rc, ["freeze"])

    manifest = read_manifest_files(tmp_path)
    version = {
        "country": manifest["country.yml"][-1]["version"],
        "org": manifest["org.yml"][-1]["version"],
        "report": manifest["report.yml"][-1]["version"],
    }
    assert version["country"]["parent"] == []
    assert version["org"]["parent"] == [version["country"]["id"]]
    assert version["report"]["parent"] == [version["org"]["id"]]

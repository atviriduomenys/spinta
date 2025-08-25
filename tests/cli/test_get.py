import pytest
import textwrap

from spinta.exceptions import NoPrimaryKeyCandidatesFound, PropertyNotFound
from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest


def yaml_content():
    return textwrap.dedent("""\
        ---
        _type: datasets/gov/example/City
        _id: 0AF24A60-00A2-4EAB-AEFF-BBA86204BC98
        name: Vilnius
        country:
          _id: 4689C28B-1C44-4184-8715-16021EE87EAD
          name: Lietuva
        ---
        _type: datasets/gov/example/Country
        _id: 4689C28B-1C44-4184-8715-16021EE87EAD
        name: Lietuva
        ---
    """)


def yaml_content_with_wrong_ref():
    return textwrap.dedent("""\
        ---
        _type: datasets/gov/example/City
        _id: 0AF24A60-00A2-4EAB-AEFF-BBA86204BC98
        name: Vilnius
        country:
          _id: 4689C28B-1C44-4184-8715-16021EE87EAD
          name: Lietuva
        ---
        _type: datasets/gov/example/Country
        _id: 4689C28B-1C44-4184-8715-16021EE87EAC
        name: Lietuva
        ---
    """)


def yaml_content_without_field():
    return textwrap.dedent("""\
        ---
        _type: datasets/gov/example/City
        _id: 0AF24A60-00A2-4EAB-AEFF-BBA86204BC98
        country:
          _id: 4689C28B-1C44-4184-8715-16021EE87EAD
          name: Lietuva
        ---
        _type: datasets/gov/example/Country
        _id: 4689C28B-1C44-4184-8715-16021EE87EAC
        name: Lietuva
        ---
    """)


def test_getall(context, rc, cli: SpintaCliRunner, tmp_path):
    manifest = striptable("""
     d | r | b | m  | property         | type         | ref     | source     | access
     datasets/gov/example              |              |         |            |
       | data                          | dask/memory  |         |            |
       |   |                           |              |         |            |
       |   |   | Country               |              | _id     |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    |                  |              |         |            |
       |   |   | City                  |              |         |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    | country          | ref          | Country | ..         |
    """)

    temp_yaml_file = tmp_path / "test_config.yaml"
    temp_manifest_file = tmp_path / "manifest.csv"

    with temp_yaml_file.open("w") as file:
        file.write(yaml_content())
    create_tabular_manifest(context, temp_manifest_file, manifest)

    result = cli.invoke(rc, ["getall", temp_manifest_file, temp_yaml_file, "datasets/gov/example/City"])
    assert (
        striptable(result.stdout)
        == """{"_data": [{"_type": "datasets/gov/example/City", "_id": "0AF24A60-00A2-4EAB-AEFF-BBA86204BC98", "name": "Vilnius", "country": {"_id": "4689C28B-1C44-4184-8715-16021EE87EAD"}}]}"""
    )


def test_getall_wrong_ref_id(context, rc, cli: SpintaCliRunner, tmp_path):
    manifest = striptable("""
     d | r | b | m  | property         | type         | ref     | source     | access
     datasets/gov/example              |              |         |            |
       | data                          | dask/memory  |         |            |
       |   |                           |              |         |            |
       |   |   | Country               |              | _id     |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    |                  |              |         |            |
       |   |   | City                  |              |         |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    | country          | ref          | Country | ..         |
    """)
    temp_yaml_file = tmp_path / "test_config.yaml"
    temp_manifest_file = tmp_path / "manifest.csv"

    with temp_yaml_file.open("w") as file:
        file.write(yaml_content_with_wrong_ref())

    create_tabular_manifest(context, temp_manifest_file, manifest)
    with pytest.raises(NoPrimaryKeyCandidatesFound):
        result = cli.invoke(
            rc,
            ["getall", temp_manifest_file, temp_yaml_file, "datasets/gov/example/City"],
            fail=False,
        )
        raise result.exception


def test_getall_field_not_available(context, rc, cli: SpintaCliRunner, tmp_path):
    manifest = striptable("""
     d | r | b | m  | property         | type         | ref     | source     | access
     datasets/gov/example              |              |         |            |
       | data                          | dask/memory  |         |            |
       |   |                           |              |         |            |
       |   |   | Country               |              | _id     |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    |                  |              |         |            |
       |   |   | City                  |              |         |            | open
       |   |   |    | _id              | string       |         | _id        |
       |   |   |    | name             | string       |         | name       |
       |   |   |    | country          | ref          | Country | ..         |
    """)
    temp_yaml_file = tmp_path / "test_config.yaml"
    temp_manifest_file = tmp_path / "manifest.csv"

    with temp_yaml_file.open("w") as file:
        file.write(yaml_content_without_field())

    create_tabular_manifest(context, temp_manifest_file, manifest)
    with pytest.raises(PropertyNotFound):
        result = cli.invoke(
            rc,
            ["getall", temp_manifest_file, temp_yaml_file, "datasets/gov/example/City"],
            fail=False,
        )
        raise result.exception

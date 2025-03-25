import json
from pathlib import Path
import pytest
from yaml.scanner import ScannerError

from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.utils.get import (
    getall_json_content_from_yaml,
    getone_json_content_from_yaml,
)


@pytest.fixture
def manifest_file(tmp_path: Path):
    manifest_path = tmp_path / "manifest.csv"
    create_tabular_manifest(
        None,
        manifest_path,
        striptable("""
     d | r | b | m  | property         | type    | ref     | source     | access
     datasets/gov/example              |         |         |            |
       | data                          | memory  |         |            |
       |   |                           |         |         |            |
       |   |   | Country               |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    |                  |         |         |            |
       |   |   | City                  |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    | country          | ref     | Country |            |
        """),
    )
    return manifest_path


def yaml_content():
    return """
        - _type: datasets/gov/example/City
          _id: ca117a8a-fdd0-4c66-9565-093d4d493e6b
          name: Vilnius
          country:
            _id: dac60010-9a29-4201-8da9-e624023eb626
            name: Lietuva
        - _type: datasets/gov/example/Country
          _id: dac60010-9a29-4201-8da9-e624023eb626
          name: Lietuva
    """


def test_getall_json_content_from_yaml_success(context: Context, manifest_file: Path):
    json_content = getall_json_content_from_yaml(
        [str(manifest_file)], yaml_content(), "datasets/gov/example/City"
    )
    assert json_content == {
        "_data": [
            {
                "_type": "datasets/gov/example/City",
                "_id": "ca117a8a-fdd0-4c66-9565-093d4d493e6b",
                "name": "Vilnius",
                "country": {
                    "_id": "dac60010-9a29-4201-8da9-e624023eb626",
                    "name": "Lietuva",
                },
            }
        ]
    }


def test_getall_json_content_from_yaml_invalid_yaml(
    context: Context, manifest_file: Path
):
    yaml_with_error = yaml_content().replace("_type:", "_type")

    with pytest.raises(ScannerError):
        getall_json_content_from_yaml(
            [str(manifest_file)], yaml_with_error, "datasets/gov/example/City"
        )


def test_getall_json_content_from_yaml_yaml_without_id(
    context: Context, manifest_file: Path
):
    yaml_content = """
        - _type: datasets/gov/example/City
          name: Vilnius
          country:
            name: Lietuva
    
        - _type: datasets/gov/example/Country
          name: Lietuva"""
    json_content = getall_json_content_from_yaml(
        [str(manifest_file)], yaml_content, "datasets/gov/example/City"
    )

    for _, items in json_content.items():
        for item in items:
            assert "_id" in item


def test_getone_json_content_from_yaml_success(context: Context, manifest_file: Path):
    json_content = getone_json_content_from_yaml(
        [str(manifest_file)],
        yaml_content(),
        "datasets/gov/example/City",
        "ca117a8a-fdd0-4c66-9565-093d4d493e6b",
    )
    assert json_content == {
        "_type": "datasets/gov/example/City",
        "_id": "ca117a8a-fdd0-4c66-9565-093d4d493e6b",
        "name": "Vilnius",
        "country": {"_id": "dac60010-9a29-4201-8da9-e624023eb626", "name": "Lietuva"},
    }


def test_getone_json_content_from_yaml_invalid_yaml(
    context: Context, manifest_file: Path
):
    yaml_with_error = yaml_content().replace("_type:", "_type")

    with pytest.raises(ScannerError):
        getone_json_content_from_yaml(
            [str(manifest_file)],
            yaml_with_error,
            "datasets/gov/example/City",
            "ca117a8a-fdd0-4c66-9565-093d4d493e6b",
        )


def test_getone_json_content_from_yaml_no_match_returns_empty_dict(
    context: Context, manifest_file: Path
):
    json_content = getone_json_content_from_yaml(
        [str(manifest_file)], yaml_content(), "datasets/gov/example/City", "wrong_id"
    )
    assert json_content == {}

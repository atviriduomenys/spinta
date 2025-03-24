import json
from pathlib import Path
import pytest
from yaml.scanner import ScannerError

from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.utils.get import get_json_content_from_yaml


@pytest.fixture
def manifest_file(tmp_path: Path):
    manifest_path = tmp_path / "manifest.csv"
    create_tabular_manifest(
        None,
        manifest_path,
        striptable("""
        d | r | b | m | property | type    | ref     |
        datasets/gov/example     |         |         |           
          | data                 | memory  |         |           
                                 |         |         |             
          |   |   | City         |         | name    |             
          |   |   |   | id       | integer |         |             
          |   |   |   | name     | string  |         |             
          |   |   |   | country  | ref     | Country |
                                 |         |         |
          |                      |         |         |
          |   |   | Country      |         |         |
          |   |   |   | id       | integer |         |
          |   |   |   | name     | string  |         |   
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


def test_get_json_content_from_yaml_success(context: Context, manifest_file: Path):
    json_content = get_json_content_from_yaml(yaml_content(), [str(manifest_file)])
    assert (
        json_content
        == """{"datasets/gov/example/City": [{"_type": "datasets/gov/example/City", "_id": "ca117a8a-fdd0-4c66-9565-093d4d493e6b", "name": "Vilnius", "country": {"_id": "dac60010-9a29-4201-8da9-e624023eb626", "name": "Lietuva"}}], "datasets/gov/example/Country": [{"_type": "datasets/gov/example/Country", "_id": "dac60010-9a29-4201-8da9-e624023eb626", "name": "Lietuva"}]}"""
    )


def test_get_json_content_from_yaml_invalid_yaml(context: Context, manifest_file: Path):
    yaml_with_error = yaml_content().replace("_type:", "_type")

    with pytest.raises(ScannerError):
        get_json_content_from_yaml(yaml_with_error, [str(manifest_file)])


def test_get_json_content_from_yaml_yaml_without_id(context: Context, manifest_file: Path):
    yaml_content = """
        - _type: datasets/gov/example/City
          name: Vilnius
          country:
            name: Lietuva
    
        - _type: datasets/gov/example/Country
          name: Lietuva"""
    json_content = json.loads(get_json_content_from_yaml(yaml_content, [str(manifest_file)]))

    for key, items in json_content.items():
        for item in items:
            assert "_id" in item

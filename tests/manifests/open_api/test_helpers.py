import json
from pathlib import Path

from spinta.manifests.open_api.helpers import read_file_data_and_transform_to_json, get_dataset_schema


def test_read_file_data_and_transform_to_json(tmp_path: Path):
    file_data = {
        'openapi': '3.0.0',
        'info': {
            'title': 'OpenAPI'
        }
    }
    path = tmp_path / 'manifest.json'
    with open(path, 'w') as json_file:
        json_file.write(json.dumps(file_data))

    result = read_file_data_and_transform_to_json(path)

    assert result == file_data


def test_get_dataset_schema():
    info = {
        'title': 'Example API',
        'version': '1.0.0',
        'summary': 'Example of an API',
        'description': 'Intricate description'
    }

    dataset_schema = get_dataset_schema(info)

    assert dataset_schema == {
        'type': 'dataset',
        'name': 'example_api',
        'title': info['summary'],
        'description': info['description']
    }

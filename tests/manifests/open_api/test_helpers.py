import json
from pathlib import Path

from spinta.manifests.open_api.helpers import (
    read_file_data_and_transform_to_json,
    get_dataset_schemas,
    get_namespace_schema,
)


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


def test_get_namespace_schema():
    title = 'Geography API'
    dataset_prefix = 'services/geography_api'
    data = {
        'info': {
            'title': title,
            'version': '1.0.0',
            'summary': 'API for geographic objects',
            'description': 'Intricate description'
        }
    }

    _, namespace_schema = get_namespace_schema(data['info'], title, dataset_prefix)

    assert namespace_schema == {
        'type': 'ns',
        'name': dataset_prefix,
        'title': 'API for geographic objects',
        'description': 'Intricate description'
    }


def test_get_dataset_schema():
    data = {
        'info': {
            'title': 'Geography API',
            'version': '1.0.0',
            'summary': 'API for geographic objects',
            'description': 'Intricate description'
        },
        'paths': {
            '/api/countries': {
                'get': {
                    'tags': ['List of Countries']
                }
            },
            '/api/cities' : {
                'get': {
                    'tags': ['List', 'Cities']
                }
            }
        }
    }

    dataset_schemas = [schema for _, schema in get_dataset_schemas(data, 'services/geography_api')]

    assert dataset_schemas == [
        {
            'type': 'dataset',
            'name': 'services/geography_api/list_of_countries',
            'title': "List of Countries",
            'description': ''
        },
        {
            'type': 'dataset',
            'name': 'services/geography_api/list_cities',
            'title': "List, Cities",
            'description': ''
        }
    ]

import json
import uuid
from pathlib import Path

from spinta.core.ufuncs import Expr
from spinta.manifests.open_api.helpers import (
    read_file_data_and_transform_to_json,
    get_dataset_schemas,
    get_namespace_schemas,
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


def test_get_namespace_schemas():
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

    namespace_schemas = [schema for _, schema in get_namespace_schemas(data['info'], title, dataset_prefix)]

    assert namespace_schemas == [
        {
            'type': 'ns',
            'name': 'services',
            'title': '',
            'description': ''
        },
        {
            'type': 'ns',
            'name': dataset_prefix,
            'title': 'API for geographic objects',
            'description': 'Intricate description'
        }
    ]


def test_get_dataset_schemas():
    resource_countries_id = str(uuid.uuid4().hex)
    resource_cities_id = str(uuid.uuid4().hex)
    data = {
        'info': {
            'title': 'Geography API',
            'version': '1.0.0',
            'summary': 'API for geographic objects',
            'description': 'Intricate description'
        },
        'tags': [
            {
                'name': 'List of Countries',
                'description': 'List known countries'
            },
            {
                'name': 'Cities',
                'description': 'Known cities'
            }
        ],
        'paths': {
            '/api/countries': {
                'get': {
                    'tags': ['List of Countries'],
                    'summary': 'List of countries API',
                    'description': 'List of known countries in the world',
                    'operationId': resource_countries_id
                }
            },
            '/api/cities' : {
                'get': {
                    'tags': ['List', 'Cities'],
                    'summary': 'List of cities API',
                    'description': 'List of known cities in the world',
                    'operationId': resource_cities_id
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
            'description': 'List known countries',
            'resources': {
                'api_countries_get': {
                    'id': resource_countries_id,
                    'external': '/api/countries',
                    'type': 'dask/json',
                    'prepare': Expr('http', method='GET', body='form'),
                    'title': 'List of countries API',
                    'description': 'List of known countries in the world'
                }
            }
        },
        {
            'type': 'dataset',
            'name': 'services/geography_api/list_cities',
            'title': "List, Cities",
            'description': 'Known cities',
            'resources': {
                'api_cities_get': {
                    'id': resource_cities_id,
                    'external': '/api/cities',
                    'type': 'dask/json',
                    'prepare': Expr('http', method='GET', body='form'),
                    'title': 'List of cities API',
                    'description': 'List of known cities in the world'
                }
            }
        }
    ]

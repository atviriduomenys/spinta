from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Generator

from spinta.core.ufuncs import Expr
from spinta.utils.naming import to_dataset_name, to_code_name, to_model_name, to_property_name
from spinta.utils.naming import Deduplicator

SUPPORTED_PARAMETER_LOCATIONS = {'query', 'header', 'path'}
DEFAULT_DATASET_NAME = 'default'
DEFAULT_MODEL_NAME = 'default'
PROPERTY_DATA_TYPES = {
    "string": "string",
    "number": "number",
    "integer": "integer",
    "boolean": "boolean",
    "array": "array",
    "object": "ref"
}


def replace_url_parameters(endpoint: str) -> str:
    """Replaces parameters in given endpoint to their codenames.

    e.g. /api/cities/{cityId}/ -> /api/cities/{city_id}
    """
    return re.sub(r'{([^{}]+)}', lambda match: f'{{{to_code_name(match.group(1))}}}', endpoint)


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        return json.load(file)


def get_namespace_schema(info: dict, title: str, dataset_prefix: str) -> Generator[tuple[None, dict], None, None]:
    yield None, {
        'type': 'ns',
        'name': dataset_prefix,
        'title': info.get('summary', title),
        'description': info.get('description', ''),
    }


def get_resource_parameters(parameters: list[dict]) -> dict[str, dict]:
    resource_parameters = {}
    for index, value in enumerate(parameters):
        name = value['name']
        location = value['in'] if value['in'] in SUPPORTED_PARAMETER_LOCATIONS else ''
        resource_parameters[f'parameter_{index}'] = {
            'name': to_code_name(name),
            'source': [name],
            'prepare': [Expr(location)],
            'type': 'param',
            'description': value.get('description', ''),
        }

    return resource_parameters

model_deduplicator = Deduplicator()

def build_models(dataset: str, resource: str, model_name: str, model_source_name: str, properties_schema: dict, root: bool = False):
    properties, nested_models = build_properties(dataset, resource, properties_schema, model_name)
    models = [{
        'type': 'model',
        'name': model_name,
        'access': 'open',
        'description': '',
        'visibility': 'absent',
        'external': {
            'dataset': dataset,
            'resource': resource,
            'name': model_source_name,
            'prepare': '',
        },
        'properties': properties
    }]
    if not root:
        models[0]['comments'] = [
                {
                    'id':'smth', 
                    'access': '',
                    'parent': 'model',
                    'author': 'wergfds',
                    'created': 'werger',
                    'comment': 'egewg',
                    # 'prepare': Expr('update', model=f"{model_name}/:part")
                    # 'external': {
                    #     'prepare': Expr('update', model=f"{model_name}/:part")
                    # }
                    #NOTE: How to add 'prepare' column for comments?
                    
                }
            ]
    models.extend(nested_models)
    return models

def build_properties(dataset: str, resource: str, properties_schema: dict, model_name: str) -> tuple[dict, list[dict]]:

    properties = {}
    nested_models = []

    for property_source_name, property_metadata in properties_schema.items():
        property_type = property_metadata['type']
        property_name = to_property_name(property_source_name)
        properties[property_name] = {
            'type': property_type, #TODO: Map property types based on 'format' if present, for more precise mapping.
            'visibility': 'absent',
            'description': property_metadata.get('description', ''),
            'external': {
                'name': property_source_name,
            }
        }
        if property_type == 'ref':
            properties[property_name]['model'] = property_metadata['model']
        if property_type == 'array' and property_metadata['items']['type'] == 'object':
            nested_model_source_name = property_source_name
            nested_model_name = model_deduplicator(to_model_name(nested_model_source_name))
            properties[f"{property_name}[]"] = {
                'type': 'backref',
                'model': nested_model_name,
                'visibility': 'absent',
                'external': {
                    'prepare': Expr('expand')
                }
            }
            property_metadata['items']['properties'][to_property_name(model_name)]= {
                'type': 'ref',
                'model': model_name
            }
            nested_models.extend(build_models(dataset, resource, nested_model_name,nested_model_source_name, property_metadata['items']['properties']))

    return properties, nested_models

def get_dataset_models(dataset_name: str, resource_name: str, schema: dict) -> list[dict]:

    model_name = to_model_name(schema['title'] if 'title' in schema else DEFAULT_MODEL_NAME) #TODO: Find a way to construct model name when 'title' is not present
    model_name = model_deduplicator(model_name)
    model_properties = schema['properties'] if 'properties' in schema else schema['items']['properties'] 

    return build_models(dataset_name, resource_name, model_name, '.', model_properties, root=True)


def get_dataset_schemas(data: dict, dataset_prefix: str) -> Generator[tuple[None, dict]]:
    datasets = {}
    models = []
    tag_metadata = {tag['name']: tag.get('description', '') for tag in data.get('tags', {})}

    for api_endpoint, api_metadata in data.get('paths', {}).items():
        for http_method, http_method_metadata in api_metadata.items():
            tags = http_method_metadata.get('tags', [])

            dataset_name = to_dataset_name('_'.join(tags)) or DEFAULT_DATASET_NAME  # Default dataset if no tags given.
            if dataset_name not in datasets:
                datasets[dataset_name] = {
                    'type': 'dataset',
                    'name': f'{dataset_prefix}/{dataset_name}',
                    'title': ', '.join(tags),
                    'description': ', '.join(tag_metadata[tag] for tag in tags if tag in tag_metadata),
                    'resources': {},
                }

            resource_name = to_code_name(f'{api_endpoint}/{http_method}')
            resource_parameters = get_resource_parameters(http_method_metadata.get('parameters', {}))

            datasets[dataset_name]['resources'][resource_name] = {
                'type': 'dask/json',
                'id': http_method_metadata.get('operationId', ''),
                'external': replace_url_parameters(api_endpoint),
                'prepare': Expr('http', method=http_method.upper(), body='form'),
                'title': http_method_metadata.get('summary', ''),
                'params': resource_parameters,
                'description': http_method_metadata.get('description', ''),
            }
            if 'content' in (response_200:= http_method_metadata.get('responses', {}).get('200',{})):
                schema = response_200['content']['application/json']['schema']
                models += get_dataset_models(f'{dataset_prefix}/{dataset_name}', resource_name, schema)
            

    if not datasets:
        dataset_name = DEFAULT_DATASET_NAME
        datasets[dataset_name] = {
            'type': 'dataset',
            'name': f'{dataset_prefix}/{dataset_name}',
            'title': '',
            'description': '',
            'resources': {},
        }

    for dataset in datasets.values():
        yield None, dataset

    if models:
        for model in models:
            yield None, model


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    """Read & Convert OpenAPI Schema structure to DSA.

    OpenAPI Schema specification: https://spec.openapis.org/oas/latest.html.
    """
    data = read_file_data_and_transform_to_json(path)

    info = data['info']
    title = info['title']
    dataset_prefix = f'services/{to_dataset_name(title)}'

    yield from get_namespace_schema(info, title, dataset_prefix)

    yield from get_dataset_schemas(data, dataset_prefix)

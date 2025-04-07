from __future__ import annotations

import json
from pathlib import Path
from typing import Generator

from spinta.core.ufuncs import Expr
from spinta.utils.naming import to_dataset_name, to_code_name


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        return json.load(file)


def get_namespace_schemas(info: dict, title: str, dataset_prefix: str) -> Generator[tuple[None, dict], None, None]:
    current_path = Path()
    path_parts = dataset_prefix.split('/')

    for index, part in enumerate(path_parts):
        current_path /= part
        is_last = index == len(path_parts) - 1
        yield None, {
            'type': 'ns',
            'name': str(current_path),
            'title': info.get('summary', title) if is_last else '',
            'description': info.get('description', '') if is_last else '',
        }


def get_dataset_schemas(data: dict, dataset_prefix: str) -> Generator[tuple[None, dict]]:
    datasets = {}
    tag_metadata = {tag['name']: tag.get('description') for tag in data.get('tags', {})}

    for api_endpoint, api_metadata in data.get('paths', {}).items():
        for http_method, http_method_metadata in api_metadata.items():
            tags = http_method_metadata.get('tags', [])
            if not tags:
                continue

            dataset_name = to_dataset_name('_'.join(tags))
            if dataset_name not in datasets:
                datasets[dataset_name] = {
                    'type': 'dataset',
                    'name': f'{dataset_prefix}/{dataset_name}',
                    'title': ', '.join(tags),
                    'description': ', '.join(tag_metadata[tag] for tag in tags if tag in tag_metadata),
                    'resources': {}
                }

            resource_source = f'{api_endpoint}/{http_method}'
            resource_name = to_code_name(resource_source)
            datasets[dataset_name]['resources'][resource_name] = {
                'type': 'dask/json',
                'id': http_method_metadata.get('operationId'),
                'external': api_endpoint,
                'prepare': Expr('http', method=http_method.upper(), body='form'),
                'title': http_method_metadata.get('summary'),
                'description': http_method_metadata.get('description'),
            }

    for dataset in datasets.values():
        yield None, dataset


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    """Transforms OpenAPI Schema structure to DSA.

    OpenAPI Schema specification: https://spec.openapis.org/oas/latest.html.
    """
    data = read_file_data_and_transform_to_json(path)

    info = data['info']
    title = info['title']
    dataset_prefix = f'services/{to_dataset_name(title)}'

    yield from get_namespace_schemas(info, title, dataset_prefix)

    yield from get_dataset_schemas(data, dataset_prefix)

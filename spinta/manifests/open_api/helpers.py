from __future__ import annotations

import json
from pathlib import Path
from typing import Generator


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        return json.load(file)


def get_namespace_schemas(info: dict, title: str, dataset_prefix: str) -> Generator[tuple[None, dict]]:
    current_path = Path()
    path_parts = dataset_prefix.split('/')
    is_last_part = lambda i: i == len(path_parts) - 1

    for index, part in enumerate(path_parts):
        current_path /= part
        yield None, {
            'type': 'ns',
            'name': str(current_path),
            'title': info.get('summary', title) if is_last_part(index) else '',
            'description': info.get('description', '') if is_last_part(index) else '',
        }


def get_dataset_schemas(data: dict, dataset_prefix: str) -> Generator[tuple[None, dict]]:
    seen_datasets = set()
    tag_metadata = {tag['name']: tag.get('description', '') for tag in data.get('tags', {})}

    for api_metadata in data.get("paths", {}).values():
        for http_method_metadata in api_metadata.values():
            tags = http_method_metadata.get('tags', [])
            if not tags:
                continue

            dataset_name = '_'.join(tag.lower().replace(' ', '_') for tag in tags)
            if dataset_name in seen_datasets:
                continue

            seen_datasets.add(dataset_name)
            yield None, {
                'type': 'dataset',
                'name': f'{dataset_prefix}/{dataset_name}',
                'title': ', '.join(tags),
                'description': ', '.join(tag_metadata[tag] for tag in tags if tag in tag_metadata)
            }


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    data = read_file_data_and_transform_to_json(path)

    # https://spec.openapis.org/oas/latest.html#info-object
    info = data['info']
    title = info['title']
    dataset_prefix = f'services/{title.lower().replace(" ", "_").replace("/", "_")}'

    yield from get_namespace_schemas(info, title, dataset_prefix)

    yield from get_dataset_schemas(data, dataset_prefix)

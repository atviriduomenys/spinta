from __future__ import annotations

import json
from pathlib import Path
from typing import Generator


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        file_data = file.read()

    return json.loads(file_data)


def get_dataset_schema(info: dict) -> dict:
    return {
        'type': 'dataset',
        'name': info['title'].lower().replace(' ', '_'),
        'title': info.get('summary', info['title']),
        'description': info.get('description')
    }


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    data = read_file_data_and_transform_to_json(path)

    # https://spec.openapis.org/oas/latest.html#info-object
    info = data['info']
    yield None, get_dataset_schema(info)

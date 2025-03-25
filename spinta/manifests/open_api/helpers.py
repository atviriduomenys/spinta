from __future__ import annotations

import json
from pathlib import Path
from typing import Generator


def read_file_data_and_transform_to_json(path: Path) -> dict:
    with Path(path).open() as file:
        return json.load(file)


def read_open_api_manifest(path: Path) -> Generator[tuple[None, dict]]:
    data = read_file_data_and_transform_to_json(path)

    info = data['info']
    title = info['title']
    dataset_name = f'services/{title.lower().replace(" ", "_")}'

    yield None, {
        'type': 'dataset',
        'name': dataset_name,
        'title': info.get('summary', title),
        'description': info.get('description', '')
    }

from __future__ import annotations

from pathlib import Path
from typing import Generator


def read_open_api_manifest(manifest_path: Path) -> Generator[tuple[None, dict]]:
    yield None, {
        'type': 'dataset',
        'name': 'openapi'
    }

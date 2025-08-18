from __future__ import annotations
from http import HTTPStatus
from io import BytesIO

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.helpers import validate_api_response
from spinta.manifests.components import ManifestPath


def create_distribution(base_path: str, headers: dict[str, str], dataset_name: str, file_bytes: bytes, dataset_id: str, manifests: list[ManifestPath]) -> None:
    response = requests.post(
        f"{base_path}/Distribution/",
        headers=headers,
        data={
            "dataset": dataset_id,
            "title": dataset_name,
        },
        files={
            "file": (manifests[0].path, BytesIO(file_bytes), CONTENT_TYPE_TEXT_CSV),
        }
    )
    validate_api_response(response, {HTTPStatus.CREATED}, "Create distribution")

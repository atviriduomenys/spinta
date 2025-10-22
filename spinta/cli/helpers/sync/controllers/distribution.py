from __future__ import annotations
from http import HTTPStatus
from io import BytesIO

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.api_helpers import validate_api_response


def create_distribution(
    base_path: str,
    headers: dict[str, str],
    distribution_name: str,
    file_bytes: bytes,
    dataset_id: str,
) -> None:
    response = requests.post(
        f"{base_path}/Distribution/",
        headers=headers,
        data={
            "dataset": dataset_id,
            "title": distribution_name,
        },
        files={"file": (f"{distribution_name}.csv", BytesIO(file_bytes), CONTENT_TYPE_TEXT_CSV)},
    )
    validate_api_response(response, {HTTPStatus.CREATED}, "Create distribution")

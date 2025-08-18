from __future__ import annotations
from http import HTTPStatus

import requests
from requests.models import Response

from spinta.cli.helpers.sync.helpers import validate_api_response


def get_dataset(base_path: str, headers: dict[str, str], dataset_name: str) -> Response:
    response = requests.get(
        f"{base_path}/Dataset/",
        headers=headers,
        params={"name": dataset_name},
    )
    validate_api_response(response, {HTTPStatus.OK, HTTPStatus.NOT_FOUND}, "Get dataset")
    return response


def create_dataset(base_path: str, headers: dict[str, str], dataset_name: str) -> Response:
    response = requests.post(
        f"{base_path}/Dataset/",
        headers=headers,
        data={
            "title": dataset_name,
            "description": "",
            "name": dataset_name,
        }
    )
    validate_api_response(response, {HTTPStatus.CREATED}, "Create dataset")
    return response

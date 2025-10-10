from __future__ import annotations
from http import HTTPStatus

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.api_helpers import validate_api_response
from spinta.exceptions import NotImplementedFeature


def create_dsa(base_path: str, headers: dict[str, str], dataset_id: str, content: str) -> None:
    response = requests.post(
        f"{base_path}/Dataset/{dataset_id}/dsa/",
        headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **headers},
        data=content,
    )
    validate_api_response(response, {HTTPStatus.NO_CONTENT}, "Create DSA")


def get_dsa(base_path: str, headers: dict[str, str], dataset_id: str) -> str:
    response = requests.get(
        f"{base_path}/Dataset/{dataset_id}/dsa",
        headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **headers},
    )
    validate_api_response(response, {HTTPStatus.OK}, "Get DSA")
    return response.content.decode()


def update_dsa(base_path: str, headers: dict[str, str], dataset_id: str) -> None:
    # TODO: Unfinished: implement w/ https://github.com/atviriduomenys/katalogas/issues/1600.
    response = requests.put(f"{base_path}/Dataset/{dataset_id}/dsa/", headers=headers)
    raise NotImplementedFeature(
        status=response.status_code,
        dataset_id=dataset_id,
        feature="Updates on existing Datasets",
    )

from __future__ import annotations
from http import HTTPStatus

import requests
from requests.models import Response

from spinta.cli.helpers.sync.helpers import validate_api_response


def get_dataset(base_path: str, headers: dict[str, str], dataset_name: str) -> Response:
    """Retrieve a dataset by its unique name (per organization).

    Sends a GET request to the Dataset API with a query parameter `name`.
    Although this endpoint returns a list, at most one dataset is expected.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        dataset_name: Unique name of the dataset (per organization) to retrieve.

    Returns:
        Response: The HTTP response object from the request.

    Raises:
        requests.HTTPError: If the response status is not `200 Ok` or `404 Not Found`.
    """
    response = requests.get(
        f"{base_path}/Dataset/",
        headers=headers,
        params={"name": dataset_name},
    )
    validate_api_response(response, {HTTPStatus.OK, HTTPStatus.NOT_FOUND}, "Get dataset")
    return response


def create_dataset(base_path: str, headers: dict[str, str], dataset_name: str) -> Response:
    """Create a new dataset.

    Sends a POST request to the Dataset API to create a dataset.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        dataset_name: Name (and title) of the new dataset.

    Returns:
        Response: The HTTP response object from the request.

    Raises:
        requests.HTTPError: If the response status is not `201 Created`.
    """
    response = requests.post(
        f"{base_path}/Dataset/",
        headers=headers,
        data={
            "title": dataset_name,
            "description": "",
            "name": dataset_name,
            "service": True,
            "subclass": "service",
        },
    )
    validate_api_response(response, {HTTPStatus.CREATED}, "Create dataset")
    return response

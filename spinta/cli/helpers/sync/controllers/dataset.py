"""
This module defines a unified controller for DCAT resources in the Catalog.

A `Dataset` object in this context can represent five types of resources:
    - Catalog
    - Dataset
    - Data Series
    - Data Service
    - Information System

Instead of maintaining separate controllers for each type, this module
handles creation and related operations for all DCAT resource types.
"""

from __future__ import annotations
from http import HTTPStatus
from typing import Optional

import requests
from requests.models import Response

from spinta.cli.helpers.sync.controllers.enum import ResourceType
from spinta.cli.helpers.sync.helpers import validate_api_response


DEFAULT_RESOURCE = ResourceType.DATASET
RESOURCE_TYPE_MAPPER = {
    ResourceType.CATALOG: {"subclass": ResourceType.CATALOG},
    ResourceType.DATASET: {"subclass": ResourceType.DATASET},
    ResourceType.DATASET_SERIES: {"subclass": ResourceType.DATASET_SERIES, "series": True},
    ResourceType.DATA_SERVICE: {"subclass": "service", "service": True},
    ResourceType.INFORMATION_SYSTEM: {"subclass": ResourceType.INFORMATION_SYSTEM},
}


def get_resource(
    base_path: str, headers: dict[str, str], resource_name: str, validate_response: bool = True
) -> Response:
    """Retrieve a resource by its unique name (per organization).

    Sends a GET request to the Dataset object API with a query parameter `name`.
    Although this endpoint returns a list, at most one resource is expected.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        resource_name: Unique name of the resource (per organization) to retrieve.

    Returns:
        Response: The HTTP response object from the request.

    Raises:
        requests.HTTPError: If the response status is not `200 Ok` or `404 Not Found`.
    """
    response = requests.get(
        f"{base_path}/Dataset/",
        headers=headers,
        params={"name": resource_name},
    )
    if validate_response:
        validate_api_response(response, {HTTPStatus.OK, HTTPStatus.NOT_FOUND}, "Get resource")
    return response


def create_resource(
    base_path: str,
    headers: dict[str, str],
    resource_name: str,
    parent_id: Optional[str] = None,
    resource_type: str = DEFAULT_RESOURCE,
    validate_response: bool = True,
) -> Response:
    """Create a new resource (dataset, data service, etc.).

    Sends a POST request to the Dataset object API to create one of the resources (Dataset model).

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        resource_name: Name (and title) of the new resource.

    Returns:
        Response: The HTTP response object from the request.

    Raises:
        requests.HTTPError: If the response status is not `201 Created`.
    """
    resource_data = RESOURCE_TYPE_MAPPER.get(resource_type, RESOURCE_TYPE_MAPPER[DEFAULT_RESOURCE])

    data = {
        "name": resource_name,
        "title": resource_name,
        "description": "",
        "parent_id": parent_id,
        **resource_data,
    }

    response = requests.post(
        f"{base_path}/Dataset/",
        headers=headers,
        data=data,
    )
    if validate_response:
        validate_api_response(response, {HTTPStatus.CREATED}, "Create resource")
    return response

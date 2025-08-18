from __future__ import annotations
from http import HTTPStatus

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.helpers import validate_api_response
from spinta.exceptions import NotImplementedFeature


def create_dsa(base_path: str, headers: dict[str, str], dataset_id: str, content: str) -> None:
    """Create a DSA (Duomenų Struktūros Aprašas) for a dataset.

    Sends a POST request to the Dataset DSA API endpoint, uploading the
    provided CSV content for the dataset identified by `dataset_id`.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        dataset_id: ID of the dataset to create the DSA for.
        content: CSV content representing the dataset structure.

    Returns:
        None

    Raises:
        requests.HTTPError: If the response status is not `204 No Content`.
    """
    response = requests.post(
        f"{base_path}/Dataset/{dataset_id}/dsa/",
        headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **headers},
        data=content,
    )
    validate_api_response(response, {HTTPStatus.NO_CONTENT}, "Create DSA")


def update_dsa(base_path: str, headers: dict[str, str], dataset_id: str) -> None:
    """Update the DSA of an existing dataset.

    Currently not implemented. Planned to send a PUT request to the
    Dataset DSA API endpoint to update the dataset structure.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        dataset_id: ID of the dataset whose DSA is to be updated.

    Raises:
        NotImplementedFeature: Always raised to indicate that updating
            existing datasets is not yet supported.
    """
    # TODO: Unfinished: implement w/ https://github.com/atviriduomenys/katalogas/issues/1600.
    response = requests.put(f"{base_path}/Dataset/{dataset_id}/dsa/", headers=headers)
    raise NotImplementedFeature(
        status=response.status_code,
        dataset_id=dataset_id,
        feature="Updates on existing Datasets",
    )

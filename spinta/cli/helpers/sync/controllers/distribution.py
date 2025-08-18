from __future__ import annotations
from http import HTTPStatus
from io import BytesIO

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.helpers import validate_api_response
from spinta.manifests.components import ManifestPath


def create_distribution(
    base_path: str,
    headers: dict[str, str],
    dataset_name: str,
    file_bytes: bytes,
    dataset_id: str,
    manifests: list[ManifestPath],
) -> None:
    """Create a distribution for a dataset with a CSV file.

    Sends a POST request to the Distribution API, attaching the given CSV file.
    The distribution is linked to the dataset identified by `dataset_id`.

    Args:
        base_path: Base URL of the API.
        headers: HTTP headers to include in the request.
        dataset_name: Name/title of the dataset for which the distribution is created.
        file_bytes: Content of the CSV file to upload as bytes.
        dataset_id: ID of the dataset to link the distribution to.
        manifests: List of ManifestPath objects representing file paths.

    Returns:
        None

    Raises:
        requests.HTTPError: If the response status is not `201 Created`.
    """
    response = requests.post(
        f"{base_path}/Distribution/",
        headers=headers,
        data={
            "dataset": dataset_id,
            "title": dataset_name,
        },
        files={
            "file": (manifests[0].path, BytesIO(file_bytes), CONTENT_TYPE_TEXT_CSV),
        },
    )
    validate_api_response(response, {HTTPStatus.CREATED}, "Create distribution")

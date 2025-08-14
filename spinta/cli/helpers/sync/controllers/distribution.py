from http import HTTPStatus
from io import BytesIO

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.helpers import check_api_response


def create_distribution(base_path, headers, dataset_name, file_bytes, dataset_id, manifests):
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
    check_api_response(response, {HTTPStatus.CREATED}, "Create distribution")

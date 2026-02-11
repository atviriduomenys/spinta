from http import HTTPStatus

import requests
from requests.models import Response

from spinta.cli.helpers.sync.api_helpers import validate_api_response


def connection_check(base_path: str, headers: dict[str, str], data: dict[str, str]) -> Response:
    response = requests.post(
        f"{base_path}/Connection/check",
        headers=headers,
        data=data,
    )
    validate_api_response(response, {HTTPStatus.NO_CONTENT}, "Connection check")
    return response

from http import HTTPStatus
from typing import Any

from requests import Response

from spinta.cli.helpers.sync import IDENTIFIER
from spinta.cli.helpers.sync.enum import ResponseType
from spinta.client import get_access_token, RemoteClientCredentials
from spinta.exceptions import UnexpectedAPIResponse, UnexpectedAPIResponseData


STATIC_BASE_PATH_TAIL = "/uapi/datasets/org/vssa/isris/dcat"


def validate_api_response(response: Response, expected_status_codes: set[HTTPStatus], operation: str) -> None:
    """Validates the status code the API has responded with."""
    if response.status_code not in expected_status_codes:
        raise UnexpectedAPIResponse(
            operation=operation,
            expected_status_code={code.value for code in expected_status_codes},
            response_status_code=response.status_code,
            response_data=format_error_response_data(response.json()),
        )


def format_error_response_data(data: dict[str, Any]) -> dict:
    """Cleans up the API error response to be user-friendly."""
    data.pop("context", None)  # Removing the full traceback.
    return data


def extract_identifier_from_response(response: Response, response_type: str) -> str:
    identifier = None
    if response_type == ResponseType.LIST:
        identifier = response.json().get("_data", [{}])[0].get(IDENTIFIER)
    elif response_type == ResponseType.DETAIL:
        identifier = response.json().get(IDENTIFIER)

    if not identifier:
        raise UnexpectedAPIResponseData(
            operation=f"Retrieve dataset `{IDENTIFIER}`",
            context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset.",
        )

    return identifier


def get_base_path_and_headers(credentials: RemoteClientCredentials) -> tuple[str, dict[str, str]]:
    access_token = get_access_token(credentials)
    headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}{STATIC_BASE_PATH_TAIL}"

    return base_path, headers

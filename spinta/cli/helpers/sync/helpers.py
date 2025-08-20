from __future__ import annotations
from http import HTTPStatus
from typing import Any, cast

from typer import Context as TyperContext
from requests import Response

from spinta.auth import DEFAULT_CREDENTIALS_SECTION
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.cli.helpers.sync import IDENTIFIER
from spinta.client import get_client_credentials, RemoteClientCredentials, get_access_token
from spinta.components import Config, Context
from spinta import commands
from spinta.core.context import configure_context
from spinta.core.enums import Mode
from spinta.exceptions import (
    NotImplementedFeature,
    ManifestFileNotProvided,
    UnexpectedAPIResponse,
    UnexpectedAPIResponseData,
)
from spinta.manifests.components import ManifestPath
from spinta.manifests.yaml.components import InlineManifest


def validate_api_response(response: Response, expected_status_codes: set[HTTPStatus], operation: str) -> None:
    """Validate that an API response has an expected status code.

    Raises an `UnexpectedAPIResponse` if the response status code is not in the expected set of status codes.

    Args:
        response: The HTTP response to validate.
        expected_status_codes: Set of HTTP status codes that are considered valid.
        operation: Description of the operation being performed (used in error messages).

    Raises:
        UnexpectedAPIResponse: If the response status code is not in `expected_status_codes`.
    """
    if response.status_code not in expected_status_codes:
        raise UnexpectedAPIResponse(
            operation=operation,
            expected_status_code={code.value for code in expected_status_codes},
            response_status_code=response.status_code,
            response_data=format_error_response_data(response.json()),
        )


def get_manifest_paths(manifests: list[str]) -> list[ManifestPath]:
    """Convert a list of manifest strings to `ManifestPath` objects.

    Args:
        manifests: List of manifest file paths or strings.

    Returns:
        List of `ManifestPath` objects.

    Raises:
        ManifestFileNotProvided: If no manifest file is provided.
    """
    manifest_paths = convert_str_to_manifest_path(manifests)
    if not manifests:
        raise ManifestFileNotProvided

    if isinstance(manifest_paths, ManifestPath):
        return [manifest_paths]

    return manifest_paths


def build_manifest_and_context(ctx: TyperContext, manifests: list[ManifestPath]) -> tuple[Context, InlineManifest]:
    """Build a Spinta context and load the corresponding manifest.

    Args:
        ctx: Context containing CLI state.
        manifests: List of `ManifestPath` objects to load.

    Returns:
        Tuple of `Context` (Spinta) and `InlineManifest` (loaded manifest).
    """
    context = configure_context(ctx.obj, manifests, mode=Mode.external)
    store = prepare_manifest(context, verbose=False, full_load=True)
    return context, cast(InlineManifest, store.manifest)


def get_dataset_name(context: Context, manifest: InlineManifest) -> str:
    """Retrieve the dataset name from a manifest.

    Raises an error if more than one dataset is present, since multi-dataset
    synchronization is not implemented.

    Args:
        context: Spinta runtime context.
        manifest: Loaded manifest.

    Returns:
        The single dataset name as a string.

    Raises:
        NotImplementedFeature: If more than one dataset is found.
    """
    datasets = commands.get_datasets(context, manifest)
    if len(datasets) > 1:
        # TODO: https://github.com/atviriduomenys/spinta/issues/1404
        raise NotImplementedFeature(feature="Synchronizing more than 1 dataset at a time")

    return next(iter(datasets))


def get_file_bytes_and_decoded_content(manifests: list[ManifestPath]) -> tuple[bytes, str]:
    """Read the first manifest file as bytes and decode it as UTF-8 text.

    Args:
        manifests: List of `ManifestPath` objects.

    Returns:
        Tuple containing:
            - The raw bytes of the file.
            - The UTF-8 decoded string content of the file.
    """
    with open(manifests[0].path, "rb") as file:
        file_bytes = file.read()
        content = file_bytes.decode("utf-8")
    return file_bytes, content


def get_configuration_credentials(context: Context) -> RemoteClientCredentials:
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)
    return credentials


def get_base_path_and_headers(credentials: RemoteClientCredentials) -> tuple[str, dict[str, str]]:
    """Construct the API base path and authentication headers.

    Retrieves client credentials, fetches an access token, and prepares
    the Authorization headers.

    Args:
        context: Spinta runtime context containing configuration.

    Returns:
        Tuple containing:
            - `base_path`: The base URL for the dataset API.
            - `headers`: Dictionary with Authorization header including Bearer token.
    """
    access_token = get_access_token(credentials)
    headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}/uapi/datasets/org/vssa/isris/dcat"

    return base_path, headers


def format_error_response_data(data: dict[str, Any]) -> dict:
    """Remove unnecessary fields from an API error response for returning to the user.

    Args:
        data: The raw error response data.

    Returns:
        The cleaned error response data dictionary without the 'context' key.
    """
    data.pop("context", None)
    return data


def extract_dataset_id(response: Response, response_type: str) -> str:
    """Extract the dataset ID from an API response.

    Args:
        response: HTTP response containing dataset data.
        response_type: Type of response, either 'list' or 'detail'.

    Returns:
        Dataset ID string.

    Raises:
        UnexpectedAPIResponseData: If the `_id` field is missing from the response.
    """
    dataset_id = None
    if response_type == "list":
        dataset_id = response.json().get("_data", [{}])[0].get(IDENTIFIER)
    elif response_type == "detail":
        dataset_id = response.json().get(IDENTIFIER)

    if not dataset_id:
        raise UnexpectedAPIResponseData(
            operation=f"Retrieve dataset `{IDENTIFIER}`",
            context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset.",
        )

    return dataset_id

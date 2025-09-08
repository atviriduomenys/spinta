from __future__ import annotations
from http import HTTPStatus
from typing import Any, Optional

from typer import echo, Context as TyperContext
from requests import Response

from spinta.auth import DEFAULT_CREDENTIALS_SECTION
from spinta.cli.helpers.sync import IDENTIFIER, ContentType
from spinta.client import get_client_credentials, RemoteClientCredentials, get_access_token
from spinta.components import Config, Context, Model
from spinta import commands
from spinta.core.enums import Visibility
from spinta.datasets.components import Resource
from spinta.datasets.inspect.helpers import create_manifest_from_inspect
from spinta.exceptions import (
    NotImplementedFeature,
    UnexpectedAPIResponse,
    UnexpectedAPIResponseData,
    InvalidCredentialsConfigurationException,
)
from spinta.formats.csv.commands import _render_manifest_csv
from spinta.manifests.components import ManifestPath, Manifest
from spinta.manifests.helpers import init_manifest
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import datasets_to_tabular
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


def get_context_and_manifest(
    ctx: TyperContext,
    manifest: ManifestPath,
    resource: Optional[tuple[str, str]],
    formula: str,
    backend: Optional[str],
    auth: Optional[str],
    priority: str,
) -> tuple[Context, Manifest]:
    """Create a context and manifest for synchronization.

    This function inspects and builds a manifest from provided resources,
    formula, backend, and authentication settings. The `priority` determines
    whether manifest or external data should take precedence.

    Args:
        context: Spinta runtime context.
        manifest: Path or identifier of the manifest to inspect.
        resource: Resource definitions to include in the manifest.
        formula: Formula definitions for derived data.
        backend: Backend configuration for the manifest.
        auth: Authentication configuration.
        priority: Either ``"manifest"`` or ``"external"`` indicating which data takes precedence.

    Returns:
        Tuple containing:
            - context: Spinta runtime context.
            - manifest: Generated manifest object.
    """
    if priority not in ["manifest", "external"]:
        echo(
            f"Priority '{priority}' does not exist, there can only be 'manifest' or 'external', it will be set to default 'manifest'."
        )
        priority = "manifest"

    context, manifest = create_manifest_from_inspect(
        context=ctx.obj,
        manifest=manifest,
        resources=resource,
        formula=formula,
        backend=backend,
        auth=auth,
        priority=priority,
    )

    return context, manifest


def get_data_service_name_prefix(credentials: RemoteClientCredentials) -> str:
    """Build a Dataset prefix for later calls to open data Catalog.

    Building a dataset prefix from:
        - Organization type;
        - Organization codename/name;
        - Information system (IS);
        - Information subsystem (subIS).

    Args:
        credentials: RemoteClientCredentials object with client ID, secret, organization info, and server details.

    Returns:
        A string type prefix to add to the beginning of the Data service name.
            - `datasets/<organization_type>/<organization_codename>/<IS>/<subIS>`
    """
    if not any([credentials.organization_type, credentials.organization]):
        raise InvalidCredentialsConfigurationException(required_credentials=["organization_type", "organization"])
    prefix = f"datasets/{credentials.organization_type}/{credentials.organization}"
    # TODO: Add IS & subIS, when that information is available (When Agents are related w/ DS and not Organizations).
    return prefix


def clean_private_attributes(resource: Resource) -> dict[str, Model]:
    resource.external = ""
    resource.type = ""
    resource.backend.name = ""

    resource_models = resource.models
    for model_name, model in resource_models.items():
        for property_name, property in model.properties.items():
            if property.basename.startswith("_"):  # Skip private properties.
                continue
            if property.visibility not in {Visibility.private, None}:
                continue
            property.external = ""

        if model.visibility not in {Visibility.private, None}:
            continue
        model.external.name = ""  # Source column cleanup.

    return resource_models


def prepare_synchronization_manifests(context: Context, manifest: Manifest, prefix: str) -> list[dict[str, Any]]:
    """Prepare dataset and resource manifests for synchronization.

    Iterates through datasets and their resources to construct separate
    manifests for each dataset and its resources, along with their models.

    Args:
        context: Spinta runtime context.
        manifest: Root manifest containing dataset definitions.

    Returns:
        List of dataset metadata dictionaries, each containing:
            - `name`: Dataset name.
            - `dataset_manifest`: Manifest object for the dataset.
            - `resources`: List of resource dictionaries with:
                - `name`: Resource name.
                - `manifest`: Manifest object for the resource.
    """
    dataset_data = []
    datasets = commands.get_datasets(context, manifest)
    for dataset_name, dataset_object in datasets.items():
        dataset_object.name = f"{prefix}/{dataset_object.name}"
        dataset_manifest = Manifest()
        init_manifest(context, dataset_manifest, "sync_dataset_manifest")

        dataset_manifest_models = []
        resource_data = []

        for resource_name, resource_object in dataset_object.resources.items():
            resource_manifest = Manifest()
            init_manifest(context, resource_manifest, "sync_resource_manifest")

            resource_models = clean_private_attributes(resource_object)
            commands.set_models(context, resource_manifest, resource_models)
            dataset_manifest_models.append(resource_models)

            resource_data.append(
                {
                    "name": resource_name,
                    "manifest": resource_manifest,
                }
            )

        dataset_models = {}
        for model in dataset_manifest_models:
            dataset_models.update(model)
        commands.set_models(context, dataset_manifest, dataset_models)

        dataset_data.append(
            {
                "name": dataset_object.title or dataset_name.rsplit("/", 1)[-1],
                "dataset_manifest": dataset_manifest,
                "resources": resource_data,
            }
        )

    return dataset_data


def get_configuration_credentials(context: Context) -> RemoteClientCredentials:
    """Retrieve remote client credentials from configuration.

    Reads the credential file specified in the Spinta configuration and
     load client credentials from the default section.

    Args:
        context: Spinta runtime context containing configuration.

    Returns:
        RemoteClientCredentials object with client ID, secret, organization info, and server details.
    """
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)
    return credentials


def validate_credentials(credentials: RemoteClientCredentials) -> None:
    required = {
        "resource_server": credentials.resource_server,
        "server": credentials.server,
        "client": credentials.client,
        "organization_type": credentials.organization_type,
        "organization": credentials.organization,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise InvalidCredentialsConfigurationException(missing_credentials=", ".join(missing))


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


def get_agent_name(credentials: RemoteClientCredentials) -> str:
    return credentials.client.rsplit("_", 1)[0]


def format_error_response_data(data: dict[str, Any]) -> dict:
    """Remove unnecessary fields from an API error response for returning to the user.

    Args:
        data: The raw error response data.

    Returns:
        The cleaned error response data dictionary without the 'context' key.
    """
    data.pop("context", None)
    return data


def extract_identifier_from_response(response: Response, response_type: str) -> str:
    """Extract the resource ID from an API response.

    Args:
        response: HTTP response containing resource (dataset, data service, etc.) data.
        response_type: Type of response, either 'list' or 'detail'.

    Returns:
        Resource ID string.

    Raises:
        UnexpectedAPIResponseData: If the `_id` field is missing from the response.
    """
    identifier = None
    if response_type == "list":
        identifier = response.json().get("_data", [{}])[0].get(IDENTIFIER)
    elif response_type == "detail":
        identifier = response.json().get(IDENTIFIER)

    if not identifier:
        raise UnexpectedAPIResponseData(
            operation=f"Retrieve dataset `{IDENTIFIER}`",
            context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset.",
        )

    return identifier


def render_content_from_manifest(context: Context, manifest: InlineManifest, content_type: ContentType) -> bytes | str:
    """Render manifest content in the requested format.

    Converts dataset information from a manifest into tabular rows and serializes it into:
        - CSV;
        - UTF-8 encoded bytes.

    Args:
        context: Spinta runtime context.
        manifest: Loaded manifest to render.
        content_type: Desired content type, either ``ContentType.CSV`` or ``ContentType.BYTES``.

    Returns:
        Rendered content as a CSV string or UTF-8 encoded bytes.

    Raises:
        NotImplementedFeature: If the provided content type is not supported.
    """
    rows = datasets_to_tabular(context, manifest)
    rows = ({c: row[c] for c in DATASET} for row in rows)

    rendered_rows = "".join(_render_manifest_csv(rows))
    if content_type == ContentType.CSV:
        return rendered_rows
    elif content_type == ContentType.BYTES:
        return rendered_rows.encode("utf-8")
    else:
        raise NotImplementedFeature()

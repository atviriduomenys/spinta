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


EXCLUDED_RESOURCE_FIELDS = frozenset({"dataset", "models", "given", "lang"})


def validate_api_response(response: Response, expected_status_codes: set[HTTPStatus], operation: str) -> None:
    """Validates the status code the API has responded with."""
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
    """Build a dataset prefix in accordance with the UAPI format."""
    if not any([credentials.organization_type, credentials.organization]):
        raise InvalidCredentialsConfigurationException(required_credentials=["organization_type", "organization"])
    prefix = f"datasets/{credentials.organization_type}/{credentials.organization}"
    # TODO: Add IS & subIS, when that information is available (When Agents are related w/ DS and not Organizations).
    return prefix


def clean_private_attributes(resource: Resource) -> dict[str, Model]:
    """Cleans up attributes that are marked `visibility=private` in the DSA (Duomenų Struktūros Aprašas)."""
    for field in resource.__annotations__:
        if field not in EXCLUDED_RESOURCE_FIELDS:
            setattr(resource, field, "")
    resource.type = ""

    resource.models = {name: model for name, model in resource.models.items() if model.visibility != Visibility.private}

    for model in resource.models.values():
        model.properties = {
            name: property for name, property in model.properties.items() if property.visibility != Visibility.private
        }

    return resource.models


def prepare_synchronization_manifests(context: Context, manifest: Manifest, prefix: str) -> list[dict[str, Any]]:
    """Prepare dataset and resource manifests for synchronization.

    Iterates through datasets and their resources to construct separate
    manifests for each dataset and its resources, along with their models & properties.
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
    """Retrieve remote client credentials from configuration."""
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)
    return credentials


def validate_credentials(credentials: RemoteClientCredentials) -> None:
    """Validates the credentials required for calls to the Catalog."""
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
    the Authorization headers for API calls to Catalog.
    """
    access_token = get_access_token(credentials)
    headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}/uapi/datasets/org/vssa/isris/dcat"

    return base_path, headers


def get_agent_name(credentials: RemoteClientCredentials) -> str:
    """Retrieves the name of the agent from the client name set in the configuration file."""
    return credentials.client.rsplit("_", 1)[0]


def format_error_response_data(data: dict[str, Any]) -> dict:
    """Cleans up the API error response to be user-friendly."""
    data.pop("context", None)  # Removing the full traceback.
    return data


def extract_identifier_from_response(response: Response, response_type: str) -> str:
    """Extract the resource ID from an API response."""
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

    Converts dataset information from a manifest into tabular rows and serializes it into the selected format:
     - CSV;
     - UTF-8 encoded bytes.
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

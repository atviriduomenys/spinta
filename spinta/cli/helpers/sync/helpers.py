from typing import List, Tuple, Dict

from typer import Context as TyperContext

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
from spinta.manifests.components import ManifestPath, Manifest


def check_api_response(response, expected_status_codes, operation):
    if response.status_code not in expected_status_codes:
        raise UnexpectedAPIResponse(
            operation=operation,
            expected_status_code=expected_status_codes,
            response_status_code=response.status_code,
            response_data=format_error_response_data(response.json())
        )


def get_manifest_paths(manifests: List[str]):
    manifests = convert_str_to_manifest_path(manifests)
    if not manifests:
        raise ManifestFileNotProvided

    return manifests


def build_manifest_and_context(ctx: TyperContext, manifests: List[str]) -> Tuple[Context, Manifest]:
    context = configure_context(ctx.obj, manifests, mode=Mode.external)
    store = prepare_manifest(context, verbose=False, full_load=True)
    return context, store.manifest


def get_dataset_name(context: Context, manifest: Manifest) -> str:
    datasets = commands.get_datasets(context, manifest)
    if len(datasets) > 1:
        # TODO: https://github.com/atviriduomenys/spinta/issues/1404
        raise NotImplementedFeature(feature="Synchronizing more than 1 dataset at a time")

    return next(iter(datasets))


def get_file_bytes_and_decoded_content(manifests: List[ManifestPath]) -> Tuple[bytes, str]:
    with open(manifests[0].path, "rb") as file:
        file_bytes = file.read()
        content = file_bytes.decode("utf-8")
    return file_bytes, content


def get_base_path_and_headers(context: Context) -> Tuple[str, Dict[str, str]]:
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)

    access_token = get_access_token(credentials)
    headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}/uapi/datasets/org/vssa/isris/dcat"

    return base_path, headers


def format_error_response_data(data: dict) -> dict:
    data.pop("context", None)
    return data


def extract_dataset_id(response, response_type):
    dataset_id = None
    if response_type == "list":
        dataset_id = response.json().get("_data", [{}])[0].get(IDENTIFIER)
    elif response_type == "detail":
        dataset_id = response.json().get(IDENTIFIER)

    if not dataset_id:
        raise UnexpectedAPIResponseData(
            operation=f"Retrieve dataset `{IDENTIFIER}`",
            context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset."
        )

    return dataset_id

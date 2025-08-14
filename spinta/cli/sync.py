import logging
from http import HTTPStatus
from io import BytesIO
from typing import List, Tuple, Dict

import requests
from typer import Context as TyperContext, Argument

from spinta.auth import DEFAULT_CREDENTIALS_SECTION
from spinta.client import get_client_credentials, RemoteClientCredentials, get_access_token
from spinta.components import Config, Context
from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.enums import Mode
from spinta.core.context import configure_context
from spinta.exceptions import NotImplementedFeature, UnexpectedAPIResponse, UnexpectedAPIResponseData, \
    ManifestFileNotProvided
from spinta.manifests.components import ManifestPath, Manifest


logger = logging.getLogger(__name__)


CONTENT_TYPE_TEXT_CSV = "text/csv"
IDENTIFIER = "_id"


def _get_dataset_name(context: Context, manifest: Manifest) -> str:
    datasets = commands.get_datasets(context, manifest)
    if len(datasets) > 1:
        # TODO: https://github.com/atviriduomenys/spinta/issues/1404
        raise NotImplementedFeature(feature="Synchronizing more than 1 dataset at a time")

    return next(iter(datasets))


def _get_file_bytes_and_decoded_content(manifests: List[ManifestPath]) -> Tuple[bytes, str]:
    with open(manifests[0].path, "rb") as file:
        file_bytes = file.read()
        content = file_bytes.decode("utf-8")
    return file_bytes, content


def _get_base_path_and_headers(context: Context) -> Tuple[str, Dict[str, str]]:
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)

    access_token = get_access_token(credentials)
    headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}/uapi/datasets/org/vssa/isris/dcat"

    return base_path, headers


def _format_error_response_data(data: dict) -> dict:
    data.pop("context", None)
    return data


def get_manifest_paths(manifests: List[str]):
    manifests = convert_str_to_manifest_path(manifests)
    if not manifests:
        raise ManifestFileNotProvided

    return manifests


def build_manifest_and_context(ctx: TyperContext, manifests: List[str]) -> Tuple[Context, Manifest]:
    context = configure_context(ctx.obj, manifests, mode=Mode.external)
    store = prepare_manifest(context, verbose=False, full_load=True)
    return context, store.manifest


def get_dataset(base_path, headers, dataset_name):
    response = requests.get(
        f"{base_path}/Dataset/",
        headers=headers,
        params={"name": dataset_name},
    )

    if response.status_code not in {HTTPStatus.OK, HTTPStatus.NOT_FOUND}:
        raise UnexpectedAPIResponse(
            operation="Get dataset",
            expected_status_code={HTTPStatus.OK, HTTPStatus.NOT_FOUND},
            response_status_code=response.status_code,
            response_data=_format_error_response_data(response.json()),
        )

    return response


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


def update_dsa(base_path, headers, dataset_id):
    # TODO: Unfinished: implement w/ https://github.com/atviriduomenys/katalogas/issues/1600.
    response = requests.put(f"{base_path}/Dataset/{dataset_id}/dsa/", headers=headers)
    raise NotImplementedFeature(
        status=response.status_code,
        dataset_id=dataset_id,
        feature="Updates on existing Datasets",
    )


def create_dataset(base_path, headers, dataset_name):
    response = requests.post(
        f"{base_path}/Dataset/",
        headers=headers,
        data={
            "title": dataset_name,
            "description": "",
            "name": dataset_name,
        }
    )

    if response.status_code != HTTPStatus.CREATED:
        raise UnexpectedAPIResponse(
            operation="Create dataset",
            expected_status_code=HTTPStatus.CREATED,
            response_status_code=response.status_code,
            response_data=_format_error_response_data(response.json()),
        )

    return response


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

    if response.status_code != HTTPStatus.CREATED:
        raise UnexpectedAPIResponse(
            operation="Create dataset distribution",
            expected_status_code=HTTPStatus.CREATED,
            response_status_code=response.status_code,
            response_data=_format_error_response_data(response.json()),
        )


def create_dsa(base_path, headers, dataset_id, content):
    response = requests.post(
        f"{base_path}/Dataset/{dataset_id}/dsa/",
        headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **headers},
        data=content,
    )

    if response.status_code != HTTPStatus.NO_CONTENT:
        raise UnexpectedAPIResponse(
            operation="Create DSA for dataset",
            expected_status_code=HTTPStatus.CREATED,
            response_status_code=response.status_code,
            response_data=_format_error_response_data(response.json()),
        )


def sync(
    ctx: TyperContext,
    manifests: List[str] = Argument(None, help=("Manifest files to load")),
):
    manifests = get_manifest_paths(manifests)
    context, manifest = build_manifest_and_context(ctx, manifests)

    dataset_name = _get_dataset_name(context, manifest)
    file_bytes, content = _get_file_bytes_and_decoded_content(manifests)
    base_path, headers = _get_base_path_and_headers(context)

    response_get_dataset = get_dataset(base_path, headers, dataset_name)
    if response_get_dataset.status_code == HTTPStatus.OK:
        dataset_id = extract_dataset_id(response_get_dataset, "list")
        update_dsa(base_path, headers, dataset_id)
    elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
        response_create_dataset = create_dataset(base_path, headers, dataset_name)
        dataset_id = extract_dataset_id(response_create_dataset, "detail")
        create_distribution(base_path, headers, dataset_name, file_bytes, dataset_id, manifests)
        create_dsa(base_path, headers, dataset_id, content)

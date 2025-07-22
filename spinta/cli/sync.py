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
from spinta.exceptions import NotImplementedFeature, UnexpectedAPIResponse, UnexpectedAPIResponseData
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


def _get_base_path_and_authorization_headers(context: Context) -> Tuple[str, Dict[str, str]]:
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)

    access_token = get_access_token(credentials)
    authorization_headers = {"Authorization": f"Bearer {access_token}"}

    resource_server = credentials.resource_server or credentials.server
    base_path = f"{resource_server}/uapi/datasets/org/vssa/isris/dcat"

    return base_path, authorization_headers


def _format_error_response_data(data: dict) -> dict:
    data.pop("context", None)
    return data


def sync(
    ctx: TyperContext,
    manifests: List[str] = Argument(None, help=("Manifest files to load")),
):
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests, mode=Mode.external)
    store = prepare_manifest(context, verbose=False, full_load=True)
    manifest: Manifest = store.manifest

    dataset_name = _get_dataset_name(context, manifest)
    file_bytes, content = _get_file_bytes_and_decoded_content(manifests)
    base_path, authorization_headers = _get_base_path_and_authorization_headers(context)

    response_get_dataset = requests.get(
        f"{base_path}/Dataset/",
        headers=authorization_headers,
        params={"name": dataset_name},
    )
    if response_get_dataset.status_code not in {HTTPStatus.OK, HTTPStatus.NOT_FOUND}:
        raise UnexpectedAPIResponse(
            operation="Get dataset",
            expected_status_code={HTTPStatus.OK, HTTPStatus.NOT_FOUND},
            response_status_code=response_get_dataset.status_code,
            response_data=_format_error_response_data(response_get_dataset.json()),
        )

    if response_get_dataset.status_code == HTTPStatus.OK:
        if not (dataset_id := response_get_dataset.json().get("_data", [{}])[0].get(IDENTIFIER)):
            raise UnexpectedAPIResponseData(
                operation=f"Retrieve dataset `{IDENTIFIER}`",
                context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset."
            )
        response_update_dsa = requests.put(f"{base_path}/Dataset/{dataset_id}/dsa/", headers=authorization_headers)
        # TODO: Implement w/ https://github.com/atviriduomenys/katalogas/issues/1600.
        raise NotImplementedFeature(
            status=response_update_dsa.status_code,
            dataset_id=dataset_id,
            feature="Updates on existing Datasets",
        )
    elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
        response_post_dataset = requests.post(
            f"{base_path}/Dataset/",
            headers=authorization_headers,
            data={
                "title": dataset_name,
                "description": "",
                "name": dataset_name,
            }
        )

        if response_post_dataset.status_code != HTTPStatus.CREATED:
            raise UnexpectedAPIResponse(
                operation="Create dataset",
                expected_status_code=HTTPStatus.CREATED,
                response_status_code=response_post_dataset.status_code,
                response_data=_format_error_response_data(response_post_dataset.json()),
            )

        dataset_id = response_post_dataset.json().get(IDENTIFIER)
        if not dataset_id:
            raise UnexpectedAPIResponseData(
                operation=f"Retrieve dataset `{IDENTIFIER}`",
                context=f"Dataset did not return the `{IDENTIFIER}` field which can be used to identify the dataset."
            )
        response_post_distribution = requests.post(
            f"{base_path}/Distribution/",
            headers=authorization_headers,
            data={
                "dataset": dataset_id,
                "title": dataset_name,
            },
            files={
                "file": (manifests[0].path, BytesIO(file_bytes), CONTENT_TYPE_TEXT_CSV),
            }
        )

        if response_post_distribution.status_code != HTTPStatus.CREATED:
            raise UnexpectedAPIResponse(
                operation="Create dataset distribution",
                expected_status_code=HTTPStatus.CREATED,
                response_status_code=response_post_distribution.status_code,
                response_data=_format_error_response_data(response_post_distribution.json()),
            )

        response_create_dsa = requests.post(
            f"{base_path}/Dataset/{dataset_id}/dsa/",
            headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **authorization_headers},
            data=content,
        )

        if response_create_dsa.status_code != HTTPStatus.NO_CONTENT:
            raise UnexpectedAPIResponse(
                operation="Create DSA for dataset",
                expected_status_code=HTTPStatus.CREATED,
                response_status_code=response_create_dsa.status_code,
                response_data=_format_error_response_data(response_create_dsa.json()),
            )

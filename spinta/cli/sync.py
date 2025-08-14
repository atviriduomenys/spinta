import logging
from http import HTTPStatus
from typing import List

from typer import Context as TyperContext, Argument

from spinta.cli.helpers.sync.controllers.dataset import get_dataset, create_dataset
from spinta.cli.helpers.sync.controllers.distribution import create_distribution
from spinta.cli.helpers.sync.controllers.dsa import update_dsa, create_dsa
from spinta.cli.helpers.sync.helpers import (
    get_dataset_name,
    get_file_bytes_and_decoded_content,
    get_base_path_and_headers,
    get_manifest_paths,
    build_manifest_and_context, extract_dataset_id,
)


logger = logging.getLogger(__name__)


def sync(
    ctx: TyperContext,
    manifests: List[str] = Argument(None, help=("Manifest files to load")),
):
    manifests = get_manifest_paths(manifests)
    context, manifest = build_manifest_and_context(ctx, manifests)

    dataset_name = get_dataset_name(context, manifest)
    file_bytes, content = get_file_bytes_and_decoded_content(manifests)
    base_path, headers = get_base_path_and_headers(context)

    response_get_dataset = get_dataset(base_path, headers, dataset_name)
    if response_get_dataset.status_code == HTTPStatus.OK:
        dataset_id = extract_dataset_id(response_get_dataset, "list")
        update_dsa(base_path, headers, dataset_id)
    elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
        response_create_dataset = create_dataset(base_path, headers, dataset_name)
        dataset_id = extract_dataset_id(response_create_dataset, "detail")
        create_distribution(base_path, headers, dataset_name, file_bytes, dataset_id, manifests)
        create_dsa(base_path, headers, dataset_id, content)

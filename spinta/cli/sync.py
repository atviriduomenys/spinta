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
    build_manifest_and_context,
    extract_dataset_id,
    get_configuration_credentials,
)
from spinta.manifests.components import ManifestPath

logger = logging.getLogger(__name__)


def sync(
    ctx: TyperContext,
    manifests: List[str] = Argument(None, help=("Manifest files to load")),
):
    """Synchronize datasets from data source to the data Catalog.

    This command reads the specified data sources (databases, files, etc.), retrieves or creates the
    corresponding datasets in the catalog, and ensures that their distributions and DSA are up to date.

    The workflow is as follows:
    1. Load and parse data sources.
    2. Build a Spinta context and load the manifest.
    3. Determine the dataset name from the manifest.
    4. Retrieve the dataset from the catalog:
       - If it exists, update its DSA.
       - If it does not exist, create the dataset, its distributions, and its DSA in the Catalog.

    Args:
        ctx: Typer CLI context object.
        manifests: List of manifest file paths to synchronize.

    Raises:
        NotImplementedFeature: If an unsupported operation is encountered
            (e.g., updating existing datasets not fully implemented).
        ManifestFileNotProvided: If no manifest files are given.
        UnexpectedAPIResponse: If the API responds with an unexpected status code.
        UnexpectedAPIResponseData: If expected dataset identifiers are missing
            from the API response.

    Documentation:
        https://atviriduomenys.readthedocs.io/agentas.html#sinchronizacija
    """
    manifest_objects: List[ManifestPath] = get_manifest_paths(manifests)
    context, manifest = build_manifest_and_context(ctx, manifest_objects)

    dataset_name = get_dataset_name(context, manifest)
    file_bytes, content = get_file_bytes_and_decoded_content(manifest_objects)
    credentials = get_configuration_credentials(context)
    base_path, headers = get_base_path_and_headers(credentials)

    response_get_dataset = get_dataset(base_path, headers, dataset_name)
    if response_get_dataset.status_code == HTTPStatus.OK:
        dataset_id = extract_dataset_id(response_get_dataset, "list")
        update_dsa(base_path, headers, dataset_id)
    elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
        response_create_dataset = create_dataset(base_path, headers, dataset_name)
        dataset_id = extract_dataset_id(response_create_dataset, "detail")
        create_distribution(base_path, headers, dataset_name, file_bytes, dataset_id, manifest_objects)
        create_dsa(base_path, headers, dataset_id, content)

import logging
from http import HTTPStatus
from typing import Optional

from typer import Context as TyperContext, Argument, Option

from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.sync import ContentType
from spinta.cli.helpers.sync.controllers.dataset import get_dataset, create_dataset
from spinta.cli.helpers.sync.controllers.distribution import create_distribution
from spinta.cli.helpers.sync.controllers.dsa import update_dsa, create_dsa
from spinta.cli.helpers.sync.helpers import (
    get_base_path_and_headers,
    get_context_and_manifest,
    prepare_synchronization_manifests,
    render_content_from_manifest,
    extract_dataset_id,
    get_configuration_credentials,
)

logger = logging.getLogger(__name__)


def sync(
    ctx: TyperContext,
    manifest: Optional[str] = Argument(None, help="Path to manifest."),
    resource: Optional[tuple[str, str]] = Option(
        (None, None),
        "-r",
        "--resource",
        help=("Resource type and source URI (-r sql sqlite:////tmp/db.sqlite)"),
    ),
    formula: str = Option("", "-f", "--formula", help=("Formula if needed, to prepare resource for reading")),
    backend: Optional[str] = Option(None, "-b", "--backend", help=("Backend connection string")),
    auth: Optional[str] = Option(None, "-a", "--auth", help=("Authorize as a client")),
    priority: str = Option("manifest", "-p", "--priority", help=("Merge property priority ('manifest' or 'external')")),
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
        NotImplementedFeature: If an unsupported operation is encountered.
        UnexpectedAPIResponse: If the API responds with an unexpected status code.
        UnexpectedAPIResponseData: If dataset identifiers that were expected are missing from the API response.
        UnexpectedAPIResponseData: If expected attributes are missing from the API response.

    Documentation:
        https://atviriduomenys.readthedocs.io/agentas.html#sinchronizacija
    """
    manifest = convert_str_to_manifest_path(manifest)
    context, manifest = get_context_and_manifest(
        ctx, manifest, resource, formula, backend, auth, priority
    )
    dataset_data = prepare_synchronization_manifests(context, manifest)

    credentials = get_configuration_credentials(context)
    base_path, headers = get_base_path_and_headers(credentials)

    for dataset in dataset_data:
        dataset_name = dataset["name"]
        response_get_dataset = get_dataset(base_path, headers, dataset_name)
        if response_get_dataset.status_code == HTTPStatus.OK:
            dataset_id = extract_dataset_id(response_get_dataset, "list")
            update_dsa(base_path, headers, dataset_id)
        elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
            response_create_dataset = create_dataset(base_path, headers, dataset_name)
            dataset_id = extract_dataset_id(response_create_dataset, "detail")
            for distribution in dataset["resources"]:
                file_bytes = render_content_from_manifest(context, distribution["manifest"], ContentType.BYTES)
                create_distribution(base_path, headers, f"{dataset_name}/{distribution['name']}", file_bytes, dataset_id
                )
            content = render_content_from_manifest(context, dataset["dataset_manifest"], ContentType.CSV)
            create_dsa(base_path, headers, dataset_id, content)

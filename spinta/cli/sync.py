import logging
from http import HTTPStatus
from typing import Optional

from typer import Context as TyperContext, Argument, Option

from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.sync import ContentType
from spinta.cli.helpers.sync.controllers.data_service import get_data_service_id
from spinta.cli.helpers.sync.controllers.dataset import get_resource, create_resource
from spinta.cli.helpers.sync.controllers.dsa import update_dsa, create_dsa
from spinta.cli.helpers.sync.helpers import (
    get_base_path_and_headers,
    get_context_and_manifest,
    prepare_synchronization_manifests,
    render_content_from_manifest,
    extract_identifier_from_response,
    get_configuration_credentials,
    get_data_service_name_prefix,
    get_agent_name,
    validate_credentials,
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
    context, manifest = get_context_and_manifest(ctx, manifest, resource, formula, backend, auth, priority)
    dataset_data = prepare_synchronization_manifests(context, manifest)

    credentials = get_configuration_credentials(context)
    validate_credentials(credentials)
    base_path, headers = get_base_path_and_headers(credentials)
    agent_name = get_agent_name(credentials)
    prefix = get_data_service_name_prefix(credentials)

    data_service_id = get_data_service_id(base_path, headers, agent_name)
    for dataset in dataset_data:
        dataset_name = f"{prefix}/{dataset['name']}"
        response_get_dataset = get_resource(base_path, headers, dataset_name)
        if response_get_dataset.status_code == HTTPStatus.OK:
            dataset_id = extract_identifier_from_response(response_get_dataset, "list")
            update_dsa(base_path, headers, dataset_id)
        elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
            response_create_dataset = create_resource(base_path, headers, dataset_name, data_service_id)
            dataset_id = extract_identifier_from_response(response_create_dataset, "detail")
            content = render_content_from_manifest(context, dataset["dataset_manifest"], ContentType.CSV)
            create_dsa(base_path, headers, dataset_id, content)

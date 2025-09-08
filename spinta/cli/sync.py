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

    This command:
     - Reads the specified data sources (databases, files, etc.)
     - Creates/Retrieves a parent Data Service for all datasets that will be created
     - Creates/Retrieves datasets corresponding to what was retrieved from source and creates datasets in Catalog.
     - Creates as many DSA (Duomenų Struktūros Aprašas) as there are datasets in Catalog.
     - Ensures that the datasets & their DSA's are up to date; # TODO: Yet to be implemented.

    Documentation:
        https://atviriduomenys.readthedocs.io/agentas.html#sinchronizacija
    """
    manifest = convert_str_to_manifest_path(manifest)
    context, manifest = get_context_and_manifest(ctx, manifest, resource, formula, backend, auth, priority)

    credentials = get_configuration_credentials(context)
    validate_credentials(credentials)
    base_path, headers = get_base_path_and_headers(credentials)
    agent_name = get_agent_name(credentials)

    prefix = get_data_service_name_prefix(credentials)
    dataset_data = prepare_synchronization_manifests(context, manifest, prefix)

    data_service_id = get_data_service_id(base_path, headers, agent_name)
    for dataset in dataset_data:
        dataset_name = dataset["name"]
        response_get_dataset = get_resource(base_path, headers, dataset_name)
        if response_get_dataset.status_code == HTTPStatus.OK:
            dataset_id = extract_identifier_from_response(response_get_dataset, "list")
            update_dsa(base_path, headers, dataset_id)
        elif response_get_dataset.status_code == HTTPStatus.NOT_FOUND:
            response_create_dataset = create_resource(base_path, headers, dataset_name, data_service_id)
            dataset_id = extract_identifier_from_response(response_create_dataset, "detail")
            content = render_content_from_manifest(context, dataset["dataset_manifest"], ContentType.CSV)
            create_dsa(base_path, headers, dataset_id, content)

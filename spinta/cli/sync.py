import logging

from typer import echo, Context as TyperContext

from spinta.cli.helpers.sync.api_helpers import get_base_path_and_headers
from spinta.cli.helpers.sync.controllers.data_service import get_data_service_children_dataset_ids
from spinta.cli.helpers.sync.controllers.synchronization.catalog_to_agent import (
    execute_synchronization_catalog_to_agent,
)
from spinta.cli.helpers.sync.helpers import (
    get_configuration_credentials,
    validate_credentials,
    get_agent_name,
    load_configuration_values,
    prepare_local_manifest_file,
    prepare_context,
)
from spinta.components import Context
from spinta.manifests.tabular.helpers import render_tabular_manifest


logger = logging.getLogger(__name__)


def prepare_synchronization(ctx: TyperContext) -> tuple[Context, str, str]:
    data_source_name, manifest_path = load_configuration_values(ctx.obj)
    prepare_local_manifest_file(manifest_path)
    context = prepare_context(ctx.obj)

    return context, data_source_name, manifest_path


def prepare_api_context(context: Context) -> tuple[str, dict[str, str], str]:
    credentials = get_configuration_credentials(context)
    validate_credentials(credentials)
    base_path, headers = get_base_path_and_headers(credentials)
    agent_name = get_agent_name(credentials)

    return base_path, headers, agent_name


def sync(ctx: TyperContext) -> None:
    context, data_source_name, manifest_path = prepare_synchronization(ctx)
    base_path, headers, agent_name = prepare_api_context(context)

    dataset_ids = get_data_service_children_dataset_ids(base_path, headers, agent_name)
    manifest = execute_synchronization_catalog_to_agent(context, base_path, headers, manifest_path, dataset_ids)
    echo(render_tabular_manifest(context, manifest))

    # TODO: Part 2: Source -> Agent: https://github.com/atviriduomenys/spinta/issues/1489.
    # TODO: Part 3: Agent -> Catalog: https://github.com/atviriduomenys/katalogas/issues/1942.

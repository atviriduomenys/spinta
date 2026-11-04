from spinta import commands
from spinta.components import Config, Context
from spinta.core.enums import Level
from spinta.datasets.components import Dataset, Resource
from spinta.dimensions.param.helpers import check_query_params_have_values, check_resource_source_query_conflicts
from spinta.exceptions import InvalidName
from spinta.utils.naming import is_valid_namespace_name


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)

    config: Config = context.get("config")

    if config.check_names:
        name = dataset.name
        if not is_valid_namespace_name(name):
            raise InvalidName(dataset, name=name, type="namespace")


@commands.check.register()
def check(context: Context, resource: Resource):
    # Always checked: `query()` without default needs a linked property to get its value.
    check_query_params_have_values(resource)

    config: Config = context.get("config")
    # Opt-in via `spinta check --check-query-conflicts`, never runs on normal manifest load.
    if config.check_query_conflicts:
        check_resource_source_query_conflicts(resource)


@commands.identifiable.register(Dataset)
def identifiable(dataset: Dataset):
    return dataset.level is None or dataset.level >= Level.identifiable


@commands.identifiable.register(Resource)
def identifiable(resource: Resource):
    return resource.level is None or resource.level >= Level.identifiable

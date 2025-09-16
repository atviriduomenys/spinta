from spinta import commands
from spinta.components import Context
from spinta.components import Config
from spinta.core.enums import Level
from spinta.datasets.components import Dataset, Resource
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
    pass


@commands.identifiable.register(Dataset)
def identifiable(dataset: Dataset):
    return dataset.level is None or dataset.level >= Level.identifiable


@commands.identifiable.register(Resource)
def identifiable(resource: Resource):
    return resource.level is None or resource.level >= Level.identifiable

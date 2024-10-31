import re

from spinta import commands
from spinta.components import Context
from spinta.components import Config
from spinta.core.enums import Level
from spinta.datasets.components import Dataset, Resource
from spinta.exceptions import InvalidName

namespace_re = re.compile(r'^[a-z](_?[a-z0-9]+)*(/[a-z](_?[a-z0-9]+)+)*$')


def check_dataset_name(name: str):
    return namespace_re.match(name) is not None


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)

    config: Config = context.get('config')

    if config.check_names:
        name = dataset.name
        if namespace_re.match(name) is None:
            raise InvalidName(dataset, name=name, type='namespace')


@commands.check.register()
def check(context: Context, resource: Resource):
    pass


@commands.identifiable.register()
def identifiable(dataset: Dataset):
    return dataset.level is None or dataset.level >= Level.identifiable


@commands.identifiable.register()
def identifiable(resource: Resource):
    return resource.level is None or resource.level >= Level.identifiable

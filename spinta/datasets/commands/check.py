import re

from spinta import commands
from spinta.components import Context
from spinta.components import Config
from spinta.datasets.components import Dataset, Resource
from spinta.exceptions import InvalidName

is_lowercase = re.compile(r'^[a-z][a-z0-9]+(/[a-z][a-z0-9]*)+$')


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)

    config: Config = context.get('config')

    if config.check_names:
        name = dataset.name

        if is_lowercase.match(name) is None:
            raise InvalidName(
                class_name=dataset.__class__.__name__,
                name=name
            )


@commands.check.register()
def check(context: Context, resource: Resource):
    pass


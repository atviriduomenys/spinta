import re

from spinta import commands
from spinta.components import Context, Config
from spinta.datasets.components import Dataset, Resource
from spinta.exceptions import InvalidName


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)

    config: Config = context.get('config')

    if config.check_names:
        name = dataset.name
        is_lowercase = re.compile(r'^[a-z0-1_]*(/[a-z0-1_]*)+$')

        # Dataset name check
        if is_lowercase.match(name) is None or name.startswith('/') or name.endswith('/') or name.count('//'):
            raise InvalidName(
                class_name=dataset.__class__.__name__,
                name=name
            )


@commands.check.register()
def check(context: Context, resource: Resource):
    pass


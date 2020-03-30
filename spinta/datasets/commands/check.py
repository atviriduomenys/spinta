from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset, Resource


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)


@commands.check.register()
def check(context: Context, resource: Resource):
    pass

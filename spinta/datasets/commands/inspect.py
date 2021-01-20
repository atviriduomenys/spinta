from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset


@commands.inspect.register(Context, Dataset)
def inspect(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.inspect(context, resource, resource.backend)

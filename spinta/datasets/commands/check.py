import pathlib

from spinta import commands
from spinta.components import Context, Config, Store
from spinta.datasets.components import Dataset, Resource
from spinta.exceptions import MultipleDatasetsError
from spinta.exceptions import InvalidFileName


@commands.check.register()
def check(context: Context, dataset: Dataset):
    config: Config = context.get('config')
    store: Store = context.get('store')

    filename = config.check_filename
    if filename:
        if len(store.manifest.datasets) > 1:
            raise MultipleDatasetsError(manifest=store.manifest)
        for name in config.check_manifests:
            path = pathlib.Path(name)
            file_path = str(path.with_suffix(''))
            if file_path not in dataset.ns.name:
                raise InvalidFileName(
                    dataset=dataset.ns.name,
                    name=file_path
                )

    for resource in dataset.resources.values():
        commands.check(context, resource)


@commands.check.register()
def check(context: Context, resource: Resource):
    pass

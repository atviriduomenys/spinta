from spinta import commands
from spinta.components import Context, Config, Store
from spinta.datasets.components import Dataset, Resource
from spinta.exceptions import MultipleDatasetsError, InvalidFileName


@commands.check.register()
def check(context: Context, dataset: Dataset):
    config: Config = context.get('config')
    store: Store = context.get('store')

    filename = config.check_filename

    if filename:
        if len(store.manifest.datasets) > 1:
            raise MultipleDatasetsError()
        for name in filename:
            if 'csv' in name:
                file_name = name[:name.find('csv') - 1]
                if file_name not in dataset.ns.name:
                    raise InvalidFileName(
                        dataset=dataset.ns.name,
                        name=file_name
                    )

    for resource in dataset.resources.values():
        commands.check(context, resource)


@commands.check.register()
def check(context: Context, resource: Resource):
    pass

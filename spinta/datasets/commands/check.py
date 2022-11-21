from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset, Resource


@commands.check.register()
def check(context: Context, dataset: Dataset):
    rc = context.get('rc')
    filename = rc.get('filename')
    if filename:
        if len(context.get('store').manifest.datasets) > 1:
            raise Exception(f'File contains more than one dataset.')
        for name in filename:
            if 'csv' in name:
                csv_ns_name = name[:name.find('csv') - 1]
                if csv_ns_name not in dataset.ns.name:
                    raise Exception(f'Dataset namespace {dataset.ns.name} not match the csv filename {csv_ns_name}.')

    for resource in dataset.resources.values():
        commands.check(context, resource)


@commands.check.register()
def check(context: Context, resource: Resource):
    pass

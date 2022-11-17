from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset, Resource
from spinta.components import Model
import re


@commands.check.register()
def check(context: Context, dataset: Dataset):
    rc = context.get('rc')
    if rc.get('names'):
        name = dataset.name
        lowercase = len([i for i in name if i.islower()] + re.findall('/', name)) == len(name)
        if not lowercase or name.startswith('/') or name.endswith('/') or re.findall('//', name):
            raise Exception(f'Dataset name {name} is not correct.')
    for resource in dataset.resources.values():
        commands.check(context, resource)


@commands.check.register()
def check(context: Context, resource: Resource):
    pass


@commands.check.register()
def check(context: Context, model: Model):
    rc = context.get('rc')
    if rc.get('names'):
        name = model.name
        if name.lower() != name and name.upper() != name and '_' in name:
            raise Exception(f'Model name {name} is not correct.')
        for name, prop in model.properties.items():
            if re.findall(r'^[A-Z]\w*$', name):
                raise Exception(f'Property name {name} is not correct.')

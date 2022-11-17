from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset, Resource
import re


@commands.check.register()
def check(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.check(context, resource)

    rc = context.get('rc')

    if rc.get('names'):
        name = dataset.name
        is_lowercase = len([i for i in name if i.islower()] + re.findall('/', name)) == len(name)

        # Dataset name check
        if not is_lowercase or name.startswith('/') or name.endswith('/') or re.findall('//', name):
            raise Exception(f'Dataset name {name} is not correct.')

        store = context.get('store')
        models = store.manifest.models

        for name, model in models.items():
            if not name.startswith('_'):
                name = name.rsplit('/', 1)[-1]
                is_lowercase = name.lower() == name
                is_uppercase = name.upper() == name

                # Model name check
                if not re.findall(r'^[A-Za-z]\w*$', name) or is_lowercase or is_uppercase:
                    raise Exception(f'Model name {name} is not correct.')

            for prop_name, prop in model.properties.items():

                # Property name check
                if re.findall(r'^[A-Z]\w*$', prop_name):
                    raise Exception(f'Property name {prop_name} is not correct.')


@commands.check.register()
def check(context: Context, resource: Resource):
    pass

